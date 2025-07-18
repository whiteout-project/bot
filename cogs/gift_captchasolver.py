#!/usr/bin/env python3
# Gift Captcha Solver for WOS Discord Bot
# Version 3 - now with ONNX model

import os
import io
import time
import logging
import logging.handlers
import json

try:
    import onnxruntime as ort
    import numpy as np
    from PIL import Image
    ONNX_AVAILABLE = True
except ImportError:
    ort = None
    np = None
    Image = None
    ONNX_AVAILABLE = False

class GiftCaptchaSolver:
    def __init__(self, save_images=0):
        """
        Initialize the ONNX captcha solver.

        Args:
            save_images (int): Image saving mode (0=None, 1=Failed, 2=Success, 3=All).
                               Note: Saving logic is primarily handled in gift_operations.py now.
        """
        self.save_images_mode = save_images
        self.onnx_session = None
        self.model_metadata = None
        self.is_initialized = False

        # Logger setup
        self.logger = logging.getLogger("gift_solver")
        if not self.logger.hasHandlers():
            self.logger.setLevel(logging.INFO)
            self.logger.propagate = False
            log_dir = 'log'
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, 'gift_solver.txt')
            handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=3 * 1024 * 1024, backupCount=3, encoding='utf-8')
            handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
            self.logger.addHandler(handler)

        self.captcha_dir = 'captcha_images'
        os.makedirs(self.captcha_dir, exist_ok=True)

        self._initialize_onnx_model()

        self.stats = {
            "total_attempts": 0,
            "successful_decodes": 0,
            "failures": 0
        }
        self.reset_run_stats()

    def reset_run_stats(self):
        """Reset statistics for the current run."""
        self.run_stats = {
            "total_attempts": 0,
            "successful_decodes": 0,
            "failures": 0,
            "start_time": time.time()
        }

    def get_run_stats_report(self):
        """Get a formatted report of run statistics."""
        duration = time.time() - self.run_stats["start_time"]
        success_rate = 0
        if self.run_stats["total_attempts"] > 0:
            success_rate = (self.run_stats["successful_decodes"] / self.run_stats["total_attempts"]) * 100

        report = [
            "\n=== Captcha Solver Statistics ===",
            f"Total captcha attempts: {self.run_stats['total_attempts']}",
            f"Successful decodes: {self.run_stats['successful_decodes']}",
            f"Failures: {self.run_stats['failures']}",
            f"Success rate: {success_rate:.2f}%",
            f"Processing time: {duration:.2f} seconds",
            "=========================================="
        ]
        return "\n".join(report)

    def _initialize_onnx_model(self):
        """Initialize the ONNX model and load metadata."""
        if not ONNX_AVAILABLE:
            self.logger.error("ONNX Runtime or required libraries not found. Captcha solving disabled.")
            self.is_initialized = False
            return

        try:
            # Look for model files in the models directory
            bot_dir = os.path.dirname(os.path.dirname(__file__))
            models_dir = os.path.join(bot_dir, 'models')
            model_path = os.path.join(models_dir, 'captcha_model.onnx')
            metadata_path = os.path.join(models_dir, 'captcha_model_metadata.json')
            
            self.logger.info(f"Looking for ONNX model at: {model_path}")
            self.logger.info(f"Looking for metadata at: {metadata_path}")
            
            if not os.path.exists(model_path):
                self.logger.error(f"ONNX model file not found at {model_path}")
                self.is_initialized = False
                return
                
            if not os.path.exists(metadata_path):
                self.logger.error(f"Model metadata file not found at {metadata_path}")
                self.is_initialized = False
                return

            self.logger.info("Loading ONNX model...")
            self.onnx_session = ort.InferenceSession(model_path)
            
            self.logger.info("Loading model metadata...")
            with open(metadata_path, 'r') as f:
                self.model_metadata = json.load(f)
            
            self.logger.info("Performing test inference...")
            # Create a dummy image matching the expected input shape
            height, width = self.model_metadata['input_shape'][1:3]
            dummy_img = np.random.rand(1, 1, height, width).astype(np.float32)
            
            input_name = self.onnx_session.get_inputs()[0].name
            outputs = self.onnx_session.run(None, {input_name: dummy_img})
            
            if len(outputs) == 4:  # Should have 4 outputs for 4 character positions
                self.logger.info(f"ONNX model test successful. Model ready for captcha solving.")
                self.is_initialized = True
            else:
                self.logger.error(f"ONNX model test failed. Expected 4 outputs, got {len(outputs)}")
                self.is_initialized = False
                
        except Exception as e:
            self.logger.exception(f"Failed during ONNX model initialization: {e}")
            self.onnx_session = None
            self.model_metadata = None
            self.is_initialized = False
    
    def _preprocess_image(self, image_bytes):
        """Preprocess image for ONNX model input."""
        try:
            # Open image
            image = Image.open(io.BytesIO(image_bytes))
            
            # Convert to grayscale
            if image.mode != 'L':
                image = image.convert('L')
            
            # Get expected dimensions from metadata
            height, width = self.model_metadata['input_shape'][1:3]
            
            # Resize image
            image = image.resize((width, height), Image.LANCZOS)
            
            # Convert to numpy array
            image_array = np.array(image, dtype=np.float32)
            
            # Normalize using metadata values
            mean = self.model_metadata['normalization']['mean'][0]
            std = self.model_metadata['normalization']['std'][0]
            image_array = (image_array / 255.0 - mean) / std
            
            # Add batch and channel dimensions: (1, 1, height, width)
            image_array = np.expand_dims(image_array, axis=0)
            image_array = np.expand_dims(image_array, axis=0)
            
            return image_array
            
        except Exception as e:
            self.logger.error(f"Error preprocessing image: {e}")
            return None

    async def solve_captcha(self, image_bytes, fid=None, attempt=0):
        """
        Attempts to solve captcha using ONNX model.

        Args:
            image_bytes (bytes): The raw byte data of the captcha image.
            fid (optional): Player ID for logging.
            attempt (int): Attempt number for logging.

        Returns:
            tuple: (solved_code, success, method, confidence, image_path)
                   - solved_code (str or None): The solved captcha text or None on failure.
                   - success (bool): True if solved successfully, False otherwise.
                   - method (str): Always "ONNX".
                   - confidence (float): Average confidence score of all positions.
                   - image_path (None): No longer provides a path from solver.
        """
        if not self.is_initialized or not self.onnx_session or not self.model_metadata:
            self.logger.error(f"ONNX model not initialized. Cannot solve captcha for FID {fid}.")
            return None, False, "ONNX", 0.0, None

        self.stats["total_attempts"] += 1
        self.run_stats["total_attempts"] += 1
        start_time = time.time()

        try:
            EXPECTED_CAPTCHA_LENGTH = 4
            VALID_CHARACTERS = set(self.model_metadata['chars'])

            # Preprocess image
            input_data = self._preprocess_image(image_bytes)
            if input_data is None:
                self.stats["failures"] += 1
                self.run_stats["failures"] += 1
                self.logger.error(f"[Solver] FID {fid}, Attempt {attempt+1}: Failed to preprocess image")
                return None, False, "ONNX", 0.0, None

            # Run inference
            input_name = self.onnx_session.get_inputs()[0].name
            outputs = self.onnx_session.run(None, {input_name: input_data})

            # Decode predictions
            idx_to_char = self.model_metadata['idx_to_char']
            predicted_text = ""
            confidences = []

            for pos in range(4):  # 4 character positions
                char_probs = outputs[pos][0]  # Get probabilities for this position
                predicted_idx = np.argmax(char_probs)  # Get highest probability
                confidence = float(char_probs[predicted_idx])  # Get confidence score
                predicted_char = idx_to_char[str(predicted_idx)]
                predicted_text += predicted_char
                confidences.append(confidence)

            # Calculate average confidence
            avg_confidence = sum(confidences) / len(confidences)

            solve_duration = time.time() - start_time
            self.logger.info(f"[Solver] FID {fid}, Attempt {attempt+1}: ONNX raw result='{predicted_text}' (confidence: {avg_confidence:.3f}, {solve_duration:.3f}s)")

            if (predicted_text and
                isinstance(predicted_text, str) and
                len(predicted_text) == EXPECTED_CAPTCHA_LENGTH and
                all(c in VALID_CHARACTERS for c in predicted_text)):

                self.stats["successful_decodes"] += 1
                self.run_stats["successful_decodes"] += 1
                self.logger.info(f"[Solver] FID {fid}, Attempt {attempt+1}: Success. Solved: {predicted_text}")
                return predicted_text, True, "ONNX", avg_confidence, None
            else:
                self.stats["failures"] += 1
                self.run_stats["failures"] += 1
                self.logger.warning(f"[Solver] FID {fid}, Attempt {attempt+1}: Failed validation (Length: {len(predicted_text) if predicted_text else 'N/A'}, Chars OK: {all(c in VALID_CHARACTERS for c in predicted_text) if predicted_text else 'N/A'})")
                return None, False, "ONNX", 0.0, None

        except Exception as e:
            self.stats["failures"] += 1
            self.run_stats["failures"] += 1
            self.logger.exception(f"[Solver] FID {fid}, Attempt {attempt+1}: Exception during ONNX inference: {e}")
            return None, False, "ONNX", 0.0, None

    def get_stats(self):
        """Get current OCR statistics."""
        return self.stats