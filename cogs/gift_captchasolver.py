"""
Captcha solver using an ONNX neural network model for gift code redemption.
"""
import os
import io
import time
import asyncio
import logging
import logging.handlers
import json

try:
    # Suppress ONNX C++ GPU warning (writes to fd 2, not sys.stderr)
    import sys
    _fd, _null = sys.stderr.fileno(), os.open(os.devnull, os.O_WRONLY)
    _bak = os.dup(_fd); os.dup2(_null, _fd); os.close(_null)
    import onnxruntime as ort
    os.dup2(_bak, _fd); os.close(_bak)
    import numpy as np
    from PIL import Image
    ONNX_AVAILABLE = True
except ImportError:
    ort = None
    np = None
    Image = None
    ONNX_AVAILABLE = False

from . import onnx_lifecycle

class GiftCaptchaSolver:
    def __init__(self, save_images=0):
        """
        Initialize the ONNX captcha solver.

        Args:
            save_images (int): Image saving mode (0=None, 1=Failed, 2=Success, 3=All).
                               Controlled via --save-captcha CLI arg.
        """
        self.save_images_mode = save_images
        self.model_metadata = None
        self.is_initialized = False
        self._model_wrapper: onnx_lifecycle.LazyOnnxModel | None = None

        # Use centralized gift logger
        self.logger = logging.getLogger('gift')

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
        """Verify model files exist and load metadata. Defers actual session
        creation until the first solve_captcha() call so memory is only used
        during gift code redemption."""
        if not ONNX_AVAILABLE:
            self.logger.error("ONNX Runtime or required libraries not found. Captcha solving disabled.")
            self.is_initialized = False
            return

        try:
            bot_dir = os.path.dirname(os.path.dirname(__file__))
            models_dir = os.path.join(bot_dir, 'models')
            model_path = os.path.join(models_dir, 'captcha_model.onnx')
            metadata_path = os.path.join(models_dir, 'captcha_model_metadata.json')

            if not os.path.exists(model_path):
                self.logger.error(f"ONNX model file not found at {model_path}")
                self.is_initialized = False
                return
            if not os.path.exists(metadata_path):
                self.logger.error(f"Model metadata file not found at {metadata_path}")
                self.is_initialized = False
                return

            with open(metadata_path, 'r') as f:
                self.model_metadata = json.load(f)

            # Pinned: captcha drives the bot's primary feature (gift code
            # redemption) and is small enough (~22 MB) that the memory savings
            # don't justify cold-start cost on every periodic-validation cycle.
            self._model_wrapper = onnx_lifecycle.get_or_create(
                name='captcha',
                display_name='Gift Captcha',
                factory=lambda: ort.InferenceSession(model_path),
                pinned=True,
            )
            self.is_initialized = True
            self.logger.info("Captcha solver ready (model will load on first use).")

        except Exception as e:
            self.logger.exception(f"Failed during captcha solver initialization: {e}")
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

    def _run_inference_sync(self, image_bytes, session):
        """Sync portion of solve_captcha: preprocess image + ONNX inference +
        decode. Returns (predicted_text, avg_confidence) or None on preprocess
        failure. Pulled out so async callers can off-load it to a thread and
        keep the asyncio event loop free for Discord heartbeats."""
        input_data = self._preprocess_image(image_bytes)
        if input_data is None:
            return None
        input_name = session.get_inputs()[0].name
        outputs = session.run(None, {input_name: input_data})
        idx_to_char = self.model_metadata['idx_to_char']
        predicted_text = ""
        confidences = []
        for pos in range(4):
            char_probs = outputs[pos][0]
            predicted_idx = np.argmax(char_probs)
            confidences.append(float(char_probs[predicted_idx]))
            predicted_text += idx_to_char[str(predicted_idx)]
        return predicted_text, sum(confidences) / len(confidences)

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
        if not self.is_initialized or not self._model_wrapper or not self.model_metadata:
            self.logger.error(f"ONNX model not initialized. Cannot solve captcha for ID {fid}.")
            return None, False, "ONNX", 0.0, None

        self.stats["total_attempts"] += 1
        self.run_stats["total_attempts"] += 1
        start_time = time.time()

        try:
            EXPECTED_CAPTCHA_LENGTH = 4
            VALID_CHARACTERS = set(self.model_metadata['chars'])

            async with self._model_wrapper.use() as session:
                inference_result = await asyncio.to_thread(
                    self._run_inference_sync, image_bytes, session
                )
            if inference_result is None:
                self.stats["failures"] += 1
                self.run_stats["failures"] += 1
                self.logger.error(f"[Solver] ID {fid}, Attempt {attempt+1}: Failed to preprocess image")
                return None, False, "ONNX", 0.0, None

            predicted_text, avg_confidence = inference_result
            solve_duration = time.time() - start_time
            self.logger.info(f"[Solver] ID {fid}, Attempt {attempt+1}: ONNX raw result='{predicted_text}' (confidence: {avg_confidence:.3f}, {solve_duration:.3f}s)")

            if (predicted_text and
                isinstance(predicted_text, str) and
                len(predicted_text) == EXPECTED_CAPTCHA_LENGTH and
                all(c in VALID_CHARACTERS for c in predicted_text)):

                self.stats["successful_decodes"] += 1
                self.run_stats["successful_decodes"] += 1
                self.logger.info(f"[Solver] ID {fid}, Attempt {attempt+1}: Success. Solved: {predicted_text}")
                return predicted_text, True, "ONNX", avg_confidence, None
            else:
                self.stats["failures"] += 1
                self.run_stats["failures"] += 1
                self.logger.warning(f"[Solver] ID {fid}, Attempt {attempt+1}: Failed validation (Length: {len(predicted_text) if predicted_text else 'N/A'}, Chars OK: {all(c in VALID_CHARACTERS for c in predicted_text) if predicted_text else 'N/A'})")
                return None, False, "ONNX", 0.0, None

        except Exception as e:
            self.stats["failures"] += 1
            self.run_stats["failures"] += 1
            self.logger.exception(f"[Solver] ID {fid}, Attempt {attempt+1}: Exception during ONNX inference: {e}")
            return None, False, "ONNX", 0.0, None

    def get_stats(self):
        """Get current OCR statistics."""
        return self.stats