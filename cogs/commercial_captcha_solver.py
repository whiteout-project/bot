"""
Commercial CAPTCHA Solver Module for Whiteout Survival Discord Bot

This module provides commercial CAPTCHA solving services using Anti-Captcha API
as a fallback when OCR-based solving fails.
"""

import os
import base64
import tempfile
import hashlib
import time
import requests
from datetime import datetime
from typing import Optional, Tuple
import logging
import traceback

class CommercialCaptchaSolver:
    """
    Commercial CAPTCHA solver using Anti-Captcha service.
    This serves as a fallback when OCR-based solving fails.
    """
    
    def __init__(self, api_key: Optional[str] = None, logger: Optional[logging.Logger] = None):
        """
        Initialize the commercial CAPTCHA solver.
        
        Args:
            api_key: Anti-Captcha API key. If None, will try to get from environment.
            logger: Logger instance. If None, creates a basic logger.
        """
        self.api_key = api_key or os.getenv('ANTICAPTCHA_API_KEY')
        self.logger = logger or self._setup_default_logger()
        
        # Anti-Captcha service configuration
        self.api_base_url = "https://api.anti-captcha.com"
        self.solver = None
        self.is_enabled = bool(self.api_key)
        
        # Statistics tracking
        self.stats = {
            'total_attempts': 0,
            'successful_solves': 0,
            'failed_solves': 0,
            'balance_checks': 0,
            'last_balance': 0.0,
            'total_cost_estimate': 0.0,
            'last_solve_time': None
        }
        
        # Initialize solver if API key is available
        if self.is_enabled:
            self._initialize_solver()
        else:
            self.logger.warning("Commercial CAPTCHA solver disabled - no API key provided")
    
    def _setup_default_logger(self) -> logging.Logger:
        """Setup a default logger for the commercial captcha solver."""
        logger = logging.getLogger('commercial_captcha')
        logger.setLevel(logging.INFO)
        
        # Avoid duplicate handlers
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _initialize_solver(self) -> None:
        """Initialize the Anti-Captcha solver."""
        try:
            # Import here to make it optional
            from anticaptchaofficial.imagecaptcha import imagecaptcha
            
            self.solver = imagecaptcha()
            self.solver.set_verbose(0)  # Reduce verbosity
            self.solver.set_key(self.api_key)
            self.solver.set_soft_id(0)
            
            self.logger.info("Commercial CAPTCHA solver initialized successfully")
            
            # Check initial balance
            self.check_balance()
            
        except ImportError as e:
            self.logger.error(f"Failed to import anticaptchaofficial library: {e}")
            self.logger.error("Install with: pip install anticaptchaofficial")
            self.is_enabled = False
            self.solver = None
        except Exception as e:
            self.logger.error(f"Failed to initialize commercial CAPTCHA solver: {e}")
            self.is_enabled = False
            self.solver = None
    
    def check_balance(self) -> float:
        """
        Check the Anti-Captcha account balance.
        
        Returns:
            Current account balance in USD, or 0.0 if check fails
        """
        if not self.is_enabled or not self.solver:
            return 0.0
        
        try:
            self.stats['balance_checks'] += 1
            balance = self.solver.get_balance()
            self.stats['last_balance'] = balance
            
            self.logger.debug(f"Anti-Captcha balance: ${balance:.3f}")
            
            if balance <= 0:
                self.logger.warning("Anti-Captcha balance is zero or negative!")
                return 0.0
            
            return balance
            
        except Exception as e:
            self.logger.error(f"Error checking Anti-Captcha balance: {e}")
            return 0.0
    
    def get_captcha_from_api(self, player_id: str, session: requests.Session, 
                           wos_encrypt_key: str, wos_captcha_url: str) -> Optional[str]:
        """
        Get CAPTCHA image from the WOS API.
        
        Args:
            player_id: Player FID
            session: Requests session
            wos_encrypt_key: WOS encryption key
            wos_captcha_url: WOS CAPTCHA endpoint URL
            
        Returns:
            Base64 encoded CAPTCHA image data, or None if failed
        """
        try:
            # Generate required parameters
            time_ms = int(time.time() * 1000)
            init = 0
            
            # Generate signature (same as in reference implementation)
            sign_string = f"fid={player_id}&init={init}&time={time_ms}{wos_encrypt_key}"
            signature = hashlib.md5(sign_string.encode()).hexdigest()
            
            # Prepare payload
            payload = {
                'fid': player_id,
                'time': time_ms,
                'init': init,
                'sign': signature
            }
            
            self.logger.debug(f"Requesting CAPTCHA for player {player_id}")
            
            response = session.post(wos_captcha_url, data=payload, timeout=10)
            
            if response.status_code != 200:
                self.logger.error(f"CAPTCHA API returned status {response.status_code}: {response.text}")
                return None
            
            # Parse JSON response
            captcha_data = response.json()
            self.logger.debug(f"CAPTCHA API response: {captcha_data}")
            
            # Check for rate limiting error
            if (captcha_data.get("code") == 1 and 
                captcha_data.get("msg") == "CAPTCHA CHECK TOO FREQUENT." and 
                captcha_data.get("err_code") == 40101):
                
                self.logger.warning("CAPTCHA rate limit reached, waiting 60 seconds...")
                time.sleep(60)  # Wait 1 minute
                
                # Retry the request after waiting
                self.logger.info("Retrying CAPTCHA request after rate limit wait")
                response = session.post(wos_captcha_url, data=payload, timeout=10)
                
                if response.status_code != 200:
                    self.logger.error(f"CAPTCHA API returned status {response.status_code} after retry: {response.text}")
                    return None
                
                captcha_data = response.json()
                self.logger.debug(f"CAPTCHA API response after retry: {captcha_data}")
            
            # Check response structure
            if captcha_data.get("code") != 0 or captcha_data.get("msg") != "SUCCESS":
                self.logger.error(f"CAPTCHA API error: {captcha_data}")
                return None
            
            # Extract base64 image data
            img_data = captcha_data.get("data", {}).get("img", "")
            if not img_data.startswith("data:image/"):
                self.logger.error("Invalid image data format")
                return None
            
            # Extract base64 part
            base64_data = img_data.split(",")[1] if "," in img_data else img_data
            
            self.logger.debug("CAPTCHA image received successfully")
            return base64_data
            
        except Exception as e:
            self.logger.error(f"Error getting CAPTCHA: {e}")
            return None
    
    def save_captcha_to_temp_file(self, base64_data: str) -> Optional[str]:
        """
        Save base64 CAPTCHA data to temporary file.
        
        Args:
            base64_data: Base64 encoded image data
            
        Returns:
            Path to temporary file, or None if failed
        """
        try:
            # Decode base64 data
            image_data = base64.b64decode(base64_data)
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpeg")
            temp_file.write(image_data)
            temp_file.close()
            
            self.logger.debug(f"CAPTCHA saved to temporary file: {temp_file.name}")
            return temp_file.name
            
        except Exception as e:
            self.logger.error(f"Error saving CAPTCHA to temp file: {e}")
            return None
    
    def solve_captcha(self, captcha_file_path: str) -> Optional[str]:
        """
        Solve CAPTCHA using Anti-Captcha service.
        
        Args:
            captcha_file_path: Path to CAPTCHA image file
            
        Returns:
            CAPTCHA solution text, or None if failed
        """
        if not self.is_enabled or not self.solver:
            self.logger.error("Commercial CAPTCHA solver not available")
            return None
        
        try:
            self.stats['total_attempts'] += 1
            
            self.logger.info("Submitting CAPTCHA to Anti-Captcha service...")
            
            # Solve CAPTCHA
            captcha_text = self.solver.solve_and_return_solution(captcha_file_path)
            
            if captcha_text != 0:
                self.stats['successful_solves'] += 1
                self.stats['total_cost_estimate'] += 0.002  # Rough estimate: $0.002 per solve
                self.stats['last_solve_time'] = datetime.now()
                
                self.logger.info(f"CAPTCHA solved successfully: '{captcha_text}'")
                return captcha_text
            else:
                self.stats['failed_solves'] += 1
                error_code = getattr(self.solver, 'error_code', 'Unknown')
                self.logger.error(f"CAPTCHA solving failed: {error_code}")
                return None
                
        except Exception as e:
            self.stats['failed_solves'] += 1
            self.logger.error(f"Error solving CAPTCHA: {e}")
            return None
        finally:
            # Clean up temporary file
            try:
                if os.path.exists(captcha_file_path):
                    os.unlink(captcha_file_path)
                    self.logger.debug(f"Cleaned up temporary file: {captcha_file_path}")
            except Exception as e:
                self.logger.warning(f"Could not clean up temp file: {e}")
    
    def report_incorrect_captcha(self) -> None:
        """Report an incorrect CAPTCHA solution to Anti-Captcha service."""
        if not self.is_enabled or not self.solver:
            return
        
        try:
            self.solver.report_incorrect_image_captcha()
            self.logger.info("Reported incorrect CAPTCHA to Anti-Captcha service")
        except Exception as e:
            self.logger.warning(f"Failed to report incorrect CAPTCHA: {e}")
    
    def solve_captcha_for_player(self, player_id: str, session: requests.Session,
                               wos_encrypt_key: str, wos_captcha_url: str) -> Tuple[Optional[str], bool]:
        """
        Complete CAPTCHA solving workflow for a player.
        
        Args:
            player_id: Player FID
            session: Requests session
            wos_encrypt_key: WOS encryption key
            wos_captcha_url: WOS CAPTCHA endpoint URL
            
        Returns:
            Tuple of (captcha_solution, success)
        """
        if not self.is_enabled:
            self.logger.error("Commercial CAPTCHA solver is disabled")
            return None, False
        
        # Check balance first
        balance = self.check_balance()
        if balance <= 0:
            self.logger.error("Insufficient Anti-Captcha balance")
            return None, False
        
        # Get CAPTCHA image
        captcha_base64 = self.get_captcha_from_api(player_id, session, wos_encrypt_key, wos_captcha_url)
        if not captcha_base64:
            self.logger.error("Failed to get CAPTCHA image")
            return None, False
        
        # Save to temporary file
        captcha_file = self.save_captcha_to_temp_file(captcha_base64)
        if not captcha_file:
            self.logger.error("Failed to save CAPTCHA to temporary file")
            return None, False
        
        # Solve CAPTCHA
        solution = self.solve_captcha(captcha_file)
        if solution:
            self.logger.info(f"Commercial CAPTCHA solver succeeded: '{solution}'")
            return solution, True
        else:
            self.logger.error("Commercial CAPTCHA solver failed")
            return None, False
    
    def get_stats(self) -> dict:
        """
        Get solver statistics.
        
        Returns:
            Dictionary containing solver statistics
        """
        stats = self.stats.copy()
        stats['is_enabled'] = self.is_enabled
        stats['has_api_key'] = bool(self.api_key)
        stats['solver_initialized'] = bool(self.solver)
        
        if self.stats['total_attempts'] > 0:
            stats['success_rate'] = (self.stats['successful_solves'] / self.stats['total_attempts']) * 100
        else:
            stats['success_rate'] = 0.0
        
        return stats
    
    def reset_stats(self) -> None:
        """Reset solver statistics."""
        self.stats = {
            'total_attempts': 0,
            'successful_solves': 0,
            'failed_solves': 0,
            'balance_checks': 0,
            'last_balance': 0.0,
            'total_cost_estimate': 0.0,
            'last_solve_time': None
        }
        self.logger.info("Commercial CAPTCHA solver statistics reset")
