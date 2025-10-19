import aiohttp
import asyncio
import hashlib
import time
import ssl
import os
from datetime import datetime
from typing import Optional, List, Dict, Callable

class LoginHandler:
    """
    Centralized handler for player login/check operations.
    Manages dual-API support and rate limiting for player data fetching.
    Note: This does NOT handle gift code operations which have separate rate limits.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoginHandler, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        # Only initialize once
        if self._initialized:
            return
            
        # API Configuration for login/player check
        self.api1_url = 'https://wos-giftcode-api.centurygame.com/api/player'
        self.api2_url = 'https://gof-report-api-formal.centurygame.com/api/player'
        self.secret = 'tB87#kPtkxqOS2'
        
        # Rate limiting for login operations
        self.api1_requests = []  # Timestamps of API1 requests
        self.api2_requests = []  # Timestamps of API2 requests
        self.rate_limit_per_api = 30
        self.rate_limit_window = 60  # seconds
        self.last_api_used = 1
        
        # API availability
        self.dual_api_mode = False
        self.available_apis = []
        self.request_delay = 2.0  # Default for single API
        
        # Alliance operation locks to prevent conflicts
        self.alliance_locks = {}
        
        # Centralized operation queue
        self.operation_queue = asyncio.Queue()
        self.operation_lock = asyncio.Lock()
        self.current_operation = None
        self.queue_processor_task = None
        
        # SSL context (reusable)
        self.ssl_context = self._create_ssl_context()
        
        # Logging
        self.log_directory = 'log'
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)
        self.log_file = os.path.join(self.log_directory, 'login_handler.txt')
        
        # Mark as initialized
        self._initialized = True
    
    def _create_ssl_context(self):
        """Create reusable SSL context"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context
    
    def log_message(self, message: str):
        """Log a message with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
    
    def get_alliance_lock(self, alliance_id: str) -> asyncio.Lock:
        """Get or create alliance-specific lock"""
        if alliance_id not in self.alliance_locks:
            self.alliance_locks[alliance_id] = asyncio.Lock()
        return self.alliance_locks[alliance_id]
    
    async def check_apis_availability(self, test_fid: str = "46765089") -> Dict[str, bool]:
        """
        Check which login APIs are available
        Returns: dict with api1_available, api2_available
        """
        api_status = {
            "api1_available": False,
            "api2_available": False,
            "api1_url": self.api1_url,
            "api2_url": self.api2_url
        }
        
        connector = aiohttp.TCPConnector(ssl=self.ssl_context)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            # Test API 1
            try:
                current_time = int(time.time() * 1000)
                form = f"fid={test_fid}&time={current_time}"
                sign = hashlib.md5((form + self.secret).encode('utf-8')).hexdigest()
                form = f"sign={sign}&{form}"
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
                
                async with session.post(self.api1_url, headers=headers, data=form, timeout=5) as response:
                    # API is available if we get 200 (success) or 429 (rate limit)
                    api_status["api1_available"] = response.status in [200, 429]
                    self.log_message(f"API1 availability check: Status {response.status}")
            except Exception as e:
                self.log_message(f"API1 availability check failed: {str(e)}")
                api_status["api1_available"] = False
            
            # Test API 2
            try:
                current_time = int(time.time() * 1000)
                form = f"fid={test_fid}&time={current_time}"
                sign = hashlib.md5((form + self.secret).encode('utf-8')).hexdigest()
                form = f"sign={sign}&{form}"
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
                
                async with session.post(self.api2_url, headers=headers, data=form, timeout=5) as response:
                    api_status["api2_available"] = response.status in [200, 429]
                    self.log_message(f"API2 availability check: Status {response.status}")
            except Exception as e:
                self.log_message(f"API2 availability check failed: {str(e)}")
                api_status["api2_available"] = False
        
        # Update configuration based on availability
        if api_status["api1_available"] and api_status["api2_available"]:
            self.dual_api_mode = True
            self.available_apis = [1, 2]
            self.request_delay = 1.0  # 1 second delay for dual mode
        elif api_status["api1_available"]:
            self.dual_api_mode = False
            self.available_apis = [1]
            self.request_delay = 2.0  # 2 seconds for single API
        elif api_status["api2_available"]:
            self.dual_api_mode = False
            self.available_apis = [2]
            self.request_delay = 2.0
        else:
            self.available_apis = []
        
        return api_status
    
    def _get_available_api(self) -> Optional[int]:
        """
        Determine which API to use based on rate limits
        Returns: API number (1 or 2) or None if both at limit
        """
        now = time.time()
        
        # Clean old requests outside the rate limit window
        self.api1_requests = [t for t in self.api1_requests if now - t < self.rate_limit_window]
        self.api2_requests = [t for t in self.api2_requests if now - t < self.rate_limit_window]
        
        if not self.dual_api_mode:
            # Single API mode
            api_num = self.available_apis[0] if self.available_apis else 1
            requests = self.api1_requests if api_num == 1 else self.api2_requests
            
            if len(requests) < self.rate_limit_per_api:
                return api_num
            else:
                # Calculate wait time until oldest request expires
                wait_time = self.rate_limit_window - (now - requests[0]) if requests else 0
                return None, max(0, wait_time)
        else:
            # Dual API mode - intelligent switching
            api1_available = 1 in self.available_apis and len(self.api1_requests) < self.rate_limit_per_api
            api2_available = 2 in self.available_apis and len(self.api2_requests) < self.rate_limit_per_api
            
            if api1_available and api2_available:
                # Both available - alternate or use the one with more capacity
                if self.last_api_used == 1:
                    return 2
                else:
                    return 1
            elif api1_available:
                return 1
            elif api2_available:
                return 2
            else:
                # Both at limit - calculate minimum wait time
                wait_time1 = self.rate_limit_window - (now - self.api1_requests[0]) if self.api1_requests else 0
                wait_time2 = self.rate_limit_window - (now - self.api2_requests[0]) if self.api2_requests else 0
                min_wait = min(wait_time1, wait_time2)
                return None, max(0, min_wait)
    
    def _record_api_request(self, api_num: int):
        """Record timestamp of API request"""
        now = time.time()
        if api_num == 1:
            self.api1_requests.append(now)
        else:
            self.api2_requests.append(now)
        self.last_api_used = api_num
    
    def _get_wait_time(self) -> float:
        """Calculate wait time when both APIs are at limit"""
        now = time.time()
        wait_time1 = self.rate_limit_window - (now - self.api1_requests[0]) if self.api1_requests else 0
        wait_time2 = self.rate_limit_window - (now - self.api2_requests[0]) if self.api2_requests else 0
        return max(0, min(wait_time1, wait_time2))
    
    async def fetch_player_data(self, fid: str, use_proxy: Optional[str] = None) -> Dict:
        """
        Fetch player login data (nickname, furnace level, kid, etc.)
        
        Args:
            fid: Player ID
            use_proxy: Optional proxy URL for fallback
            
        Returns:
            {
                'status': 'success' | 'error' | 'rate_limited' | 'not_found',
                'data': {
                    'nickname': str,
                    'stove_lv': int,
                    'stove_lv_content': str,
                    'kid': str,
                    # ... other player data
                } | None,
                'api_used': 1 | 2,
                'error_message': str | None
            }
        """
        # Check rate limits and get available API
        api_result = self._get_available_api()
        
        if api_result is None or (isinstance(api_result, tuple) and api_result[0] is None):
            # Both APIs at limit
            wait_time = api_result[1] if isinstance(api_result, tuple) else self._get_wait_time()
            return {
                'status': 'rate_limited',
                'data': None,
                'wait_time': wait_time,
                'error_message': f'Rate limit reached. Wait {wait_time:.1f} seconds.'
            }
        
        # Get the API to use
        api_num = api_result if isinstance(api_result, int) else api_result
        api_url = self.api1_url if api_num == 1 else self.api2_url
        
        # Prepare request
        current_time = int(time.time() * 1000)
        form = f"fid={fid}&time={current_time}"
        sign = hashlib.md5((form + self.secret).encode('utf-8')).hexdigest()
        form = f"sign={sign}&{form}"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        
        try:
            # Use proxy if provided and main request fails
            if use_proxy:
                from aiohttp_socks import ProxyConnector
                connector = ProxyConnector.from_url(use_proxy, ssl=self.ssl_context)
            else:
                connector = aiohttp.TCPConnector(ssl=self.ssl_context)
            
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(api_url, headers=headers, data=form) as response:
                    # Record the API request
                    self._record_api_request(api_num)
                    
                    if response.status == 200:
                        data = await response.json()
                        
                        # Check if we have valid data
                        if data.get('data'):
                            return {
                                'status': 'success',
                                'data': data['data'],
                                'api_used': api_num,
                                'error_message': None
                            }
                        
                        # Check if this is specifically error 40004 (role not exist)
                        elif data.get('err_code') == 40004:
                            return {
                                'status': 'not_found',
                                'data': None,
                                'api_used': api_num,
                                'error_message': 'Player does not exist (role not exist)',
                                'err_code': 40004
                            }
                        
                        # Other cases where data is empty but not error 40004
                        else:
                            err_code = data.get('err_code', 'unknown')
                            err_msg = data.get('msg', 'Unknown error')
                            return {
                                'status': 'error',
                                'data': None,
                                'api_used': api_num,
                                'error_message': f'API Error {err_code}: {err_msg}',
                                'err_code': err_code
                            }
                    elif response.status == 429:
                        # This shouldn't happen with our rate limiting, but handle it
                        return {
                            'status': 'rate_limited',
                            'data': None,
                            'api_used': api_num,
                            'error_message': 'Unexpected rate limit'
                        }
                    else:
                        return {
                            'status': 'error',
                            'data': None,
                            'api_used': api_num,
                            'error_message': f'HTTP {response.status}'
                        }
                        
        except Exception as e:
            self.log_message(f"Error fetching player data for ID {fid}: {str(e)}")
            return {
                'status': 'error',
                'data': None,
                'api_used': api_num,
                'error_message': str(e)
            }
    
    async def fetch_player_batch(self, fids: List[str], progress_callback: Optional[Callable] = None, 
                               alliance_id: Optional[str] = None) -> List[Dict]:
        """
        Fetch multiple players efficiently with progress updates
        
        Args:
            fids: List of player IDs
            progress_callback: async function(current, total, status_msg)
            alliance_id: Alliance ID for locking (optional)
            
        Returns:
            List of results in same format as fetch_player_data
        """
        results = []
        total = len(fids)
        
        # Use alliance lock if provided
        if alliance_id:
            async with self.get_alliance_lock(alliance_id):
                return await self._fetch_batch_internal(fids, progress_callback, total)
        else:
            return await self._fetch_batch_internal(fids, progress_callback, total)
    
    async def _fetch_batch_internal(self, fids: List[str], progress_callback: Optional[Callable], 
                                  total: int) -> List[Dict]:
        """Internal method to fetch batch of players"""
        results = []
        
        for i, fid in enumerate(fids):
            # Update progress
            if progress_callback:
                await progress_callback(i + 1, total, f"Fetching player {i + 1}/{total}")
            
            # Fetch player data
            result = await self.fetch_player_data(fid)
            results.append(result)
            
            # Handle rate limiting
            if result['status'] == 'rate_limited':
                wait_time = result.get('wait_time', 60)
                if progress_callback:
                    await progress_callback(i + 1, total, f"Rate limited. Waiting {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                
                # Retry after wait
                result = await self.fetch_player_data(fid)
                results[-1] = result
            
            # Add delay between requests
            if i < total - 1:  # Don't delay after last request
                await asyncio.sleep(self.request_delay)
        
        return results
    
    def get_mode_text(self) -> str:
        """Get human-readable description of current API mode"""
        if self.dual_api_mode:
            return "✅ Dual-API mode active (1 member/second)"
        elif self.available_apis:
            api_num = self.available_apis[0]
            return f"⚠️ Single-API mode (1 member/2 seconds) - API {3-api_num} unavailable"
        else:
            return "❌ No APIs available"
    
    def get_processing_rate(self) -> str:
        """Get user-friendly processing rate"""
        if self.dual_api_mode:
            return "⚡ Rate: 1 member/second"
        elif self.available_apis:
            return "⚡ Rate: 1 member/2 seconds"
        else:
            return "❌ Service unavailable"
    
    def get_rate_limit_info(self) -> Dict[str, int]:
        """Get current rate limit information"""
        now = time.time()
        self.api1_requests = [t for t in self.api1_requests if now - t < self.rate_limit_window]
        self.api2_requests = [t for t in self.api2_requests if now - t < self.rate_limit_window]
        
        return {
            'api1_used': len(self.api1_requests),
            'api1_remaining': self.rate_limit_per_api - len(self.api1_requests),
            'api2_used': len(self.api2_requests),
            'api2_remaining': self.rate_limit_per_api - len(self.api2_requests),
            'total_available': (self.rate_limit_per_api - len(self.api1_requests)) + 
                             (self.rate_limit_per_api - len(self.api2_requests)) if self.dual_api_mode else
                             (self.rate_limit_per_api - len(self.api1_requests if 1 in self.available_apis else self.api2_requests))
        }
    
    async def start_queue_processor(self):
        """Start the queue processor if not already running"""
        if not self.queue_processor_task or self.queue_processor_task.done():
            self.queue_processor_task = asyncio.create_task(self._process_operation_queue())
            self.log_message("Queue processor started")
    
    async def queue_operation(self, operation_info: Dict) -> int:
        """
        Queue an operation and return its position
        operation_info should contain:
        - type: 'member_addition' | 'alliance_control' | 'gift_code' etc
        - callback: async function to execute
        - description: string description
        - alliance_id: optional alliance ID for locking
        - interaction: discord interaction for status updates
        """
        # Mark if this operation will be queued (not the first)
        current_size = self.operation_queue.qsize()
        operation_info['was_queued'] = current_size > 0
        
        await self.operation_queue.put(operation_info)
        queue_size = self.operation_queue.qsize()
        self.log_message(f"Operation queued: {operation_info['description']} (Position: {queue_size})")
        
        # Start processor if not running
        await self.start_queue_processor()
        
        return queue_size
    
    async def _process_operation_queue(self):
        """Process queued operations one at a time"""
        self.log_message("Queue processor starting...")
        
        while True:
            try:
                # Wait for an operation
                operation = await self.operation_queue.get()
                self.current_operation = operation
                
                self.log_message(f"Processing operation: {operation['description']}")
                
                try:
                    # Use alliance lock if specified
                    if operation.get('alliance_id'):
                        async with self.get_alliance_lock(str(operation['alliance_id'])):
                            await operation['callback']()
                    else:
                        await operation['callback']()
                    
                    self.log_message(f"Operation completed: {operation['description']}")
                    
                except Exception as e:
                    self.log_message(f"Operation failed: {operation['description']} - Error: {str(e)}")
                    # Send error message if interaction is available
                    if operation.get('interaction'):
                        try:
                            await operation['interaction'].followup.send(
                                f"❌ Operation failed: {str(e)}", ephemeral=True
                            )
                        except:
                            pass
                
                finally:
                    self.current_operation = None
                    self.operation_queue.task_done()
                
            except asyncio.CancelledError:
                self.log_message("Queue processor cancelled")
                break
            except Exception as e:
                self.log_message(f"Queue processor error: {str(e)}")
                await asyncio.sleep(1)  # Prevent tight loop on error
    
    def get_queue_info(self) -> Dict:
        """Get current queue status"""
        return {
            'queue_size': self.operation_queue.qsize(),
            'current_operation': self.current_operation,
            'is_processing': self.current_operation is not None
        }