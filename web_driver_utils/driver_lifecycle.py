"""
driver_lifecycle.py - Module quản lý vòng đời của WebDriver
Version: 2.0.0 (Production)

Tính năng chính:
- Khởi tạo và quản lý WebDriver kết hợp với GenLogin
- Xử lý các tình huống đặc biệt: kết nối GenLogin, chạy profile, xử lý lỗi execution context
- Quản lý vòng đời của driver với các bước được tách biệt rõ ràng
- Cơ chế retry thông minh cho từng bước khởi tạo
- Tối ưu hóa quản lý tài nguyên và xử lý lỗi
"""

import os
import time
import gc
import logging
import random
import socket
import functools
from typing import Optional, Tuple, Dict, Union, Any, List, Callable, Type, TypeVar, NamedTuple, dataclass
from dataclasses import dataclass, field

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    WebDriverException, TimeoutException, InvalidSessionIdException,
    NoSuchWindowException, StaleElementReferenceException, SessionNotCreatedException
)

# Import từ các module đã tách
from driver_init import check_driver_path, create_chrome_options
from driver_maintenance import verify_driver_connection, close_current_driver, _cleanup_orphaned_chrome_processes
from genlogin_profile import GenloginProfileManager, ProfileRunningError

# Import từ global_state nếu có
try:
    from global_state import request_shutdown, is_shutdown_requested
except ImportError:
    # Fallback nếu không có module global_state
    def is_shutdown_requested():
        return False
    
    def request_shutdown():
        pass

# Thiết lập logger
logger = logging.getLogger(__name__)

# -------------------------------
# Configuration Classes
# -------------------------------
@dataclass
class TimeoutConfig:
    """Lớp quản lý tập trung các cấu hình timeout."""
    page_load: int = 30
    script: int = 30
    implicit_wait: int = 10
    connection: int = 10
    profile_startup: int = 30
    window_check_delay: float = 2.0
    context_check_interval: float = 1.0
    
    def as_dict(self) -> Dict[str, Any]:
        """Trả về tất cả cấu hình dưới dạng dict."""
        return {
            'page_load': self.page_load,
            'script': self.script,
            'implicit_wait': self.implicit_wait,
            'connection': self.connection,
            'profile_startup': self.profile_startup,
            'window_check_delay': self.window_check_delay,
            'context_check_interval': self.context_check_interval
        }

@dataclass
class RetryConfig:
    """Lớp quản lý cấu hình cho cơ chế retry."""
    count: int = 3
    backoff_factor: float = 2.0
    jitter: bool = True
    base_delay: float = 1.0
    
    def as_dict(self) -> Dict[str, Any]:
        """Trả về tất cả cấu hình dưới dạng dict."""
        return {
            'count': self.count,
            'backoff_factor': self.backoff_factor,
            'jitter': self.jitter,
            'base_delay': self.base_delay
        }

@dataclass
class ResourceOptions:
    """Lớp quản lý cấu hình cho quản lý tài nguyên."""
    disable_cache: bool = True
    force_kill_on_error: bool = True
    cleanup_orphaned: bool = True
    memory_optimization: bool = True
    
    def as_dict(self) -> Dict[str, Any]:
        """Trả về tất cả cấu hình dưới dạng dict."""
        return {
            'disable_cache': self.disable_cache,
            'force_kill_on_error': self.force_kill_on_error,
            'cleanup_orphaned': self.cleanup_orphaned,
            'memory_optimization': self.memory_optimization
        }

@dataclass
class GenLoginConfig:
    """Lớp quản lý cấu hình kết nối GenLogin."""
    api_key: str = ""
    base_url: str = "http://localhost:55550/backend"
    force_stop_running: bool = True
    verify_ws_endpoint: bool = True
    
    def as_dict(self) -> Dict[str, Any]:
        """Trả về tất cả cấu hình dưới dạng dict."""
        return {
            'api_key': self.api_key,
            'base_url': self.base_url,
            'force_stop_running': self.force_stop_running,
            'verify_ws_endpoint': self.verify_ws_endpoint
        }

@dataclass
class DriverInitOptions:
    """Lớp quản lý tất cả cấu hình để khởi tạo driver."""
    chrome_driver_path: str
    profile_id: Optional[str] = None
    genlogin: GenLoginConfig = field(default_factory=GenLoginConfig)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    resources: ResourceOptions = field(default_factory=ResourceOptions)
    execution_context_check: bool = True

# -------------------------------
# Exceptions
# -------------------------------
class DriverInitError(Exception):
    """Ngoại lệ khi khởi tạo driver thất bại."""
    pass

class ExecutionContextError(DriverInitError):
    """Ngoại lệ cụ thể khi gặp lỗi No Such Execution Context."""
    pass

class GenloginConnectionError(DriverInitError):
    """Ngoại lệ khi kết nối GenLogin thất bại."""
    pass

class ProfileStartupError(DriverInitError):
    """Ngoại lệ khi khởi động profile thất bại."""
    pass

# -------------------------------
# Decorator
# -------------------------------
def retry_with_backoff(max_retries=3, initial_delay=1.0, backoff_factor=2.0, jitter=True, 
                      error_types=(Exception,), logger_func=None):
    """
    Decorator để thực thi hàm với cơ chế retry và exponential backoff.
    
    Args:
        max_retries: Số lần thử lại tối đa
        initial_delay: Thời gian chờ ban đầu giữa các lần thử (giây)
        backoff_factor: Hệ số tăng thời gian chờ
        jitter: Thêm nhiễu ngẫu nhiên vào thời gian chờ
        error_types: Tuple các loại lỗi cần xử lý
        logger_func: Hàm logger tùy chọn
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log = logger_func or logger.warning
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except error_types as e:
                    last_exception = e
                    
                    # Kiểm tra cờ shutdown trước khi retry
                    if is_shutdown_requested():
                        log(f"Đã phát hiện yêu cầu shutdown, hủy retry của hàm {func.__name__}")
                        break
                    
                    # Tính thời gian chờ với backoff và jitter (nếu có)
                    delay = initial_delay * (backoff_factor ** attempt)
                    if jitter:
                        delay *= random.uniform(0.8, 1.2)
                    
                    if attempt < max_retries - 1:
                        log(f"Lỗi khi thực thi {func.__name__} (lần {attempt+1}/{max_retries}): {str(e)}. Thử lại sau {delay:.2f}s...")
                        time.sleep(delay)
                    else:
                        log(f"Lỗi khi thực thi {func.__name__} (lần {attempt+1}/{max_retries}): {str(e)}. Đã hết số lần thử.")
                        
            # Nếu đã hết số lần thử, ném ngoại lệ đã bắt được cuối cùng
            if last_exception:
                raise last_exception
            
            # Nếu không có ngoại lệ nào được bắt nhưng vẫn thất bại
            raise RuntimeError(f"Hàm {func.__name__} thất bại sau {max_retries} lần thử với lỗi không xác định")
            
        return wrapper
    return decorator

# -------------------------------
# Helper Functions
# -------------------------------
def get_retry_config_from_options(options: Optional[Union[RetryConfig, Dict[str, Any], None]] = None) -> RetryConfig:
    """
    Tạo RetryConfig từ nhiều loại đầu vào khác nhau.
    
    Args:
        options: Có thể là RetryConfig, Dict, hoặc None
        
    Returns:
        RetryConfig: Cấu hình retry đã chuẩn hóa
    """
    if options is None:
        return RetryConfig()
    
    if isinstance(options, RetryConfig):
        return options
    
    if isinstance(options, dict):
        return RetryConfig(
            count=options.get('count', 3),
            backoff_factor=options.get('backoff_factor', 2.0),
            jitter=options.get('jitter', True),
            base_delay=options.get('base_delay', 1.0)
        )
    
    return RetryConfig()

def _check_execution_context(driver, detail_level=0, retries=2):
    """
    Kiểm tra execution context của driver với nhiều mức độ chi tiết.
    
    Args:
        driver: WebDriver cần kiểm tra
        detail_level: Mức độ chi tiết của kiểm tra (0=cơ bản, 1=trung bình, 2=đầy đủ)
        retries: Số lần thử lại nếu gặp lỗi
        
    Returns:
        bool: True nếu execution context hoạt động tốt, False nếu không
        
    Raises:
        ExecutionContextError: Nếu phát hiện lỗi "No such execution context" và hết số lần thử
    """
    if not driver:
        return False
        
    for attempt in range(retries):
        try:
            if detail_level == 0:
                # Thử thực thi JavaScript đơn giản nhất
                result = driver.execute_script("return true;")
                if result is not True:
                    logger.debug("Kiểm tra execution context không thành công: kết quả không phải True")
                    if attempt < retries - 1:
                        time.sleep(1.0)
                        continue
                    return False
                
                logger.debug("Kiểm tra execution context cơ bản OK")
                return True
                
            elif detail_level == 1:
                # Thử thực thi JavaScript cơ bản
                result = driver.execute_script("""
                    return {
                        url: window.location.href,
                        title: document.title,
                        readyState: document.readyState,
                        timestamp: new Date().toISOString()
                    };
                """)
                
                if not result or 'readyState' not in result:
                    logger.debug("Kiểm tra execution context không thành công: kết quả không hợp lệ")
                    if attempt < retries - 1:
                        time.sleep(1.0)
                        continue
                    return False
                
                logger.debug(f"Kiểm tra execution context trung bình OK: {result.get('readyState', 'unknown')}")
                return True
                
            else:
                # Thử thực thi JavaScript phức tạp
                result = driver.execute_script("""
                    // Tạo và thao tác với DOM
                    var testDiv = document.createElement('div');
                    testDiv.id = 'test-div-' + Date.now();
                    document.body.appendChild(testDiv);
                    
                    // Thực hiện tính toán
                    var sum = 0;
                    for (var i = 0; i < 100; i++) {
                        sum += i;
                    }
                    
                    // Cleanup
                    document.body.removeChild(testDiv);
                    
                    return {
                        url: location.href,
                        title: document.title,
                        readyState: document.readyState,
                        timestamp: Date.now(),
                        sum: sum
                    };
                """)
                
                if not result or 'sum' not in result or result['sum'] != 4950:
                    logger.debug("Kiểm tra execution context chi tiết không thành công: kết quả tính toán không đúng")
                    if attempt < retries - 1:
                        time.sleep(1.0)
                        continue
                    return False
                
                logger.debug("Kiểm tra execution context chi tiết OK")
                return True
                
        except Exception as e:
            error_msg = str(e).lower()
            if "no such execution context" in error_msg:
                logger.warning(f"Lỗi No Such Execution Context (lần thử {attempt+1}/{retries})")
                if attempt < retries - 1:
                    time.sleep(1.0)
                    continue
                raise ExecutionContextError(f"Lỗi No Such Execution Context: {str(e)}")
            else:
                logger.warning(f"Lỗi khi kiểm tra execution context: {str(e)} (lần thử {attempt+1}/{retries})")
                if attempt < retries - 1:
                    time.sleep(1.0)
                    continue
                return False
    return False

def _verify_driver_health(driver, max_wait=10, execution_context_level=1):
    """
    Kiểm tra sức khỏe driver toàn diện.
    
    Args:
        driver: WebDriver cần kiểm tra
        max_wait: Thời gian tối đa chờ đợi (giây)
        execution_context_level: Mức độ kiểm tra execution context (0-2)
        
    Returns:
        bool: True nếu driver khỏe mạnh, False nếu không
    """
    if not driver:
        return False
        
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            # Kiểm tra cơ bản
            handles = driver.window_handles
            if not handles:
                logger.debug("Không có cửa sổ nào")
                time.sleep(1)
                continue
            
            # Kiểm tra URL
            current_url = driver.current_url
            
            # Kiểm tra JavaScript execution context
            return _check_execution_context(driver, detail_level=execution_context_level)
            
        except NoSuchWindowException as e:
            logger.warning(f"Lỗi No Such Window trong kiểm tra sức khỏe: {str(e)}")
            return False
        except Exception as e:
            if "no such execution context" in str(e).lower():
                logger.warning(f"Lỗi No Such Execution Context trong kiểm tra sức khỏe: {str(e)}")
            else:
                logger.warning(f"Lỗi trong kiểm tra sức khỏe driver: {type(e).__name__}: {str(e)}")
            time.sleep(1)
    
    logger.warning(f"Kiểm tra sức khỏe driver thất bại sau {max_wait}s")
    return False

class GenloginConnector:
    """
    Lớp kết nối và quản lý với GenLogin API.
    """
    
    def __init__(self, api_key="", base_url="http://localhost:55550/backend", retry_config=None):
        """
        Khởi tạo connector với GenLogin API.
        
        Args:
            api_key: API key GenLogin (để trống nếu sử dụng local)
            base_url: URL cơ sở cho GenLogin API
            retry_config: Cấu hình retry
        """
        self.api_key = api_key
        self.base_url = base_url
        self.retry_config = get_retry_config_from_options(retry_config)
        self.gen = None
        
    def connect(self, connect_timeout=10):
        """
        Kết nối với GenLogin API và trả về instance.
        
        Args:
            connect_timeout: Timeout khi kết nối với GenLogin API (giây)
            
        Returns:
            Genlogin: Instance GenLogin API đã kết nối
            
        Raises:
            GenloginConnectionError: Nếu không thể kết nối với GenLogin API
        """
        from Genlogin import Genlogin
        
        logger.info("Khởi tạo GenLogin API client...")
        self.gen = Genlogin(self.api_key, base_url=self.base_url)
        
        # Đặt timeout cho request của GenLogin
        if hasattr(self.gen, 'request_timeout'):
            self.gen.request_timeout = connect_timeout
        
        # Kiểm tra API GenLogin hoạt động bằng cách lấy danh sách profiles
        logger.debug("Kiểm tra kết nối GenLogin bằng cách lấy profiles...")
        start_time = time.time()
        
        try:
            test_profiles = self.gen.getProfiles(0, 1)
            elapsed_time = time.time() - start_time
            
            # Kiểm tra kết quả trả về có hợp lệ không
            if not isinstance(test_profiles, dict) or "profiles" not in test_profiles:
                error_msg = f"API GenLogin trả về định dạng không hợp lệ: {test_profiles}"
                logger.error(error_msg)
                raise GenloginConnectionError(error_msg)
            
            # Kiểm tra dữ liệu profiles có tồn tại không
            if not test_profiles.get("profiles") and not test_profiles.get("profiles") == []:
                error_msg = "API GenLogin không trả về danh sách profiles"
                logger.error(error_msg)
                raise GenloginConnectionError(error_msg)
            
            logger.info(f"Kết nối GenLogin API thành công (thời gian: {elapsed_time:.2f}s)")
            return self.gen
            
        except Exception as e:
            error_msg = f"Không thể kết nối với GenLogin API: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            raise GenloginConnectionError(error_msg)
    
    @retry_with_backoff(max_retries=3, error_types=(Exception,))
    def connect_with_retry(self, connect_timeout=10):
        """
        Kết nối với GenLogin API với cơ chế retry.
        
        Args:
            connect_timeout: Timeout khi kết nối với GenLogin API (giây)
            
        Returns:
            Genlogin: Instance GenLogin API đã kết nối
            
        Raises:
            GenloginConnectionError: Nếu không thể kết nối với GenLogin API sau nhiều lần thử
        """
        return self.connect(connect_timeout)
        
    def select_profile_id(self, profile_id=None):
        """
        Lựa chọn profile ID. Nếu profile_id là None, sẽ chọn profile đầu tiên có sẵn.
        
        Args:
            profile_id: ID profile (None để tự động chọn)
            
        Returns:
            str: ID profile đã chọn
            
        Raises:
            GenloginConnectionError: Nếu không tìm thấy profile nào
        """
        if not self.gen:
            raise GenloginConnectionError("Chưa kết nối với GenLogin API")
            
        if profile_id:
            logger.info(f"Sử dụng profile ID đã chỉ định: {profile_id}")
            return profile_id
        
        try:
            logger.info("Lấy danh sách profiles từ GenLogin...")
            # Đặt timeout ngắn cho request lấy profiles
            if hasattr(self.gen, 'request_timeout'):
                original_timeout = self.gen.request_timeout
                self.gen.request_timeout = 10  # 10 giây cho request lấy profiles
            
            try:
                profiles_data = self.gen.getProfiles(0, 10)
            finally:
                # Khôi phục timeout gốc
                if hasattr(self.gen, 'request_timeout') and 'original_timeout' in locals():
                    self.gen.request_timeout = original_timeout
            
            profiles = profiles_data.get("profiles", [])
            if not profiles:
                logger.error("Không tìm thấy profile GenLogin nào!")
                raise GenloginConnectionError("Không tìm thấy profile GenLogin nào!")
            
            logger.debug(f"Tìm thấy {len(profiles)} profiles")
            
            # Kiểm tra các profiles đang chạy
            try:
                logger.info("Kiểm tra các profiles đang chạy...")
                # Đặt timeout ngắn cho request
                if hasattr(self.gen, 'request_timeout'):
                    original_timeout = self.gen.request_timeout
                    self.gen.request_timeout = 8  # Timeout ngắn hơn cho request này
                
                try:
                    running_profiles = self.gen.getProfilesRunning()
                finally:
                    # Khôi phục timeout gốc
                    if hasattr(self.gen, 'request_timeout') and 'original_timeout' in locals():
                        self.gen.request_timeout = original_timeout
                    
                items = running_profiles.get("data", {}).get("items", [])
                running_ids = [p.get("id") for p in items if isinstance(p, dict)]
                
                logger.debug(f"Profiles đang chạy: {len(running_ids)}")
                
                # Lọc các profile không đang chạy
                available_profiles = [p for p in profiles if p["id"] not in running_ids]
                
                # Nếu có profile khả dụng, chọn profile đầu tiên
                if available_profiles:
                    profile_id = available_profiles[0]["id"]
                    logger.info(f"Dùng profile khả dụng ID={profile_id}")
                else:
                    profile_id = profiles[0]["id"]
                    logger.info(f"Tất cả profile đều đang chạy, dùng ID={profile_id}")
            except Exception as running_e:
                # Xử lý lỗi khi không thể lấy danh sách profiles đang chạy
                logger.warning(f"Không thể lấy danh sách profiles đang chạy: {str(running_e)}")
                profile_id = profiles[0]["id"]
                logger.info(f"Chọn profile đầu tiên ID={profile_id} (không kiểm tra trạng thái)")
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách profile: {type(e).__name__}: {str(e)}")
            raise GenloginConnectionError(f"Lỗi khi lấy danh sách profile: {str(e)}")
        
        return profile_id
        
    def ensure_profile_stopped(self, profile_id, force_stop_running=True, cleanup_orphaned=True):
        """
        Đảm bảo profile đã dừng trước khi khởi chạy. Nếu profile đang chạy, dừng nếu force_stop_running=True.
        
        Args:
            profile_id: ID profile cần kiểm tra
            force_stop_running: Dừng profile đang chạy nếu True
            cleanup_orphaned: Dọn dẹp các tiến trình Chrome bị treo nếu True
            
        Returns:
            bool: True nếu profile đã dừng thành công hoặc không đang chạy
            
        Raises:
            ProfileRunningError: Nếu profile đang chạy và force_stop_running=False
        """
        if not self.gen:
            raise GenloginConnectionError("Chưa kết nối với GenLogin API")
            
        profile_was_running = False
        try:
            logger.info(f"Kiểm tra xem profile {profile_id} có đang chạy không...")
            # Đặt timeout ngắn cho request
            if hasattr(self.gen, 'request_timeout'):
                original_timeout = self.gen.request_timeout
                self.gen.request_timeout = 8  # Timeout ngắn hơn cho request này
            
            try:
                # Thử cách 1: Sử dụng getProfilesRunning
                running_profiles = self.gen.getProfilesRunning()
                items = running_profiles.get("data", {}).get("items", [])
                running_ids = [p.get("id") for p in items if isinstance(p, dict)]
                profile_running = profile_id in running_ids
                
                # Nếu cách 1 không phát hiện, thử cách 2
                if not profile_running:
                    logger.debug("Thử kiểm tra bằng phương pháp getWsEndpoint...")
                    try:
                        ws_data = self.gen.getWsEndpoint(profile_id)
                        if ws_data.get("success", False) and ws_data.get("data", {}).get("wsEndpoint"):
                            profile_running = True
                            profile_was_running = True
                            logger.debug("Phát hiện profile đang chạy qua getWsEndpoint")
                    except Exception as ws_e:
                        logger.debug(f"Lỗi khi kiểm tra getWsEndpoint: {str(ws_e)}")
                        # Bỏ qua lỗi, tiếp tục với kết quả từ getProfilesRunning
            except Exception as method_e:
                logger.warning(f"Lỗi khi kiểm tra profiles đang chạy: {str(method_e)}")
                # Giả định là không chạy để tiếp tục
                profile_running = False
            finally:
                # Khôi phục timeout gốc
                if hasattr(self.gen, 'request_timeout') and 'original_timeout' in locals():
                    self.gen.request_timeout = original_timeout
            
            if profile_running:
                logger.warning(f"Profile {profile_id} đang chạy")
                profile_was_running = True
                
                if force_stop_running:
                    # Dừng profile đang chạy và đợi
                    logger.info(f"Thử dừng profile {profile_id} đang chạy...")
                    
                    # Đặt timeout ngắn cho request dừng profile
                    if hasattr(self.gen, 'request_timeout'):
                        original_timeout = self.gen.request_timeout
                        self.gen.request_timeout = 10
                    
                    try:
                        stop_result = self.gen.stopProfile(profile_id)
                        stop_success = stop_result.get("success", False)
                        
                        if stop_success:
                            logger.info(f"Lệnh dừng profile {profile_id} đã được gửi thành công")
                        else:
                            error_msg = stop_result.get("message", "Không rõ lỗi")
                            logger.warning(f"Không thể dừng profile {profile_id}: {error_msg}")
                    except Exception as stop_e:
                        logger.warning(f"Lỗi khi gửi lệnh dừng profile: {str(stop_e)}")
                        stop_success = False
                    finally:
                        # Khôi phục timeout gốc
                        if hasattr(self.gen, 'request_timeout') and 'original_timeout' in locals():
                            self.gen.request_timeout = original_timeout
                    
                    # Đợi profile dừng hoàn toàn
                    max_stop_wait = 15  # 15 giây là thời gian đợi tối đa
                    stop_attempts = 0
                    profile_stopped = False
                    
                    logger.info(f"Đợi profile {profile_id} dừng hoàn toàn (tối đa {max_stop_wait}s)...")
                    while stop_attempts < max_stop_wait:
                        try:
                            # Kiểm tra lại xem profile có còn chạy không
                            running_check = self.gen.getProfilesRunning()
                            running_ids = [p.get("id") for p in running_check.get("data", {}).get("items", []) 
                                          if isinstance(p, dict)]
                            
                            if profile_id not in running_ids:
                                logger.info(f"Profile {profile_id} đã dừng thành công sau {stop_attempts}s")
                                profile_stopped = True
                                
                                # Tăng thời gian chờ từ 2.0 lên 5.0 giây
                                additional_wait = 5.0
                                logger.debug(f"Đợi thêm {additional_wait}s để đảm bảo tài nguyên được giải phóng...")
                                time.sleep(additional_wait)
                                
                                # Kiểm tra và dọn dẹp tiến trình Chrome còn sót lại
                                if cleanup_orphaned:
                                    logger.info("Kiểm tra triệt để các tiến trình Chrome liên quan...")
                                    _cleanup_orphaned_chrome_processes(profile_id)
                                
                                break
                        except Exception as check_e:
                            logger.debug(f"Lỗi khi kiểm tra trạng thái profile: {str(check_e)}")
                            # Bỏ qua lỗi, tiếp tục đợi
                        
                        time.sleep(1)
                        stop_attempts += 1
                    
                    # Nếu vẫn không dừng được sau khi đợi tối đa
                    if not profile_stopped:
                        logger.warning(f"Không thể dừng profile {profile_id} sau {max_stop_wait}s")
                        logger.info("Thử dùng lệnh stopBrowser mạnh hơn...")
                        
                        try:
                            # Đặt timeout ngắn cho request
                            if hasattr(self.gen, 'request_timeout'):
                                original_timeout = self.gen.request_timeout
                                self.gen.request_timeout = 10
                            
                            # Dùng lệnh stopBrowser mạnh hơn
                            stop_browser_result = self.gen.stopBrowser(profile_id)
                            logger.debug(f"Kết quả stopBrowser: {stop_browser_result}")
                            
                            # Khôi phục timeout gốc
                            if hasattr(self.gen, 'request_timeout') and 'original_timeout' in locals():
                                self.gen.request_timeout = original_timeout
                            
                            # Đợi thêm sau khi dùng lệnh mạnh
                            logger.info("Đợi thêm sau khi dùng lệnh stopBrowser...")
                            time.sleep(3)
                            
                            # Dọn dẹp các tiến trình Chrome treo liên quan đến profile
                            if cleanup_orphaned:
                                _cleanup_orphaned_chrome_processes(profile_id)
                        except Exception as sb_e:
                            logger.warning(f"Lỗi khi gọi stopBrowser: {str(sb_e)}")
                else:
                    raise ProfileRunningError(f"Profile {profile_id} đang chạy trên thiết bị khác")
        except ProfileRunningError:
            raise
        except Exception as e:
            logger.warning(f"Lỗi khi kiểm tra và xử lý profile đang chạy: {str(e)}")
            # Tiếp tục thực hiện bước tiếp theo ngay cả khi có lỗi
        
        # Tăng thời gian chờ từ 3 lên 5 giây và thêm dọn dẹp tài nguyên
        if profile_was_running and force_stop_running:
            logger.info("Đợi thêm thời gian để đảm bảo tài nguyên được giải phóng hoàn toàn...")
            time.sleep(5)  # Tăng lên 5 giây
            
            # Thêm kiểm tra và dọn dẹp tài nguyên một lần nữa
            if cleanup_orphaned:
                logger.info("Dọn dẹp triệt để các tiến trình Chrome còn sót...")
                _cleanup_orphaned_chrome_processes(profile_id)
        
        return True
        
    @retry_with_backoff(max_retries=3, error_types=(Exception,))
    def run_profile(self, profile_id, verify_ws_endpoint=True, window_check_delay=2.0,
                   connection_check_timeout=3.0, force_restart=False):
        """
        Khởi chạy profile GenLogin với cơ chế retry và trả về wsEndpoint.
        
        Args:
            profile_id: ID profile cần khởi chạy
            verify_ws_endpoint: Kiểm tra wsEndpoint có khả dụng hay không
            window_check_delay: Độ trễ đợi cửa sổ (giây)
            connection_check_timeout: Timeout kiểm tra kết nối (giây)
            force_restart: Buộc khởi động lại kể cả nếu profile đã chạy
            
        Returns:
            str: wsEndpoint của profile đã chạy
            
        Raises:
            ProfileStartupError: Nếu không thể khởi chạy profile
        """
        if not self.gen:
            raise GenloginConnectionError("Chưa kết nối với GenLogin API")
            
        logger.info(f"Khởi chạy profile {profile_id}...")
        
        # Đặt timeout cho việc khởi chạy profile
        original_timeout = None
        if hasattr(self.gen, 'request_timeout'):
            original_timeout = self.gen.request_timeout
            self.gen.request_timeout = 15  # 15 giây cho việc khởi chạy profile
        
        try:
            # Force_restart = True nếu được yêu cầu
            force_restart_param = {"force_restart": force_restart} if force_restart else {}
            
            logger.debug(f"Gọi runProfile với force_restart={force_restart}")
            ws_data = self.gen.runProfile(profile_id, **force_restart_param)
        finally:
            # Khôi phục timeout gốc
            if hasattr(self.gen, 'request_timeout') and original_timeout is not None:
                self.gen.request_timeout = original_timeout
        
        if not ws_data.get("success", False):
            error_msg = ws_data.get("message", "Không rõ lỗi")
            logger.warning(f"Không thể chạy profile {profile_id}: {error_msg}")
            raise ProfileStartupError(f"Không thể chạy profile: {error_msg}")
        
        # Kiểm tra wsEndpoint trong kết quả
        if "wsEndpoint" not in ws_data or not ws_data["wsEndpoint"]:
            # Thử lấy từ trường data nếu có
            wsEndpoint = ws_data.get("data", {}).get("wsEndpoint", "")
            if not wsEndpoint:
                logger.warning(f"runProfile không trả về wsEndpoint hợp lệ: {ws_data}")
                raise ProfileStartupError("runProfile không trả về wsEndpoint hợp lệ")
        else:
            wsEndpoint = ws_data["wsEndpoint"]
        
        # Chuẩn hóa format wsEndpoint
        wsEndpoint = wsEndpoint.replace("ws://", "").split('/')[0]
        logger.info(f"Nhận được wsEndpoint: {wsEndpoint}")
        
        # Thêm kiểm tra wsEndpoint nếu cần
        if verify_ws_endpoint:
            logger.debug(f"Kiểm tra kết nối đến wsEndpoint: {wsEndpoint}")
            host, port = wsEndpoint.split(':')
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(connection_check_timeout)
                    result = sock.connect_ex((host, int(port)))
                
                if result != 0:
                    logger.warning(f"wsEndpoint {wsEndpoint} không thể kết nối (error code: {result})")
                    raise ProfileStartupError(f"wsEndpoint {wsEndpoint} không thể kết nối")
                else:
                    logger.debug(f"wsEndpoint {wsEndpoint} có thể kết nối")
            except Exception as sock_e:
                logger.warning(f"Lỗi khi kiểm tra kết nối wsEndpoint: {str(sock_e)}")
                # Không ném ngoại lệ, vẫn tiếp tục
        
        # Đợi cho profile khởi động hoàn toàn
        adaptive_delay = window_check_delay
        logger.info(f"Đợi {adaptive_delay:.2f}s cho profile khởi động hoàn toàn...")
        time.sleep(adaptive_delay)
        
        # Kiểm tra lại xem profile đã thực sự chạy chưa
        try:
            logger.debug("Kiểm tra lại trạng thái profile trước khi tiếp tục...")
            running_check = self.gen.getProfilesRunning()
            running_ids = [p.get("id") for p in running_check.get("data", {}).get("items", []) 
                          if isinstance(p, dict)]
            
            if profile_id not in running_ids:
                logger.warning(f"Profile {profile_id} đã khởi chạy nhưng không xuất hiện trong danh sách đang chạy")
                
                # Thử kiểm tra lại wsEndpoint để xác nhận
                try:
                    ws_check = self.gen.getWsEndpoint(profile_id)
                    if ws_check.get("success", False) and ws_check.get("data", {}).get("wsEndpoint"):
                        logger.info("Profile đang chạy (xác nhận qua getWsEndpoint)")
                    else:
                        logger.warning("Profile không trả về wsEndpoint hợp lệ khi kiểm tra")
                        raise ProfileStartupError(f"Profile {profile_id} không trả về wsEndpoint hợp lệ khi kiểm tra")
                except Exception as ws_check_e:
                    logger.warning(f"Lỗi khi kiểm tra getWsEndpoint: {str(ws_check_e)}")
                    # Vẫn tiếp tục nếu có lỗi
            else:
                logger.info(f"Xác nhận profile {profile_id} đang chạy")
        except Exception as recheck_e:
            logger.warning(f"Lỗi khi kiểm tra lại trạng thái profile: {str(recheck_e)}")
        
        return wsEndpoint

@retry_with_backoff(max_retries=3, error_types=(Exception, ExecutionContextError))
def _attach_selenium_to_profile(
    driver_path, 
    wsEndpoint, 
    page_load_timeout=30, 
    disable_cache=True
):
    """
    Khởi tạo Selenium WebDriver và kết nối với profile đang chạy.
    
    Args:
        driver_path: Đường dẫn đến ChromeDriver
        wsEndpoint: wsEndpoint của profile đang chạy
        page_load_timeout: Timeout cho việc tải trang (giây)
        disable_cache: Vô hiệu hóa cache nếu True
        
    Returns:
        webdriver.Chrome: WebDriver đã kết nối
        
    Raises:
        DriverInitError: Nếu không thể khởi tạo driver
        ExecutionContextError: Nếu gặp lỗi "No such execution context"
    """
    logger.debug("Khởi tạo WebDriver với debuggerAddress: " + wsEndpoint)
    
    # Kiểm tra driver_path
    driver_path = check_driver_path(driver_path)

    # Khởi tạo service
    service = Service(executable_path=driver_path)

    # Vô hiệu hóa cơ chế retry
    try:
        # Tích hợp trực tiếp, không phụ thuộc vào module network_utils
        from urllib3.util.retry import Retry
        service.http_retries = Retry(
            total=0, 
            connect=0, 
            read=0, 
            redirect=0, 
            status=0, 
            respect_retry_after_header=False, 
            backoff_factor=0, 
            raise_on_status=False
        )
        logger.debug("Đã áp dụng cấu hình no_retry tích hợp")
    except Exception as retry_e:
        # Lỗi khác khi xử lý retry - bỏ qua và tiếp tục
        logger.warning(f"Không thể thiết lập cấu hình no-retry: {str(retry_e)}")

    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", wsEndpoint)
    
    # Thêm các options khác để tránh crash và tối ưu hóa
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-breakpad")  # Tắt crash reporter
    
    # Vô hiệu hóa cache nếu được yêu cầu (giúp tránh lỗi context)
    if disable_cache:
        chrome_options.add_argument("--disable-application-cache")
        chrome_options.add_argument("--disable-cache")
        chrome_options.add_argument("--disable-offline-load-stale-cache")
    
    # Khởi tạo WebDriver
    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logger.debug("Đã khởi tạo WebDriver thành công")
    except Exception as e:
        # Xử lý trường hợp đặc biệt khi có lỗi với cấu hình retry
        if "isinstance" in str(e) and "arg 2 must be a type" in str(e):
            logger.warning("Phát hiện lỗi liên quan đến cấu hình retry - thử phương pháp thay thế")
            
            # Tạo service mới không đặt http_retries
            alt_service = Service(executable_path=driver_path)
            # Không đặt http_retries lần này để tránh lỗi
            
            # Thử khởi tạo lại driver
            try:
                driver = webdriver.Chrome(service=alt_service, options=chrome_options)
                logger.info("Đã khởi tạo driver thành công với phương pháp thay thế")
            except Exception as e2:
                if "no such execution context" in str(e2).lower():
                    logger.warning("Phát hiện lỗi 'No Such Execution Context' khi khởi tạo driver với phương pháp thay thế")
                    raise ExecutionContextError(f"Lỗi 'No Such Execution Context': {str(e2)}")
                else:
                    raise DriverInitError(f"Lỗi WebDriver khi khởi tạo: {str(e2)}")
        elif "no such execution context" in str(e).lower():
            logger.warning("Phát hiện lỗi 'No Such Execution Context' khi khởi tạo driver")
            raise ExecutionContextError(f"Lỗi 'No Such Execution Context': {str(e)}")
        else:
            # Xử lý các lỗi WebDriver khác
            raise DriverInitError(f"Lỗi WebDriver khi khởi tạo: {str(e)}")
    
    # Thiết lập timeout phù hợp
    driver.set_page_load_timeout(page_load_timeout)
    driver.set_script_timeout(30)
    driver.implicitly_wait(10)
    
    # Kiểm tra kết nối driver
    delayed_window_check = 1.0
    logger.info(f"Đợi {delayed_window_check:.2f}s trước khi kiểm tra cửa sổ...")
    time.sleep(delayed_window_check)
    
    # Kiểm tra window handles
    handles = driver.window_handles
    logger.debug(f"Số cửa sổ: {len(handles)}")
    
    # Nếu không có cửa sổ nào, ném ngoại lệ
    if len(handles) == 0:
        logger.warning("Không tìm thấy cửa sổ nào sau khi khởi tạo driver")
        raise NoSuchWindowException("Không tìm thấy cửa sổ nào")
    
    # Chọn cửa sổ đầu tiên nếu có nhiều cửa sổ
    if len(handles) > 1:
        logger.debug(f"Có nhiều cửa sổ, chuyển đến cửa sổ đầu tiên")
        driver.switch_to.window(handles[0])
    
    # Thử lấy current_url để xác nhận kết nối
    try:
        current_url = driver.current_url
        logger.debug(f"URL hiện tại: {current_url}")
    except Exception as url_e:
        if "no such execution context" in str(url_e).lower():
            logger.warning(f"Lỗi No Such Execution Context khi lấy URL hiện tại: {str(url_e)}")
            raise ExecutionContextError(f"Lỗi 'No Such Execution Context' khi lấy URL: {str(url_e)}")
        else:
            raise DriverInitError(f"Lỗi khi lấy URL hiện tại: {str(url_e)}")
    
    # Kiểm tra execution context bằng JavaScript
    if not _check_execution_context(driver, detail_level=1, retries=2):
        raise ExecutionContextError("Driver không có execution context ổn định")
    
    # Thêm thông tin debug vào window object
    try:
        driver.execute_script("""
            window.selenium_info = {
                "initialized": true,
                "profile_id": arguments[0],
                "timestamp": new Date().toISOString(),
                "session_id": arguments[1]
            };
        """, wsEndpoint, driver.session_id)
        logger.debug("Đã thêm thông tin debug vào window object")
    except Exception as e:
        logger.debug(f"Không thể thêm thông tin debug vào window object: {str(e)}")
        # Bỏ qua lỗi này và tiếp tục
    
    # Kiểm tra cuối cùng
    try:
        # Kiểm tra lại execution context bằng script chi tiết hơn
        comprehensive_check = driver.execute_script("""
            return {
                'url': window.location.href,
                'title': document.title,
                'readyState': document.readyState,
                'windowCount': window.length,
                'timestamp': Date.now()
            };
        """)
        logger.debug(f"Kiểm tra cuối cùng thành công: {comprehensive_check}")
    except Exception as script_e:
        if "no such execution context" in str(script_e).lower():
            logger.warning(f"Lỗi No Such Execution Context trong kiểm tra cuối cùng: {str(script_e)}")
            raise ExecutionContextError(f"Lỗi 'No Such Execution Context' trong kiểm tra cuối cùng: {str(script_e)}")
        else:
            raise DriverInitError(f"Lỗi khi thực hiện kiểm tra cuối cùng: {str(script_e)}")
    
    logger.info("Đã khởi tạo và kiểm tra WebDriver thành công")
    return driver

# -------------------------------
# Core Functions
# -------------------------------
def init_driver_with_genlogin(options: DriverInitOptions) -> Tuple[Optional[webdriver.Chrome], Optional[Any], Optional[str]]:
    """
    Khởi tạo profile GenLogin & Selenium WebDriver với xử lý tình huống phức tạp.
    Phiên bản tối ưu để xử lý lỗi "No Such Window" và "No Such Execution Context",
    cải thiện hiệu suất và tính ổn định cho môi trường production.
    
    Args:
        options: Cấu hình khởi tạo driver

    Returns:
        Tuple[Optional[webdriver.Chrome], Optional[Any], Optional[str]]:
            (driver, gen, actual_profile_id) hoặc (None, None, None) nếu lỗi.

    Raises:
        DriverInitError: Nếu quá số lần retry mà vẫn không khởi tạo được.
        ProfileRunningError: Nếu profile đang chạy trên thiết bị khác và force_stop_running=False.
    """
    driver = None
    gen = None
    actual_profile_id = None
    init_start_time = time.time()
    
    # Khởi tạo logger
    logger = logging.getLogger(__name__)

    # Kiểm tra cờ shutdown đầu tiên
    if is_shutdown_requested():
        logger.warning("Phát hiện yêu cầu shutdown, không khởi tạo driver mới")
        return None, None, None
    
    # Xử lý đường dẫn ChromeDriver
    try:
        driver_path = check_driver_path(options.chrome_driver_path)
        logger.debug(f"Sử dụng ChromeDriver tại: {driver_path}")
    except Exception as e:
        logger.error(f"Lỗi đường dẫn ChromeDriver: {str(e)}")
        raise
    
    # Dọn dẹp các tiến trình Chrome mồ côi trước khi bắt đầu
    if options.resources.cleanup_orphaned:
        _cleanup_orphaned_chrome_processes()
        logger.debug("Đã dọn dẹp các tiến trình Chrome mồ côi")
    
    # Khởi tạo GenloginConnector
    genlogin_connector = GenloginConnector(
        api_key=options.genlogin.api_key,
        base_url=options.genlogin.base_url,
        retry_config=options.retry
    )
    
    # Vòng lặp thử lại chính
    for attempt in range(options.retry.count):
        try:
            # Nếu có driver, gen hoặc profile đã được khởi tạo, cleanup trước khi thử lại
            if driver or gen or actual_profile_id:
                try:
                    logger.info("Đóng driver hiện tại trước khi thử lại")
                    close_current_driver(
                        driver, 
                        gen, 
                        actual_profile_id, 
                        force_kill=options.resources.force_kill_on_error
                    )
                except Exception as e:
                    logger.warning(f"Lỗi khi đóng driver hiện tại: {str(e)}")
                driver = None
                gen = None
                actual_profile_id = None
                
                # Đợi thêm thời gian sau khi đóng driver cũ
                time.sleep(2.0)
                
                # Tối ưu hóa bộ nhớ
                if options.resources.memory_optimization:
                    gc.collect()
            
            # 1. Khởi tạo GenLogin API client
            try:
                gen = genlogin_connector.connect_with_retry(connect_timeout=options.timeouts.connection)
            except Exception as e:
                logger.error(f"Không thể kết nối với GenLogin API: {str(e)}")
                raise GenloginConnectionError(f"Không thể kết nối với GenLogin API: {str(e)}")
            
            # 2. Chọn profile_id nếu chưa được chỉ định
            try:
                actual_profile_id = genlogin_connector.select_profile_id(options.profile_id)
            except Exception as e:
                logger.error(f"Không thể chọn profile ID: {str(e)}")
                raise DriverInitError(f"Không thể chọn profile ID: {str(e)}")
            
            logger.info(f"Sử dụng profile ID: {actual_profile_id}")
            
            # 3. Đảm bảo profile đã dừng nếu đang chạy
            try:
                genlogin_connector.ensure_profile_stopped(
                    actual_profile_id,
                    options.genlogin.force_stop_running,
                    options.resources.cleanup_orphaned
                )
            except ProfileRunningError as e:
                logger.error(f"Lỗi profile đang chạy: {str(e)}")
                raise
            except Exception as e:
                logger.warning(f"Lỗi khi dừng profile đang chạy: {str(e)}")
                # Tiếp tục ngay cả khi có lỗi không nghiêm trọng
            
            # 4. Khởi chạy profile và lấy wsEndpoint
            try:
                wsEndpoint = genlogin_connector.run_profile(
                    actual_profile_id, 
                    options.genlogin.verify_ws_endpoint, 
                    options.timeouts.window_check_delay, 
                    options.timeouts.connection
                )
            except Exception as e:
                logger.error(f"Không thể khởi chạy profile: {str(e)}")
                raise ProfileStartupError(f"Không thể khởi chạy profile: {str(e)}")
            
            # 5. Kết nối Selenium WebDriver với profile
            try:
                driver = _attach_selenium_to_profile(
                    driver_path, 
                    wsEndpoint, 
                    options.timeouts.page_load, 
                    options.resources.disable_cache
                )
            except ExecutionContextError as e:
                logger.error(f"Lỗi execution context khi kết nối với profile: {str(e)}")
                raise DriverInitError(f"Lỗi execution context khi kết nối với profile: {str(e)}")
            except Exception as e:
                logger.error(f"Không thể kết nối WebDriver với profile: {str(e)}")
                raise DriverInitError(f"Không thể kết nối WebDriver với profile: {str(e)}")
            
            # 6. Kiểm tra sức khỏe driver tổng thể
            execution_context_level = 2 if options.execution_context_check else 1
            if not _verify_driver_health(driver, execution_context_level=execution_context_level):
                logger.error("Driver không có execution context ổn định sau khi khởi tạo")
                raise DriverInitError("Driver không có execution context ổn định sau khi khởi tạo")
            
            # Ghi thời gian khởi tạo
            total_init_time = time.time() - init_start_time
            logger.info(f"Khởi tạo driver hoàn tất sau {total_init_time:.2f}s với profile {actual_profile_id}")
            
            # Trả về kết quả thành công
            return driver, gen, actual_profile_id
        
        except ProfileRunningError as e:
            logger.error(f"Lỗi profile đang chạy: {str(e)}")
            try:
                close_current_driver(
                    driver, 
                    gen, 
                    actual_profile_id, 
                    force_kill=options.resources.force_kill_on_error
                )
            except Exception as close_e:
                logger.warning(f"Lỗi khi đóng driver sau ProfileRunningError: {str(close_e)}")
            raise
        
        except DriverInitError as e:
            logger.warning(f"Lỗi khởi tạo driver (lần thử {attempt+1}/{options.retry.count}): {str(e)}")
            try:
                close_current_driver(
                    driver, 
                    gen, 
                    actual_profile_id, 
                    force_kill=options.resources.force_kill_on_error
                )
            except Exception as close_e:
                logger.warning(f"Lỗi khi đóng driver sau DriverInitError: {str(close_e)}")
                
            driver = None
            gen = None
            actual_profile_id = None
            
            if attempt == options.retry.count - 1:
                logger.error(f"Đã hết số lần thử khởi tạo driver: {str(e)}")
                raise
            
            # Dọn dẹp tiến trình treo
            if options.resources.cleanup_orphaned:
                _cleanup_orphaned_chrome_processes(actual_profile_id)
                
            wait_time = options.retry.base_delay * (options.retry.backoff_factor ** attempt)
            if options.retry.jitter:
                wait_time *= random.uniform(0.8, 1.2)
            logger.debug(f"Chờ {wait_time:.2f}s trước khi thử lại...")
            time.sleep(wait_time)
        
        except Exception as e:
            logger.warning(f"Lỗi không mong đợi (lần thử {attempt+1}/{options.retry.count}): {type(e).__name__}: {str(e)}")
            
            # Kiểm tra error message có chứa "execution context" không
            execution_context_error = "no such execution context" in str(e).lower()
            
            try:
                # Đóng tài nguyên bằng cách mạnh mẽ hơn
                # Kill các process Chrome liên quan đến profile nếu là lỗi execution context
                if execution_context_error and options.resources.cleanup_orphaned:
                    _cleanup_orphaned_chrome_processes(actual_profile_id)
                
                close_current_driver(
                    driver, 
                    gen, 
                    actual_profile_id, 
                    force_kill=options.resources.force_kill_on_error
                )
            except Exception as close_e:
                logger.warning(f"Lỗi khi đóng driver sau Exception: {str(close_e)}")
                
            driver = None
            gen = None
            actual_profile_id = None
            
            if attempt == options.retry.count - 1:
                logger.error(f"Lỗi khởi tạo driver sau {options.retry.count} lần thử: {type(e).__name__}: {str(e)}")
                raise DriverInitError(f"Lỗi khởi tạo driver sau {options.retry.count} lần thử: {str(e)}")
                
            # Điều chỉnh thời gian chờ dựa trên loại lỗi
            if execution_context_error:
                # Chờ lâu hơn với lỗi execution context
                wait_time = 5.0 * (options.retry.backoff_factor ** attempt)
            else:
                wait_time = options.retry.base_delay * (options.retry.backoff_factor ** attempt)
                
            if options.retry.jitter:
                wait_time *= random.uniform(0.8, 1.2)
            logger.debug(f"Chờ {wait_time:.2f}s trước khi thử lại...")
            time.sleep(wait_time)
            
            # Cố gắng dọn dẹp bộ nhớ
            if options.resources.memory_optimization:
                gc.collect()
    
    # Trường hợp đã vượt quá số lần thử và không có ngoại lệ gì được throw
    # Ghi nhật ký tổng thời gian đã cố gắng
    total_time = time.time() - init_start_time
    logger.error(f"Không thể khởi tạo driver sau {options.retry.count} lần thử trong {total_time:.2f}s.")
    
    # Dọn dẹp tiến trình treo lần cuối
    if options.resources.cleanup_orphaned:
        _cleanup_orphaned_chrome_processes()
        
    raise DriverInitError(f"Không thể khởi tạo driver sau {options.retry.count} lần thử.")

def init_driver_with_genlogin_profile_manager(options: DriverInitOptions) -> Tuple[Optional[webdriver.Chrome], Optional[str]]:
    """
    Khởi tạo profile GenLogin & Selenium WebDriver sử dụng GenloginProfileManager.
    Phiên bản tối ưu hơn, tách biệt quản lý GenLogin profile.
    
    Args:
        options: Cấu hình khởi tạo driver
    
    Returns:
        Tuple[Optional[webdriver.Chrome], Optional[str]]:
            (driver, actual_profile_id) hoặc (None, None) nếu lỗi
    
    Raises:
        DriverInitError: Nếu quá số lần retry mà vẫn không khởi tạo được
        ProfileRunningError: Nếu profile đang chạy trên thiết bị khác và force_stop_running=False
    """
    driver = None
    actual_profile_id = None
    init_start_time = time.time()
    
    # Kiểm tra cờ shutdown
    if is_shutdown_requested():
        logger.warning("Phát hiện yêu cầu shutdown, không khởi tạo driver mới")
        return None, None
    
    # Xử lý đường dẫn ChromeDriver
    try:
        driver_path = check_driver_path(options.chrome_driver_path)
        logger.debug(f"Sử dụng ChromeDriver tại: {driver_path}")
    except Exception as e:
        logger.error(f"Lỗi đường dẫn ChromeDriver: {e}")
        raise
    
    # Dọn dẹp các tiến trình Chrome mồ côi trước khi bắt đầu
    if options.resources.cleanup_orphaned:
        _cleanup_orphaned_chrome_processes()
    
    # Khởi tạo profile manager
    profile_manager = GenloginProfileManager(
        genlogin_api_key=options.genlogin.api_key,
        genlogin_base_url=options.genlogin.base_url,
        retry_backoff_factor=options.retry.backoff_factor,
        retry_jitter=options.retry.jitter
    )
    
    # Vòng lặp thử lại chính
    for attempt in range(options.retry.count):
        try:
            # Nếu có driver hoặc profile đã được khởi tạo, cleanup trước khi thử lại
            if driver or actual_profile_id:
                try:
                    logger.info("Đóng driver hiện tại trước khi thử lại")
                    if actual_profile_id:
                        profile_manager.stop_profile(
                            actual_profile_id,
                            force_kill=options.resources.force_kill_on_error
                        )
                    if driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                except Exception as e:
                    logger.warning(f"Lỗi khi dọn dẹp trước khi thử lại: {str(e)}")
                driver = None
                actual_profile_id = None
                
                # Đợi thêm thời gian sau khi đóng driver cũ
                time.sleep(2.0)
                
                # Tối ưu hóa bộ nhớ
                if options.resources.memory_optimization:
                    gc.collect()
            
            # 1. Khởi chạy profile và lấy wsEndpoint
            actual_profile_id, wsEndpoint = profile_manager.start_profile(
                profile_id=options.profile_id,
                force_stop_running=options.genlogin.force_stop_running,
                force_restart=False,
                max_retries=max(2, options.retry.count)
            )
            
            # 2. Kết nối Selenium WebDriver với profile
            ctx_retry_count = 3  # Số lần thử cho vấn đề execution context
            for ctx_attempt in range(ctx_retry_count):
                try:
                    driver = _attach_selenium_to_profile(
                        driver_path,
                        wsEndpoint,
                        options.timeouts.page_load,
                        options.resources.disable_cache
                    )
                    break
                except ExecutionContextError as e:
                    logger.warning(f"Lỗi execution context khi kết nối với profile (lần {ctx_attempt+1}/{ctx_retry_count}): {str(e)}")
                    if ctx_attempt < ctx_retry_count - 1:
                        # Đợi thời gian dài hơn với lỗi Execution Context
                        wait_time = 5.0 * (1.5 ** ctx_attempt)
                        logger.info(f"Đợi {wait_time:.2f}s trước khi thử lại...")
                        time.sleep(wait_time)
                        continue
                    raise DriverInitError(f"Lỗi execution context khi kết nối với profile: {str(e)}")
                except Exception as e:
                    logger.error(f"Không thể kết nối WebDriver với profile: {str(e)}")
                    raise DriverInitError(f"Không thể kết nối WebDriver với profile: {str(e)}")
            
            # 3. Kiểm tra sức khỏe driver tổng thể
            execution_context_level = 2 if options.execution_context_check else 1
            if not _verify_driver_health(driver, execution_context_level=execution_context_level):
                logger.error("Driver không có execution context ổn định sau khi khởi tạo")
                raise DriverInitError("Driver không có execution context ổn định sau khi khởi tạo")
            
            # Ghi thời gian khởi tạo
            total_init_time = time.time() - init_start_time
            logger.info(f"Khởi tạo driver hoàn tất sau {total_init_time:.2f}s với profile {actual_profile_id}")
            
            # Trả về kết quả thành công
            return driver, actual_profile_id
        
        except (ProfileRunningError, DriverInitError) as e:
            logger.warning(f"Lỗi khởi tạo driver (lần thử {attempt+1}/{options.retry.count}): {str(e)}")
            
            # Dọn dẹp
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None
                
            if actual_profile_id:
                try:
                    profile_manager.stop_profile(
                        actual_profile_id,
                        force_kill=options.resources.force_kill_on_error
                    )
                except Exception:
                    pass
                    
            # Nếu là ProfileRunningError, ném lại ngoại lệ
            if isinstance(e, ProfileRunningError):
                raise
                
            # Nếu đã hết số lần thử với DriverInitError, ném lại ngoại lệ
            if attempt == options.retry.count - 1 and isinstance(e, DriverInitError):
                raise
            
            # Dọn dẹp tiến trình treo
            if options.resources.cleanup_orphaned:
                _cleanup_orphaned_chrome_processes(actual_profile_id)
                
            # Tính thời gian chờ với backoff/jitter
            wait_time = options.retry.base_delay * (options.retry.backoff_factor ** attempt)
            if options.retry.jitter:
                wait_time *= random.uniform(0.8, 1.2)
                
            logger.debug(f"Chờ {wait_time:.2f}s trước khi thử lại...")
            time.sleep(wait_time)
            
            # Tối ưu hóa bộ nhớ
            if options.resources.memory_optimization:
                gc.collect()
        
        except Exception as e:
            logger.warning(f"Lỗi không mong đợi (lần thử {attempt+1}/{options.retry.count}): {type(e).__name__}: {str(e)}")
            
            # Dọn dẹp
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None
                
            if actual_profile_id:
                try:
                    profile_manager.stop_profile(
                        actual_profile_id,
                        force_kill=options.resources.force_kill_on_error
                    )
                except Exception:
                    pass
            
            if attempt == options.retry.count - 1:
                logger.error(f"Lỗi khởi tạo driver sau {options.retry.count} lần thử: {type(e).__name__}: {str(e)}")
                raise DriverInitError(f"Lỗi khởi tạo driver sau {options.retry.count} lần thử: {str(e)}")
                
            # Dọn dẹp tiến trình treo
            if options.resources.cleanup_orphaned:
                _cleanup_orphaned_chrome_processes(actual_profile_id)
                
            # Tính thời gian chờ với backoff/jitter
            wait_time = options.retry.base_delay * (options.retry.backoff_factor ** attempt)
            if options.retry.jitter:
                wait_time *= random.uniform(0.8, 1.2)
                
            logger.debug(f"Chờ {wait_time:.2f}s trước khi thử lại...")
            time.sleep(wait_time)
            
            # Tối ưu hóa bộ nhớ
            if options.resources.memory_optimization:
                gc.collect()
    
    # Trường hợp đã vượt quá số lần thử và không có ngoại lệ gì được throw
    total_time = time.time() - init_start_time
    logger.error(f"Không thể khởi tạo driver sau {options.retry.count} lần thử trong {total_time:.2f}s.")
    
    # Dọn dẹp tiến trình treo lần cuối
    if options.resources.cleanup_orphaned:
        _cleanup_orphaned_chrome_processes()
        
    raise DriverInitError(f"Không thể khởi tạo driver sau {options.retry.count} lần thử.")

class DriverLifecycleManager:
    """
    Lớp quản lý vòng đời của WebDriver, bao gồm khởi tạo, kiểm tra và đóng.
    """
    
    def __init__(self, options: Optional[DriverInitOptions] = None):
        """
        Khởi tạo DriverLifecycleManager.
        
        Args:
            options: Cấu hình khởi tạo driver
        """
        self.options = options or DriverInitOptions(chrome_driver_path="chromedriver")
        self.driver = None
        self.gen = None
        self.profile_id = None
        self.profile_manager = None
        
        # Tạo profile manager nếu cần sử dụng GenLogin
        self._init_profile_manager()
    
    def _init_profile_manager(self):
        """Khởi tạo GenloginProfileManager"""
        try:
            self.profile_manager = GenloginProfileManager(
                genlogin_api_key=self.options.genlogin.api_key,
                genlogin_base_url=self.options.genlogin.base_url,
                retry_backoff_factor=self.options.retry.backoff_factor,
                retry_jitter=self.options.retry.jitter
            )
            logger.debug("Đã khởi tạo GenloginProfileManager")
        except Exception as e:
            logger.warning(f"Không thể khởi tạo GenloginProfileManager: {str(e)}")
            self.profile_manager = None
    
    def init_driver(self, profile_id=None, force_stop_running=None):
        """
        Khởi tạo driver với profile GenLogin.
        
        Args:
            profile_id: ID profile GenLogin (None => dùng từ options hoặc chọn profile đầu tiên)
            force_stop_running: Ghi đè giá trị từ options
            
        Returns:
            bool: True nếu khởi tạo thành công, False nếu thất bại
        """
        # Đóng driver hiện tại nếu có
        self.close_driver()
        
        # Cập nhật tham số từ đầu vào
        current_options = self.options
        if profile_id is not None:
            current_options.profile_id = profile_id
        if force_stop_running is not None:
            current_options.genlogin.force_stop_running = force_stop_running
            
        try:
            # Sử dụng profile manager nếu có
            if self.profile_manager:
                self.driver, self.profile_id = init_driver_with_genlogin_profile_manager(current_options)
            else:
                # Sử dụng hàm truyền thống nếu không có profile manager
                self.driver, self.gen, self.profile_id = init_driver_with_genlogin(current_options)
            
            # Thiết lập timeouts nếu driver đã được khởi tạo
            if self.driver:
                self.driver.set_page_load_timeout(current_options.timeouts.page_load)
                self.driver.set_script_timeout(current_options.timeouts.script)
                self.driver.implicitly_wait(current_options.timeouts.implicit_wait)
                logger.info(f"Đã khởi tạo driver thành công với profile {self.profile_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo driver: {str(e)}")
            # Đảm bảo tài nguyên được giải phóng
            self.close_driver()
            return False
    
    def check_driver(self, throw_exception=False, detail_level=1):
        """
        Kiểm tra driver còn hoạt động hay không.
        
        Args:
            throw_exception: Có ném ngoại lệ khi driver không hoạt động không
            detail_level: Mức độ chi tiết của kiểm tra (0-2)
            
        Returns:
            bool: True nếu driver hoạt động, False nếu không
            
        Raises:
            DriverConnectionError: Nếu throw_exception=True và driver không hoạt động
        """
        if not self.driver:
            if throw_exception:
                from driver_maintenance import DriverConnectionError
                raise DriverConnectionError("Driver không được khởi tạo")
            return False
        
        # Kiểm tra execution context thay vì dùng verify_driver_connection
        # Cho phép kiểm soát chi tiết hơn mức độ kiểm tra
        try:
            return _check_execution_context(self.driver, detail_level=detail_level)
        except Exception as e:
            if throw_exception:
                from driver_maintenance import DriverConnectionError
                raise DriverConnectionError(f"Lỗi khi kiểm tra driver: {str(e)}")
            return False
    
    def close_driver(self):
        """
        Đóng driver và profile GenLogin hiện tại.
        
        Returns:
            bool: True nếu đóng thành công, False nếu thất bại
        """
        success = True
        
        # Sử dụng profile manager nếu có
        if self.profile_manager and self.profile_id:
            try:
                if self.driver:
                    try:
                        self.driver.quit()
                    except Exception as e:
                        logger.warning(f"Lỗi khi đóng driver: {str(e)}")
                        success = False
                
                profile_stopped = self.profile_manager.stop_profile(
                    self.profile_id, 
                    force_kill=self.options.resources.force_kill_on_error
                )
                success = success and profile_stopped
            except Exception as e:
                logger.error(f"Lỗi khi đóng driver và profile: {str(e)}")
                success = False
        else:
            # Sử dụng close_current_driver truyền thống
            try:
                success = close_current_driver(
                    self.driver, 
                    self.gen, 
                    self.profile_id, 
                    force_kill=self.options.resources.force_kill_on_error
                )
            except Exception as e:
                logger.error(f"Lỗi khi đóng driver và profile: {str(e)}")
                success = False
        
        # Xóa tham chiếu
        self.driver = None
        self.profile_id = None
        
        # Dọn dẹp tiến trình treo nếu cần
        if self.options.resources.cleanup_orphaned:
            _cleanup_orphaned_chrome_processes()
        
        # Tối ưu hóa bộ nhớ
        if self.options.resources.memory_optimization:
            gc.collect()
        
        return success

# Nhóm hàm tiện ích để sử dụng dễ dàng
def create_driver_options(
    chrome_driver_path: str,
    profile_id: Optional[str] = None,
    genlogin_api_key: str = "",
    genlogin_base_url: str = "http://localhost:55550/backend",
    execution_context_check: bool = True
) -> DriverInitOptions:
    """
    Tạo đối tượng DriverInitOptions với các giá trị mặc định.
    
    Args:
        chrome_driver_path: Đường dẫn đến ChromeDriver
        profile_id: ID profile GenLogin (None để tự động chọn)
        genlogin_api_key: API key GenLogin
        genlogin_base_url: URL cơ sở cho GenLogin API
        execution_context_check: Có thực hiện kiểm tra JavaScript execution context chi tiết không
        
    Returns:
        DriverInitOptions: Đối tượng cấu hình driver
    """
    return DriverInitOptions(
        chrome_driver_path=chrome_driver_path,
        profile_id=profile_id,
        genlogin=GenLoginConfig(
            api_key=genlogin_api_key,
            base_url=genlogin_base_url
        ),
        execution_context_check=execution_context_check
    )

def init_driver_simple(
    chrome_driver_path: str,
    profile_id: Optional[str] = None,
    genlogin_api_key: str = "",
    genlogin_base_url: str = "http://localhost:55550/backend",
    force_stop_running: bool = True,
    page_load_timeout: int = 30
) -> Tuple[Optional[webdriver.Chrome], Optional[str]]:
    """
    Phiên bản đơn giản của init_driver, ít tham số hơn và trả về (driver, profile_id).
    
    Args:
        chrome_driver_path: Đường dẫn đến ChromeDriver
        profile_id: ID profile GenLogin (None để tự động chọn)
        genlogin_api_key: API key GenLogin
        genlogin_base_url: URL cơ sở cho GenLogin API
        force_stop_running: Dừng profile đang chạy nếu True
        page_load_timeout: Timeout cho việc tải trang (giây)
        
    Returns:
        Tuple[Optional[webdriver.Chrome], Optional[str]]: (driver, profile_id) hoặc (None, None) nếu lỗi
    """
    options = create_driver_options(
        chrome_driver_path=chrome_driver_path,
        profile_id=profile_id,
        genlogin_api_key=genlogin_api_key,
        genlogin_base_url=genlogin_base_url
    )
    
    options.genlogin.force_stop_running = force_stop_running
    options.timeouts.page_load = page_load_timeout
    
    try:
        driver, gen, actual_profile_id = init_driver_with_genlogin(options)
        return driver, actual_profile_id
    except Exception as e:
        logger.error(f"Lỗi trong init_driver_simple: {str(e)}")
        return None, None

# -------------------------------
# End of Module
# -------------------------------