"""
driver_maintenance.py - Module quản lý bảo trì và xử lý driver
Version: 2.0.0 (Production)

Tính năng chính:
- Kiểm tra và xác minh trạng thái kết nối của WebDriver
- Đóng driver an toàn, bao gồm xử lý profile GenLogin
- Quản lý các tab và cửa sổ trong browser
- Dọn dẹp tiến trình Chrome bị treo
- Các hàm tiện ích cho việc tương tác với trang web: đợi trang load, kiểm tra phần tử
- Thực thi các hàm với cơ chế retry linh hoạt
- Lấy thông tin về phiên bản Chrome
"""

import os
import time
import gc
import logging
import random
import platform
import subprocess
import socket
import psutil
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Union, Any, List, Callable, Type, TypeVar

from selenium import webdriver
from selenium.common.exceptions import (
    WebDriverException, TimeoutException, InvalidSessionIdException,
    NoSuchWindowException, StaleElementReferenceException, SessionNotCreatedException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# Hàm kiểm tra shutdown state
try:
    from global_state import request_shutdown, is_shutdown_requested
except ImportError:
    # Fallback nếu không có module global_state
    def is_shutdown_requested():
        return False
    
    def request_shutdown():
        pass

# -------------------------------
# Cấu hình tập trung
# -------------------------------
@dataclass
class DriverMaintenanceConfig:
    """Lớp cấu hình tập trung cho module driver_maintenance."""
    timeouts: Dict[str, int] = field(default_factory=lambda: {
        "page_load": 30,
        "script": 30,
        "connection_check": 10,
        "window_check": 5,
        "network_idle": 10,
        "dom_stability": 2
    })
    
    retry: Dict[str, Any] = field(default_factory=lambda: {
        "max_retries": 3,
        "backoff_factor": 2.0,
        "jitter": True,
        "base_delay": 1.0
    })
    
    logging: Dict[str, str] = field(default_factory=lambda: {
        "level": "INFO",
        "detail_level": "MEDIUM"
    })
    
    cleanup: Dict[str, bool] = field(default_factory=lambda: {
        "force_kill": True,
        "cleanup_orphaned": True,
        "memory_optimization": True
    })
    
    connection_check: Dict[str, Any] = field(default_factory=lambda: {
        "levels": ["BASIC", "STANDARD", "DETAILED"],
        "default_level": "STANDARD"
    })

# Khởi tạo cấu hình mặc định
DEFAULT_CONFIG = DriverMaintenanceConfig()

# -------------------------------
# Logger tập trung
# -------------------------------
class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class DetailLevel(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"

class DriverLogger:
    """Lớp quản lý logging chuyên biệt cho module driver_maintenance."""
    
    def __init__(self, name="driver_maintenance", log_level=LogLevel.INFO, detail_level=DetailLevel.MEDIUM):
        """
        Khởi tạo DriverLogger.
        
        Args:
            name (str): Tên logger
            log_level (LogLevel): Mức độ log
            detail_level (DetailLevel): Mức độ chi tiết của log
        """
        self.logger = logging.getLogger(name)
        self.log_level = log_level
        self.detail_level = detail_level
        
        # Áp dụng log level
        numeric_level = getattr(logging, log_level.value, logging.INFO)
        self.logger.setLevel(numeric_level)
    
    def set_log_level(self, level: LogLevel):
        """Đặt mức độ log."""
        self.log_level = level
        numeric_level = getattr(logging, level.value, logging.INFO)
        self.logger.setLevel(numeric_level)
    
    def set_detail_level(self, level: DetailLevel):
        """Đặt mức độ chi tiết của log."""
        self.detail_level = level
    
    def debug(self, message, **kwargs):
        """Log ở mức debug."""
        self.logger.debug(message, **kwargs)
    
    def info(self, message, **kwargs):
        """Log ở mức info."""
        self.logger.info(message, **kwargs)
    
    def warning(self, message, **kwargs):
        """Log ở mức warning."""
        self.logger.warning(message, **kwargs)
    
    def error(self, message, **kwargs):
        """Log ở mức error."""
        self.logger.error(message, **kwargs)
    
    def critical(self, message, **kwargs):
        """Log ở mức critical."""
        self.logger.critical(message, **kwargs)
    
    def get_timestamp(self):
        """Trả về timestamp hiện tại dưới dạng chuỗi."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    def format_error(self, e: Exception):
        """
        Format lỗi để log với định dạng chuẩn.
        
        Args:
            e (Exception): Lỗi cần format
            
        Returns:
            str: Chuỗi lỗi đã được format
        """
        return f"{type(e).__name__}: {str(e)}"

# Khởi tạo logger mặc định
driver_logger = DriverLogger(
    log_level=LogLevel[DEFAULT_CONFIG.logging["level"]], 
    detail_level=DetailLevel[DEFAULT_CONFIG.logging["detail_level"]]
)

# -------------------------------
# Exceptions
# -------------------------------
class DriverConnectionError(Exception):
    """Ngoại lệ khi driver mất kết nối."""
    pass

class ExecutionContextError(DriverConnectionError):
    """Ngoại lệ khi gặp lỗi No such execution context."""
    pass

class ResourceCleanupError(Exception):
    """Ngoại lệ khi không thể dọn dẹp tài nguyên."""
    pass

class TimeoutError(Exception):
    """Ngoại lệ khi quá thời gian chờ."""
    pass

# -------------------------------
# Hàm kiểm tra kết nối - Đã tối ưu
# -------------------------------
def _check_basic_connection(driver, max_retries=1):
    """
    Kiểm tra kết nối cơ bản của driver.
    
    Args:
        driver: WebDriver cần kiểm tra
        max_retries: Số lần thử lại tối đa
        
    Returns:
        bool: True nếu kết nối cơ bản hoạt động, False nếu không
    """
    if not driver:
        return False
    
    for attempt in range(max_retries):
        try:
            # Kiểm tra cơ bản: session_id và window_handles
            session_id = driver.session_id
            if not session_id:
                driver_logger.debug("Session ID không tồn tại")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                return False
            
            # Kiểm tra window_handles
            handles = driver.window_handles
            if not handles:
                driver_logger.debug("Không có cửa sổ nào")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                return False
            
            return True
            
        except (InvalidSessionIdException, NoSuchWindowException) as e:
            driver_logger.debug(f"Lỗi kết nối cơ bản: {driver_logger.format_error(e)}")
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            return False
        except Exception as e:
            driver_logger.debug(f"Lỗi không xác định khi kiểm tra kết nối cơ bản: {driver_logger.format_error(e)}")
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            return False
    
    return False

def _check_context_execution(driver, max_retries=1):
    """
    Kiểm tra execution context của driver.
    
    Args:
        driver: WebDriver cần kiểm tra
        max_retries: Số lần thử lại tối đa
        
    Returns:
        bool: True nếu execution context hoạt động, False nếu không
        
    Raises:
        ExecutionContextError: Nếu phát hiện lỗi execution context và hết số lần thử
    """
    if not driver:
        return False
    
    for attempt in range(max_retries):
        try:
            # Thử thực thi JavaScript đơn giản
            result = driver.execute_script("return true;")
            if result is not True:
                driver_logger.debug("Script không trả về True")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                return False
            
            return True
            
        except WebDriverException as e:
            error_msg = str(e).lower()
            if "no such execution context" in error_msg or "target closed" in error_msg:
                driver_logger.debug(f"Lỗi execution context: {error_msg}")
                if attempt < max_retries - 1:
                    time.sleep(1.0)
                    continue
                raise ExecutionContextError(f"Lỗi execution context: {str(e)}")
            else:
                driver_logger.debug(f"Lỗi WebDriver khi kiểm tra execution context: {error_msg}")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                return False
        except Exception as e:
            driver_logger.debug(f"Lỗi không xác định khi kiểm tra execution context: {driver_logger.format_error(e)}")
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            return False
    
    return False

def _check_window_state(driver, max_retries=1):
    """
    Kiểm tra trạng thái cửa sổ của driver.
    
    Args:
        driver: WebDriver cần kiểm tra
        max_retries: Số lần thử lại tối đa
        
    Returns:
        bool: True nếu trạng thái cửa sổ ổn định, False nếu không
    """
    if not driver:
        return False
    
    for attempt in range(max_retries):
        try:
            # Kiểm tra có cửa sổ nào không
            handles = driver.window_handles
            if not handles:
                driver_logger.debug("Không có cửa sổ nào")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                return False
            
            # Kiểm tra current_url
            current_url = driver.current_url
            driver_logger.debug(f"URL hiện tại: {current_url}")
            
            # Kiểm tra current_window_handle
            current_handle = driver.current_window_handle
            if current_handle not in handles:
                driver_logger.debug("Handle hiện tại không hợp lệ")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                return False
            
            return True
            
        except NoSuchWindowException as e:
            driver_logger.debug(f"Lỗi No Such Window: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            return False
        except WebDriverException as e:
            error_msg = str(e).lower()
            if "no such execution context" in error_msg or "target closed" in error_msg:
                driver_logger.debug(f"Lỗi execution context khi kiểm tra cửa sổ: {error_msg}")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                return False
            else:
                driver_logger.debug(f"Lỗi WebDriver khi kiểm tra cửa sổ: {error_msg}")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                return False
        except Exception as e:
            driver_logger.debug(f"Lỗi không xác định khi kiểm tra cửa sổ: {driver_logger.format_error(e)}")
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            return False
    
    return False

def verify_driver_connection(
    driver: webdriver.Chrome, 
    throw_exception: bool = False, 
    check_level: str = "STANDARD", 
    max_retries: int = 2
) -> bool:
    """
    Kiểm tra toàn diện xem WebDriver còn kết nối hay không với phát hiện sớm lỗi execution context.
    Phiên bản tối ưu với nhiều lớp kiểm tra và chi tiết lỗi.
    
    Args:
        driver: WebDriver cần kiểm tra
        throw_exception: Có ném ngoại lệ khi phát hiện lỗi không
        check_level: Mức độ kiểm tra (BASIC, STANDARD, DETAILED)
        max_retries: Số lần thử lại tối đa
        
    Returns:
        bool: True nếu driver còn kết nối và execution context hoạt động, False nếu không
        
    Raises:
        DriverConnectionError: Nếu throw_exception=True và driver không hoạt động
    """
    # Kiểm tra cờ shutdown trước khi bắt đầu
    if is_shutdown_requested():
        driver_logger.debug("Phát hiện cờ shutdown, bỏ qua kiểm tra driver connection")
        return False
    
    # Kiểm tra cơ bản
    if driver is None:
        if throw_exception:
            raise DriverConnectionError("Driver is None")
        return False
    
    # Đặt timeout thấp cho driver trong trường hợp shutdown
    original_page_timeout = None
    original_script_timeout = None
    try:
        # Lưu timeout hiện tại
        if hasattr(driver, 'timeouts'):
            if hasattr(driver.timeouts, 'page_load'):
                original_page_timeout = driver.timeouts.page_load
            if hasattr(driver.timeouts, 'script'):
                original_script_timeout = driver.timeouts.script
                
        # Đặt timeout thấp nếu đang shutdown
        if is_shutdown_requested():
            driver.set_page_load_timeout(1)
            driver.set_script_timeout(1)
    except Exception as e:
        driver_logger.debug(f"Không thể đặt timeout thấp: {str(e)}")
    
    # Chỉ retry một lần nếu đang trong quá trình shutdown 
    actual_max_retries = 1 if is_shutdown_requested() else max_retries
    
    try:
        # Kiểm tra cơ bản đầu tiên
        if not _check_basic_connection(driver, actual_max_retries):
            if throw_exception:
                raise DriverConnectionError("Kết nối cơ bản không hoạt động")
            return False
        
        # Nếu level là BASIC, dừng ở đây
        if check_level == "BASIC":
            return True
        
        # Kiểm tra execution context
        try:
            if not _check_context_execution(driver, actual_max_retries):
                if throw_exception:
                    raise DriverConnectionError("Execution context không hoạt động")
                return False
        except ExecutionContextError as e:
            if throw_exception:
                raise DriverConnectionError(str(e))
            return False
        
        # Nếu level là STANDARD, dừng ở đây
        if check_level == "STANDARD":
            return True
        
        # Kiểm tra window state
        if not _check_window_state(driver, actual_max_retries):
            if throw_exception:
                raise DriverConnectionError("Trạng thái cửa sổ không ổn định")
            return False
        
        # Kiểm tra process ChromeDriver còn chạy không (chỉ dành cho DETAILED)
        if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
            if driver.service.process is None or not driver.service.process.poll() is None:
                driver_logger.debug("ChromeDriver process không còn chạy")
                if throw_exception:
                    raise DriverConnectionError("ChromeDriver process không còn chạy")
                return False
        
        # Kiểm tra service connectivity (chỉ dành cho DETAILED)
        if hasattr(driver, 'service') and hasattr(driver.service, 'is_connectable'):
            if not driver.service.is_connectable():
                driver_logger.debug("Driver service không kết nối")
                if throw_exception:
                    raise DriverConnectionError("Driver service không còn kết nối")
                return False
        
        # Kiểm tra execution context chi tiết hơn (chỉ dành cho DETAILED)
        try:
            context_info = driver.execute_script("""
                return {
                    readyState: document.readyState,
                    location: window.location.href,
                    userAgent: navigator.userAgent,
                    timestamp: Date.now()
                };
            """)
            
            if not context_info or not isinstance(context_info, dict):
                driver_logger.debug("Execution context không trả về dữ liệu hợp lệ")
                if throw_exception:
                    raise DriverConnectionError("Execution context không trả về dữ liệu hợp lệ")
                return False
        except WebDriverException as e:
            error_msg = str(e).lower()
            if "no such execution context" in error_msg or "target closed" in error_msg:
                driver_logger.debug(f"Phát hiện lỗi execution context trong kiểm tra chi tiết: {error_msg}")
                if throw_exception:
                    raise DriverConnectionError(f"Lỗi execution context trong kiểm tra chi tiết: {str(e)}")
                return False
            else:
                driver_logger.debug(f"Lỗi WebDriver trong kiểm tra chi tiết: {error_msg}")
                if throw_exception:
                    raise DriverConnectionError(f"Lỗi WebDriver trong kiểm tra chi tiết: {str(e)}")
                return False
        
        # Tất cả kiểm tra đều thành công
        driver_logger.debug("Driver vẫn còn kết nối và execution context hoạt động tốt")
        return True
    
    finally:
        # Khôi phục timeout ban đầu nếu đã lưu và driver vẫn kết nối
        if not is_shutdown_requested() and driver is not None:
            try:
                if original_page_timeout is not None:
                    driver.set_page_load_timeout(original_page_timeout)
                if original_script_timeout is not None:
                    driver.set_script_timeout(original_script_timeout)
            except Exception:
                # Bỏ qua lỗi khôi phục timeout
                pass

# -------------------------------
# Hàm đóng driver - Đã tối ưu
# -------------------------------
def _handle_shutdown_case(driver, process_id=None):
    """
    Xử lý đặc biệt cho trường hợp shutdown.
    
    Args:
        driver: WebDriver cần đóng
        process_id: ID của tiến trình Chrome nếu đã biết
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        # Đặt timeout cực ngắn trước khi thử quit để tránh treo
        try:
            driver.set_page_load_timeout(1)
            driver.set_script_timeout(1)
            driver.implicitly_wait(1)
        except Exception:
            pass

        # Kiểm tra kết nối trước khi quit
        connection_active = False
        try:
            # Kiểm tra kết nối bằng cách truy cập thuộc tính cơ bản
            _ = driver.session_id
            connection_active = True
        except Exception:
            # Không thể truy cập session_id, kết nối đã mất
            pass
                
        if connection_active:
            # Thử quit với timeout rất ngắn
            try:
                driver_logger.debug("Thực hiện driver.quit() với timeout ngắn (shutdown mode)")
                driver.quit()
            except Exception as e:
                driver_logger.debug(f"Lỗi khi quit trong shutdown mode: {str(e)}")
        
        # Nếu đã lưu process ID, kill trực tiếp để đảm bảo đóng
        if process_id:
            try:
                import signal
                driver_logger.debug(f"Kill process ChromeDriver (PID: {process_id})")
                # Dùng SIGTERM trước
                os.kill(process_id, signal.SIGTERM)
                time.sleep(0.1)  # Đợi rất ngắn
                # Nếu vẫn còn, dùng SIGKILL
                if psutil.pid_exists(process_id):
                    os.kill(process_id, signal.SIGKILL)
            except Exception:
                pass
                
        return True
        
    except Exception:
        # Bỏ qua lỗi nếu đang shutdown
        return True

def _close_browser_tabs(driver, original_handle=None):
    """
    Đóng tất cả các tab trình duyệt.
    
    Args:
        driver: WebDriver cần đóng tab
        original_handle: Handle của tab hiện tại cần giữ lại
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        if hasattr(driver, 'window_handles') and len(driver.window_handles) > 1:
            driver_logger.debug(f"Đóng {len(driver.window_handles)} tab...")
            
            # Nếu không có original_handle, lấy handle hiện tại
            if not original_handle:
                try:
                    original_handle = driver.current_window_handle
                except Exception:
                    # Nếu không lấy được handle hiện tại, sử dụng handle đầu tiên
                    original_handle = driver.window_handles[0]
            
            # Đóng từng tab
            for handle in list(driver.window_handles):
                if handle != original_handle:
                    try:
                        driver.switch_to.window(handle)
                        driver.close()
                    except Exception as e:
                        driver_logger.debug(f"Không thể đóng tab {handle}: {str(e)}")
            
            # Quay lại tab ban đầu nếu còn tồn tại
            try:
                if original_handle in driver.window_handles:
                    driver.switch_to.window(original_handle)
                else:
                    # Nếu tab ban đầu đã đóng, chuyển đến tab đầu tiên
                    driver.switch_to.window(driver.window_handles[0])
            except Exception as e:
                driver_logger.debug(f"Lỗi khi quay lại tab ban đầu: {str(e)}")
                return False
                
        return True
        
    except Exception as e:
        driver_logger.debug(f"Lỗi khi đóng các tab: {str(e)}")
        return False

def _terminate_process(process_id, force=False):
    """
    Dừng tiến trình Chrome và GenLogin.
    
    Args:
        process_id: ID của tiến trình cần dừng
        force: Nếu True, sẽ sử dụng SIGKILL để buộc dừng
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    if not process_id:
        return True
        
    try:
        if psutil.pid_exists(process_id):
            driver_logger.debug(f"Dừng tiến trình PID={process_id}, force={force}")
            
            import signal
            try:
                # Dùng SIGTERM trước
                os.kill(process_id, signal.SIGTERM)
                
                # Đợi một chút để tiến trình kết thúc
                wait_time = 0.5
                driver_logger.debug(f"Đợi {wait_time}s để tiến trình kết thúc sau SIGTERM")
                time.sleep(wait_time)
                
                # Nếu vẫn còn và force=True, dùng SIGKILL
                if force and psutil.pid_exists(process_id):
                    driver_logger.debug(f"Tiến trình vẫn còn sau SIGTERM, dùng SIGKILL")
                    os.kill(process_id, signal.SIGKILL)
                    
                return True
            except ProcessLookupError:
                # Process đã không còn
                return True
            except Exception as e:
                driver_logger.warning(f"Lỗi khi dừng tiến trình PID={process_id}: {str(e)}")
                return False
        else:
            return True  # Process không tồn tại
            
    except Exception as e:
        driver_logger.debug(f"Lỗi khi kiểm tra/dừng tiến trình: {str(e)}")
        return False

def _close_selenium_driver(driver: Optional[webdriver.Chrome], chrome_process_id: Optional[int] = None, 
                          force_kill: bool = False, max_retries: int = 2) -> bool:
    """
    Đóng WebDriver instance an toàn và hiệu quả.
    
    Args:
        driver: Instance WebDriver cần đóng
        chrome_process_id: ID của tiến trình Chrome nếu đã biết
        force_kill: Nếu True, sẽ buộc kết thúc các tiến trình Chrome treo
        max_retries: Số lần thử lại tối đa khi đóng driver
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    if not driver:
        return True  # Không có driver để đóng
    
    success = True
    process_id = chrome_process_id
    
    try:
        # Lưu process ID sớm để có thể kill nếu cần
        if process_id is None and hasattr(driver, 'service') and hasattr(driver.service, 'process'):
            try:
                if driver.service.process:
                    process_id = driver.service.process.pid
            except Exception:
                pass

        # Kiểm tra nhanh, xử lý đặc biệt nếu đang shutdown
        if is_shutdown_requested():
            return _handle_shutdown_case(driver, process_id)
        
        # Kiểm tra kết nối bình thường
        connection_active = False
        try:
            # Kiểm tra kết nối bằng cách truy cập thuộc tính đơn giản
            _ = driver.session_id
            connection_active = True
        except Exception:
            # Không thể truy cập session_id, kết nối đã mất
            driver_logger.debug("Driver không còn kết nối (không thể truy cập session_id)")
            return True  # Không cần đóng nữa
        
        if not connection_active or not verify_driver_connection(driver, check_level="BASIC"):
            driver_logger.debug("Driver không còn kết nối, không gọi quit()")
            return True  # Không cần đóng nữa

        driver_logger.info("Đóng WebDriver...")
        
        # Đóng tất cả các tab
        if not _close_browser_tabs(driver):
            driver_logger.debug("Không thể đóng tất cả các tab, tiếp tục với quit()")
        
        # Thực hiện quit() với số lần thử
        quit_success = False
        max_attempts = 1 if is_shutdown_requested() else max_retries
        
        for retry in range(max_attempts):
            # Kiểm tra lại connection trước mỗi lần retry
            if retry > 0:
                try:
                    _ = driver.session_id
                except Exception:
                    driver_logger.debug(f"Driver mất kết nối trước lần retry {retry+1}, bỏ qua quit()")
                    break

            try:
                # Giảm timeout trước khi quit để tránh treo
                try:
                    driver.set_page_load_timeout(3)
                    driver.set_script_timeout(3)
                except Exception:
                    pass
                    
                driver_logger.debug("Thực hiện driver.quit()...")
                driver.quit()
                quit_success = True
                break
            except Exception as e:
                driver_logger.warning(f"Lỗi khi quit driver (lần {retry+1}/{max_attempts}): {str(e)}")
                # Đợi ngắn hơn giữa các lần retry
                time.sleep(0.2)
                
                # Kiểm tra xem có phải lỗi kết nối bị từ chối không
                if "connection refused" in str(e).lower() or "no connection" in str(e).lower():
                    driver_logger.debug("Phát hiện lỗi kết nối bị từ chối, chuyển sang kill process")
                    break
        
        # Force kill nếu quit() thất bại
        if not quit_success and (force_kill or process_id):
            success = _terminate_process(process_id, force=force_kill)
            
            # Thử forcefully terminate process từ driver.service nếu chưa làm
            if not success and hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                process = driver.service.process
                if process:
                    success = _terminate_process(process.pid, force=force_kill)
        
        driver_logger.debug("Đã đóng WebDriver thành công" if success else "Đóng WebDriver không thành công")
        
    except InvalidSessionIdException:
        driver_logger.debug("Driver session không còn hợp lệ, bỏ qua quit()")
    except WebDriverException as e:
        # Dùng xử lý riêng cho lỗi kết nối bị từ chối
        if "connection refused" in str(e).lower() or "no connection" in str(e).lower():
            driver_logger.debug(f"Lỗi kết nối khi đóng driver: {str(e)}")
            # Thử kill process nếu có thể
            if process_id:
                success = _terminate_process(process_id, force=True)
        else:
            driver_logger.warning(f"Lỗi WebDriver khi đóng: {str(e)}")
        success = False if not force_kill else success
    except Exception as e:
        driver_logger.warning(f"Lỗi khi đóng WebDriver: {type(e).__name__}: {str(e)}")
        success = False if not force_kill else success
    
    return success

def _cleanup_windows():
    """
    Dọn dẹp các tiến trình Chrome bị treo trên Windows.
    
    Returns:
        int: Số lượng tiến trình đã dọn dẹp
    """
    chrome_processes = []
    cleaned_count = 0
    
    try:
        import psutil
        # Tìm tất cả các tiến trình chrome.exe
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any("GenLogin" in arg for arg in cmdline if arg):
                        chrome_processes.append(proc)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        
        # Kết thúc các tiến trình tìm thấy
        for proc in chrome_processes:
            try:
                proc.terminate()
                driver_logger.debug(f"Đã kết thúc tiến trình Chrome bị treo, PID={proc.pid}")
                cleaned_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                driver_logger.debug(f"Không thể kết thúc tiến trình Chrome, PID={proc.pid}: {str(e)}")
        
    except ImportError:
        driver_logger.warning("Không thể import module psutil để dọn dẹp tiến trình Chrome")
        
        # Phương pháp thay thế sử dụng taskkill
        try:
            result = subprocess.run('taskkill /f /im chrome.exe', shell=True, 
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode == 0:
                driver_logger.info("Đã dọn dẹp các tiến trình Chrome bằng taskkill")
                cleaned_count = 1  # Không biết chính xác số lượng
        except Exception as e:
            driver_logger.warning(f"Lỗi khi chạy taskkill: {str(e)}")
    
    return cleaned_count

def _cleanup_unix(profile_id=None):
    """
    Dọn dẹp các tiến trình Chrome bị treo trên Linux/macOS.
    
    Args:
        profile_id: ID của profile GenLogin cần dọn dẹp tiến trình liên quan
        
    Returns:
        int: Số lượng tiến trình đã dọn dẹp (ước lượng)
    """
    try:
        cmd = []
        if profile_id:
            cmd = f"pkill -f 'chrome.*{profile_id}'"
        else:
            cmd = "pkill -f 'chrome.*GenLogin'"
        
        result = subprocess.run(cmd, shell=True, 
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # pkill trả về 0 nếu có ít nhất một process bị kill
        if result.returncode == 0:
            driver_logger.info(f"Đã chạy lệnh dọn dẹp Chrome: {cmd}")
            return 1  # Không biết chính xác số lượng
        else:
            return 0  # Không có process nào bị kill
            
    except Exception as e:
        driver_logger.warning(f"Lỗi khi dọn dẹp tiến trình Chrome: {str(e)}")
        return 0

def _cleanup_orphaned_chrome_processes(profile_id: Optional[str] = None) -> int:
    """
    Dọn dẹp các tiến trình Chrome bị treo liên quan đến profile_id.
    
    Args:
        profile_id: ID của profile GenLogin cần dọn dẹp tiến trình liên quan.
                   Nếu None, sẽ dọn dẹp tất cả tiến trình Chrome liên quan đến GenLogin.
    
    Returns:
        int: Số lượng tiến trình đã dọn dẹp (ước lượng)
    """
    driver_logger.info("Bắt đầu dọn dẹp các tiến trình Chrome bị treo...")
    
    try:
        if platform.system() == "Windows":
            # Xử lý trên Windows
            cleaned_count = _cleanup_windows()
            
            # Xử lý riêng cho profile_id cụ thể nếu có
            if profile_id:
                try:
                    cmd = f'taskkill /f /im chrome.exe /fi "WINDOWTITLE eq *{profile_id}*"'
                    subprocess.run(cmd, shell=True, 
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    driver_logger.debug(f"Đã chạy lệnh dọn dẹp cho profile {profile_id}")
                except Exception as e:
                    driver_logger.warning(f"Lỗi khi chạy taskkill cho profile cụ thể: {str(e)}")
        else:
            # Xử lý trên Linux/macOS
            cleaned_count = _cleanup_unix(profile_id)
            
        if cleaned_count > 0:
            driver_logger.info(f"Đã dọn dẹp khoảng {cleaned_count} tiến trình Chrome bị treo")
        else:
            driver_logger.debug("Không tìm thấy tiến trình Chrome cần dọn dẹp")
            
        return cleaned_count
            
    except Exception as e:
        driver_logger.warning(f"Lỗi khi dọn dẹp các tiến trình Chrome bị treo: {str(e)}")
        return 0

def _stop_genlogin_profile(gen: Optional[Any], profile_id: Optional[str], 
                          force_kill: bool = False) -> bool:
    """
    Dừng profile GenLogin.
    
    Args:
        gen: Instance GenLogin API
        profile_id: ID của profile GenLogin
        force_kill: Nếu True, sẽ dùng stopBrowser và force kill nếu cần
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    if not gen or not profile_id:
        return True  # Không có gì để dừng
    
    success = True
    
    # Nếu đang shutdown, sử dụng cách tiếp cận nhanh chóng
    if is_shutdown_requested():
        try:
            # Giảm timeout trước khi gọi API
            gen.request_timeout = 2  # Timeout cực ngắn khi đang shutdown
            
            # Gọi stopProfile một cách "fire and forget"
            try:
                gen.stopProfile(profile_id)
            except Exception:
                # Nếu không được, thử gọi stopBrowser
                try:
                    gen.stopBrowser(profile_id)
                except Exception:
                    pass
        except Exception:
            # Bỏ qua lỗi nếu đang shutdown
            pass
        
        return True
    
    # Xử lý bình thường nếu không đang shutdown
    try:
        # Đặt timeout ngắn cho các request để tránh treo
        original_timeout = getattr(gen, 'request_timeout', 30)
        gen.request_timeout = 5  # 5 giây là đủ cho API đơn giản
        
        # Kiểm tra xem profile có đang chạy không
        is_running = False
        try:
            # Dùng getWsEndpoint nhưng với timeout ngắn
            ws_data = gen.getWsEndpoint(profile_id)
            if ws_data.get("data", {}).get("wsEndpoint", ""):
                is_running = True
        except Exception:
            # Nếu có lỗi, thử cách khác với getProfilesRunning 
            try:
                running_profiles = gen.getProfilesRunning()
                items = running_profiles.get("data", {}).get("items", [])
                is_running = any(p.get("id") == profile_id for p in items if isinstance(p, dict))
            except Exception:
                # Nếu vẫn lỗi, giả định đang chạy để an toàn
                is_running = True
        
        # Dừng profile nếu đang chạy
        if is_running:
            driver_logger.info(f"Thử dừng profile {profile_id} qua Genlogin...")
            try:
                stop_result = gen.stopProfile(profile_id)
                if stop_result.get("success", False):
                    driver_logger.info(f"Đã dừng profile {profile_id}")
                else:
                    driver_logger.warning(f"Không thể dừng profile {profile_id}")
                    # Thử dùng stopBrowser nếu stopProfile thất bại và force_kill=True
                    if force_kill:
                        try:
                            driver_logger.info(f"Thử dùng stopBrowser cho profile {profile_id}...")
                            gen.stopBrowser(profile_id)
                            driver_logger.info(f"Đã dừng browser cho profile {profile_id}")
                        except Exception as sb_e:
                            driver_logger.warning(f"Lỗi khi dùng stopBrowser: {str(sb_e)}")
                
                # Kiểm tra profile đã dừng chưa
                try:
                    time.sleep(1.5)  # Đợi để đảm bảo API đã xử lý lệnh dừng
                    running_check = gen.getProfilesRunning()
                    running_ids = [p.get("id") for p in running_check.get("data", {}).get("items", []) 
                                  if isinstance(p, dict)]
                                  
                    if profile_id in running_ids:
                        driver_logger.warning(f"Profile {profile_id} vẫn đang chạy sau khi gọi stopProfile")
                        
                        # Thử lại với stopBrowser nếu force_kill = True và chưa thử
                        if force_kill and stop_result.get("success", False):
                            driver_logger.info(f"Cố gắng dừng mạnh profile {profile_id} với stopBrowser...")
                            gen.stopBrowser(profile_id)
                            time.sleep(1.5)
                    else:
                        driver_logger.debug(f"Profile {profile_id} đã dừng thành công")
                except Exception as check_e:
                    driver_logger.debug(f"Không thể kiểm tra trạng thái profile sau khi dừng: {str(check_e)}")
                    
            except Exception as e:
                driver_logger.warning(f"Lỗi khi dừng profile {profile_id}: {type(e).__name__}: {str(e)}")
                success = False if not force_kill else success
        else:
            driver_logger.debug(f"Profile {profile_id} đã dừng sẵn")
        
        # Khôi phục timeout ban đầu
        gen.request_timeout = original_timeout
        
    except Exception as e:
        driver_logger.warning(f"Lỗi khi dừng profile {profile_id}: {type(e).__name__}: {str(e)}")
        success = False if not force_kill else success
    
    return success

def close_current_driver(
    driver: Optional[webdriver.Chrome], 
    gen: Optional[Any], 
    profile_id: Optional[str], 
    force_kill: bool = False,
    max_retries: int = 2
    ) -> bool:
    """
    Đóng WebDriver và dừng profile GenLogin một cách an toàn và hiệu quả.
    
    Args:
        driver: Instance WebDriver cần đóng
        gen: Instance GenLogin API
        profile_id: ID của profile GenLogin
        force_kill: Nếu True, sẽ buộc kết thúc các tiến trình Chrome treo
        max_retries: Số lần thử lại tối đa khi đóng driver
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    success = True
    chrome_process_id = None
    
    try:
        # --- Phần 1: Lưu process ID của Chrome nếu có thể ---
        if driver and hasattr(driver, 'service') and hasattr(driver.service, 'process'):
            try:
                if driver.service.process:
                    chrome_process_id = driver.service.process.pid
            except Exception:
                pass
        
        # --- Phần 2: Đóng WebDriver ---
        if driver:
            selenium_success = _close_selenium_driver(driver, chrome_process_id, force_kill, max_retries)
            success = success and selenium_success
        
        # --- Phần 3: Dừng profile GenLogin ---
        if gen and profile_id:
            genlogin_success = _stop_genlogin_profile(gen, profile_id, force_kill)
            success = success and genlogin_success
    
    except Exception as e:
        driver_logger.error(f"Lỗi không mong đợi trong close_current_driver: {type(e).__name__}: {str(e)}")
        success = False if not force_kill else success
    
    finally:
        # Nếu vẫn còn process ChromeDriver, kill trực tiếp
        if chrome_process_id:
            try:
                if psutil.pid_exists(chrome_process_id):
                    import signal
                    os.kill(chrome_process_id, signal.SIGKILL)
                    driver_logger.debug(f"Đã kill process ChromeDriver trong finally (PID: {chrome_process_id})")
            except Exception:
                pass
                
        # Dọn dẹp các tiến trình Chrome treo nếu force_kill
        if force_kill and profile_id:
            _cleanup_orphaned_chrome_processes(profile_id)
                
        # Đảm bảo tham chiếu được giải phóng
        driver = None
        gen = None
        
    return success

# -------------------------------
# Xử lý trang và cơ chế retry - Đã tối ưu
# -------------------------------
def _check_document_ready(driver, timeout=10):
    """
    Kiểm tra document.readyState.
    
    Args:
        driver: WebDriver cần kiểm tra
        timeout: Thời gian tối đa chờ đợi (giây)
        
    Returns:
        bool: True nếu document.readyState = "complete", False nếu không
    """
    try:
        # Sử dụng phương pháp polling an toàn hơn thay vì WebDriverWait
        poll_start = time.time()
        while time.time() - poll_start < timeout:
            try:
                ready_state = driver.execute_script("return document.readyState")
                if ready_state == "complete":
                    driver_logger.debug(f"Trang đã load (readyState=complete): {driver.current_url}")
                    return True
            except WebDriverException as poll_e:
                if "no such window" in str(poll_e).lower():
                    driver_logger.error(f"Cửa sổ đã đóng khi kiểm tra readyState: {str(poll_e)}")
                    return False
                # Bỏ qua lỗi tạm thời khác và tiếp tục poll
                pass
            time.sleep(0.2)
        
        # Hết timeout mà chưa hoàn thành
        driver_logger.warning(f"Timeout ({timeout}s) khi đợi document.readyState=complete: {driver.current_url}")
        return False
        
    except NoSuchWindowException as e:
        driver_logger.error(f"Lỗi 'No Such Window' khi kiểm tra readyState: {str(e)}")
        return False
    except WebDriverException as e:
        if "no such window" in str(e).lower():
            driver_logger.error(f"Cửa sổ đã đóng khi kiểm tra readyState: {str(e)}")
            return False
        elif "no such execution context" in str(e):
            driver_logger.warning(f"Lỗi context không hợp lệ khi kiểm tra readyState: {str(e)}")
            return False
        else:
            driver_logger.warning(f"Lỗi khi kiểm tra readyState: {str(e)}")
            return False
    except Exception as e:
        driver_logger.warning(f"Lỗi không xác định khi kiểm tra readyState: {driver_logger.format_error(e)}")
        return False

def _check_network_idle(driver, timeout=10):
    """
    Kiểm tra network đã ổn định chưa (jQuery.active = 0).
    
    Args:
        driver: WebDriver cần kiểm tra
        timeout: Thời gian tối đa chờ đợi (giây)
        
    Returns:
        bool: True nếu network đã ổn định, False nếu không
    """
    try:
        # Kiểm tra jQuery bằng polling thay vì WebDriverWait
        jquery_check = False
        poll_start = time.time()
        
        try:
            jquery_check = driver.execute_script("return typeof jQuery !== 'undefined'")
        except WebDriverException as jq_e:
            if "no such window" in str(jq_e).lower():
                driver_logger.error(f"Cửa sổ đã đóng khi kiểm tra jQuery: {str(jq_e)}")
                return False
            driver_logger.debug(f"Lỗi khi kiểm tra jQuery: {str(jq_e)}")
            jquery_check = False
        
        if jquery_check:
            driver_logger.debug("Tìm thấy jQuery, đợi jQuery.active = 0")
            while time.time() - poll_start < timeout:
                try:
                    jquery_active = driver.execute_script("return jQuery.active")
                    if jquery_active == 0:
                        driver_logger.debug("Các AJAX request đã hoàn tất (jQuery.active=0)")
                        return True
                except WebDriverException as active_e:
                    if "no such window" in str(active_e).lower():
                        driver_logger.error(f"Cửa sổ đã đóng khi kiểm tra jQuery.active: {str(active_e)}")
                        return False
                    # Bỏ qua lỗi tạm thời và tiếp tục poll
                    pass
                time.sleep(0.2)
                
            # Hết timeout mà chưa hoàn thành
            driver_logger.warning(f"Timeout khi đợi jQuery.active=0: {driver.current_url}")
            return False
        else:
            driver_logger.debug("Không tìm thấy jQuery, kiểm tra network idle bằng phương pháp khác")
            # Coi như đã ổn định nếu không có jQuery
            return True
            
    except NoSuchWindowException as e:
        driver_logger.error(f"Lỗi 'No Such Window' khi kiểm tra network idle: {str(e)}")
        return False
    except WebDriverException as e:
        if "no such window" in str(e).lower():
            driver_logger.error(f"Cửa sổ đã đóng khi kiểm tra network idle: {str(e)}")
            return False
        elif "no such execution context" in str(e):
            driver_logger.warning(f"Lỗi context không hợp lệ khi kiểm tra network idle: {str(e)}")
            return False
        else:
            driver_logger.debug(f"Lỗi khi kiểm tra network idle: {str(e)}")
            # Vẫn tiếp tục vì đây không phải lỗi nghiêm trọng
            return True
    except Exception as e:
        driver_logger.warning(f"Lỗi không xác định khi kiểm tra network idle: {driver_logger.format_error(e)}")
        return True

def _check_dom_stability(driver, timeout=2):
    """
    Kiểm tra độ ổn định của DOM dựa trên kích thước HTML.
    
    Args:
        driver: WebDriver cần kiểm tra
        timeout: Thời gian tối đa chờ đợi (giây)
        
    Returns:
        bool: True nếu DOM ổn định, False nếu không
    """
    try:
        # Lấy kích thước HTML ban đầu
        initial_html_size = len(driver.page_source)
        
        # Đợi một khoảng thời gian
        time.sleep(timeout / 2)
        
        # Lấy kích thước HTML sau một khoảng thời gian
        final_html_size = len(driver.page_source)
        
        # Tính toán sự thay đổi
        change_percentage = abs(final_html_size - initial_html_size) / max(initial_html_size, 1) * 100
        
        # Nếu thay đổi nhỏ hơn 1%, coi như DOM ổn định
        if change_percentage < 1:
            driver_logger.debug(f"DOM ổn định (thay đổi {change_percentage:.2f}%)")
            return True
        else:
            driver_logger.debug(f"DOM chưa ổn định (thay đổi {change_percentage:.2f}%)")
            # Đợi thêm một khoảng thời gian
            time.sleep(timeout / 2)
            return False
            
    except NoSuchWindowException as e:
        driver_logger.error(f"Lỗi 'No Such Window' khi kiểm tra DOM: {str(e)}")
        return False
    except WebDriverException as e:
        if "no such window" in str(e).lower():
            driver_logger.error(f"Cửa sổ đã đóng khi kiểm tra DOM: {str(e)}")
            return False
        elif "no such execution context" in str(e):
            driver_logger.warning(f"Lỗi context không hợp lệ khi kiểm tra DOM: {str(e)}")
            return False
        else:
            driver_logger.debug(f"Lỗi khi kiểm tra DOM: {str(e)}")
            # Vẫn tiếp tục vì đây không phải lỗi nghiêm trọng
            return True
    except Exception as e:
        driver_logger.warning(f"Lỗi không xác định khi kiểm tra DOM: {driver_logger.format_error(e)}")
        return True

def wait_for_page_load(
    driver: webdriver.Chrome,
    timeout: int = DEFAULT_CONFIG.timeouts["page_load"],
    check_js_complete: bool = True,
    check_network_idle: bool = False,
    check_dom_stability: bool = False,
    max_retries: int = 3,
    retry_on_timeout: bool = False
    ) -> bool:
    """
    Đợi trang tải hoàn tất với cơ chế state machine và xử lý lỗi tốt hơn.
    
    Args:
        driver (webdriver.Chrome): Đối tượng WebDriver của Selenium.
        timeout (int): Thời gian tối đa (giây) để đợi trang load, mặc định là 30.
        check_js_complete (bool): Kiểm tra document.readyState, mặc định là True.
        check_network_idle (bool): Kiểm tra trạng thái AJAX qua jQuery, mặc định là False.
        check_dom_stability (bool): Kiểm tra độ ổn định của DOM, mặc định là False.
        max_retries (int): Số lần thử lại khi gặp lỗi context, mặc định là 3.
        retry_on_timeout (bool): Thử lại khi hết timeout bằng cách refresh trang, mặc định là False.

    Returns:
        bool: True nếu trang load thành công, False nếu thất bại.
    """
    if not driver:
        driver_logger.error("wait_for_page_load: driver = None")
        return False

    # Kiểm tra driver còn hoạt động không trước khi tiếp tục
    try:
        # Thử lấy thông tin cơ bản để đảm bảo driver còn kết nối
        _ = driver.window_handles
    except NoSuchWindowException as e:
        driver_logger.error(f"Lỗi 'No Such Window' khi bắt đầu wait_for_page_load: {str(e)}")
        return False
    except WebDriverException as e:
        driver_logger.error(f"Lỗi WebDriver khi bắt đầu wait_for_page_load: {str(e)}")
        return False

    # Thời gian bắt đầu
    start_time = time.time()
    
    # Bước 1: Kiểm tra document.readyState
    if check_js_complete:
        js_retry_count = 0
        while js_retry_count < max_retries:
            # Kiểm tra quá thời gian chưa
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                if retry_on_timeout and js_retry_count < max_retries - 1:
                    driver_logger.warning(f"Timeout ({timeout}s) khi đợi document.readyState=complete, thử refresh trang")
                    try:
                        driver.refresh()
                        time.sleep(1)
                    except WebDriverException as refresh_e:
                        driver_logger.warning(f"Lỗi khi refresh trang: {str(refresh_e)}")
                    js_retry_count += 1
                    start_time = time.time()  # Reset thời gian bắt đầu
                    continue
                else:
                    driver_logger.warning(f"Timeout ({timeout}s) khi đợi document.readyState=complete: {driver.current_url}")
                    return False
            
            # Dùng hàm kiểm tra document ready đã tách riêng
            remaining_timeout = max(1, timeout - elapsed)
            if _check_document_ready(driver, remaining_timeout):
                break  # Đã hoàn tất
            
            # Nếu chưa hoàn tất, thử lại nếu còn retry
            js_retry_count += 1
            if js_retry_count >= max_retries:
                driver_logger.error("Hết số lần thử kiểm tra JS readyState")
                return False
    
    # Bước 2: Kiểm tra network idle
    if check_network_idle:
        # Tính thời gian còn lại trong timeout
        elapsed = time.time() - start_time
        remaining_timeout = max(1, timeout - elapsed)
        
        network_retry_count = 0
        while network_retry_count < max_retries:
            # Kiểm tra quá thời gian chưa
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                if retry_on_timeout and network_retry_count < max_retries - 1:
                    driver_logger.warning(f"Timeout ({timeout}s) khi đợi network idle, thử refresh trang")
                    try:
                        driver.refresh()
                        time.sleep(1)
                    except WebDriverException as refresh_e:
                        driver_logger.warning(f"Lỗi khi refresh trang: {str(refresh_e)}")
                    network_retry_count += 1
                    start_time = time.time()  # Reset thời gian bắt đầu
                    continue
                else:
                    driver_logger.warning(f"Timeout ({timeout}s) khi đợi network idle: {driver.current_url}")
                    # Vẫn coi là thành công vì đã qua bước document.readyState
                    break
            
            # Dùng hàm kiểm tra network idle đã tách riêng
            remaining_timeout = max(1, timeout - elapsed)
            if _check_network_idle(driver, remaining_timeout):
                break  # Đã hoàn tất
            
            # Nếu chưa hoàn tất, thử lại nếu còn retry
            network_retry_count += 1
            if network_retry_count >= max_retries:
                driver_logger.warning("Hết số lần thử kiểm tra network idle")
                # Vẫn coi là thành công vì đã qua bước document.readyState
                break
    
    # Bước 3: Kiểm tra độ ổn định của DOM
    if check_dom_stability:
        # Tính thời gian còn lại trong timeout
        elapsed = time.time() - start_time
        remaining_timeout = max(1, timeout - elapsed)
        
        dom_retry_count = 0
        while dom_retry_count < max_retries:
            # Kiểm tra quá thời gian chưa
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                if retry_on_timeout and dom_retry_count < max_retries - 1:
                    driver_logger.warning(f"Timeout ({timeout}s) khi đợi DOM ổn định, thử refresh trang")
                    try:
                        driver.refresh()
                        time.sleep(1)
                    except WebDriverException as refresh_e:
                        driver_logger.warning(f"Lỗi khi refresh trang: {str(refresh_e)}")
                    dom_retry_count += 1
                    start_time = time.time()  # Reset thời gian bắt đầu
                    continue
                else:
                    driver_logger.warning(f"Timeout ({timeout}s) khi đợi DOM ổn định: {driver.current_url}")
                    # Vẫn coi là thành công vì đã qua các bước trước
                    break
            
            # Dùng hàm kiểm tra DOM stability đã tách riêng
            stability_timeout = min(DEFAULT_CONFIG.timeouts["dom_stability"], remaining_timeout)
            if _check_dom_stability(driver, stability_timeout):
                break  # Đã hoàn tất
            
            # Nếu chưa hoàn tất, thử lại nếu còn retry
            dom_retry_count += 1
            if dom_retry_count >= max_retries:
                driver_logger.warning("Hết số lần thử kiểm tra DOM ổn định")
                # Vẫn coi là thành công vì đã qua các bước trước
                break
    
    try:
        # Kiểm tra cửa sổ trước khi chờ thêm
        try:
            _ = driver.window_handles
            # Chờ thêm 0.5 giây để đảm bảo mạng ổn định
            time.sleep(0.5)
            return True
        except NoSuchWindowException:
            driver_logger.error("Cửa sổ đã đóng trong bước cuối cùng của wait_for_page_load")
            return False
    except InvalidSessionIdException as e:
        driver_logger.error(f"Phiên WebDriver đã hết hạn: {str(e)}")
        return False
    except WebDriverException as e:
        if "no such window" in str(e).lower():
            driver_logger.error(f"Cửa sổ đã đóng trong bước cuối cùng: {str(e)}")
            return False
        driver_logger.warning(f"Lỗi WebDriver trong bước cuối cùng: {str(e)}")
        return False
    except Exception as e:
        driver_logger.warning(f"Lỗi không xác định khi đợi trang load: {type(e).__name__}: {str(e)}")
        return False

class RetryStrategy:
    """
    Lớp chiến lược retry linh hoạt cho việc thực thi các hàm.
    """
    
    def __init__(
        self, 
        max_retries=DEFAULT_CONFIG.retry["max_retries"], 
        base_delay=DEFAULT_CONFIG.retry["base_delay"],
        backoff_factor=DEFAULT_CONFIG.retry["backoff_factor"],
        jitter=DEFAULT_CONFIG.retry["jitter"],
        error_types=None,
        pre_retry_hook=None,
        post_retry_hook=None
    ):
        """
        Khởi tạo RetryStrategy.
        
        Args:
            max_retries: Số lần thử lại tối đa
            base_delay: Độ trễ cơ bản giữa các lần thử (giây)
            backoff_factor: Hệ số tăng thời gian chờ
            jitter: Thêm nhiễu ngẫu nhiên vào thời gian chờ
            error_types: Danh sách các loại lỗi cần xử lý
            pre_retry_hook: Hàm được gọi trước mỗi lần retry
            post_retry_hook: Hàm được gọi sau mỗi lần retry
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        
        # Nếu không chỉ định error_types, sử dụng các loại lỗi mặc định
        if error_types is None:
            self.error_types = [WebDriverException, TimeoutException, StaleElementReferenceException]
        else:
            self.error_types = error_types
            
        self.pre_retry_hook = pre_retry_hook
        self.post_retry_hook = post_retry_hook
    
    def calculate_wait_time(self, attempt):
        """
        Tính toán thời gian chờ giữa các lần retry.
        
        Args:
            attempt: Số lần thử đã thực hiện
            
        Returns:
            float: Thời gian chờ (giây)
        """
        wait_time = self.base_delay * (self.backoff_factor ** attempt)
        
        if self.jitter:
            # Thêm jitter từ -20% đến +20%
            jitter_factor = random.uniform(0.8, 1.2)
            wait_time *= jitter_factor
            
        return wait_time
    
    def should_retry(self, exception, attempt):
        """
        Quyết định xem có nên retry không dựa trên exception và số lần đã thử.
        
        Args:
            exception: Exception đã bắt được
            attempt: Số lần thử đã thực hiện
            
        Returns:
            bool: True nếu nên retry, False nếu không
        """
        # Kiểm tra cờ shutdown
        if is_shutdown_requested():
            return False
            
        # Kiểm tra số lần thử
        if attempt >= self.max_retries:
            return False
            
        # Kiểm tra loại exception
        for error_type in self.error_types:
            if isinstance(exception, error_type):
                return True
                
        return False
        
    def execute(self, func, *args, **kwargs):
        """
        Thực thi hàm với cơ chế retry.
        
        Args:
            func: Hàm cần thực thi
            *args, **kwargs: Tham số cho hàm
            
        Returns:
            Any: Kết quả trả về của hàm
            
        Raises:
            Exception: Nếu không thể thực thi hàm sau tất cả các lần thử
        """
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                # Kiểm tra cờ shutdown trước khi thực thi
                if is_shutdown_requested():
                    driver_logger.warning("Phát hiện yêu cầu shutdown, hủy thực thi hàm")
                    raise RuntimeError("Shutdown requested")
                    
                return func(*args, **kwargs)
                
            except tuple(self.error_types) as e:
                last_exception = e
                driver_logger.warning(f"Lỗi khi thực thi (lần {attempt+1}/{self.max_retries}): {type(e).__name__}: {str(e)}")
                
                # Kiểm tra xem có nên retry không
                if not self.should_retry(e, attempt):
                    break
                    
                # Gọi pre_retry_hook nếu có
                if self.pre_retry_hook:
                    try:
                        self.pre_retry_hook(attempt, e)
                    except Exception as hook_e:
                        driver_logger.debug(f"Lỗi trong pre_retry_hook: {str(hook_e)}")
                
                # Tính thời gian chờ và đợi
                wait_time = self.calculate_wait_time(attempt)
                driver_logger.debug(f"Chờ {wait_time:.2f}s trước khi retry...")
                time.sleep(wait_time)
                
                # Gọi post_retry_hook nếu có
                if self.post_retry_hook:
                    try:
                        self.post_retry_hook(attempt, e)
                    except Exception as hook_e:
                        driver_logger.debug(f"Lỗi trong post_retry_hook: {str(hook_e)}")
            
            except Exception as e:
                # Bắt tất cả các exception không trong danh sách error_types
                driver_logger.error(f"Lỗi không nằm trong danh sách retry: {type(e).__name__}: {str(e)}")
                raise
        
        # Nếu đã hết số lần thử và có exception cuối cùng, ném lại exception đó
        if last_exception:
            driver_logger.error(f"Thất bại sau {self.max_retries} lần retry: {type(last_exception).__name__}: {str(last_exception)}")
            raise last_exception
            
        # Nếu đã hết số lần thử nhưng không có exception, ném RuntimeError
        raise RuntimeError(f"Thất bại không xác định sau {self.max_retries} lần retry")

def execute_with_retry(
    driver: webdriver.Chrome,
    func: Callable,
    max_retries: int = DEFAULT_CONFIG.retry["max_retries"],
    retry_delay: float = DEFAULT_CONFIG.retry["base_delay"],
    backoff_factor: float = DEFAULT_CONFIG.retry["backoff_factor"],
    jitter: bool = DEFAULT_CONFIG.retry["jitter"],
    error_types: List[type] = None
    ) -> Any:
    """
    Thực thi hàm với cơ chế retry có exponential backoff và jitter.
    
    Args:
        driver: WebDriver instance để thực hiện hành động
        func: Hàm cần thực thi, nhận driver làm tham số đầu tiên
        max_retries: Số lần thử lại tối đa
        retry_delay: Độ trễ ban đầu giữa các lần thử (giây)
        backoff_factor: Hệ số tăng thời gian chờ
        jitter: Thêm nhiễu ngẫu nhiên vào thời gian chờ
        error_types: Danh sách các loại lỗi cần xử lý, mặc định là WebDriverException, 
                    TimeoutException, StaleElementReferenceException
        
    Returns:
        Any: Kết quả trả về của hàm func
        
    Raises:
        Exception: Nếu không thể thực thi hàm sau tất cả các lần thử
    """
    # Khởi tạo RetryStrategy
    retry_strategy = RetryStrategy(
        max_retries=max_retries,
        base_delay=retry_delay,
        backoff_factor=backoff_factor,
        jitter=jitter,
        error_types=error_types
    )
    
    # Nếu func không nhận driver làm tham số, gọi trực tiếp
    if driver is None:
        return retry_strategy.execute(func)
    
    # Nếu func nhận driver làm tham số, gọi với driver
    return retry_strategy.execute(func, driver)

# -------------------------------
# Hàm tiện ích
# -------------------------------
def is_element_present(driver: webdriver.Chrome, by: str, value: str, timeout: int = 0) -> bool:
    """
    Kiểm tra xem element có tồn tại hay không.
    
    Args:
        driver: WebDriver instance cần kiểm tra
        by: Phương thức định vị (ID, CSS_SELECTOR, XPATH, v.v.)
        value: Giá trị định vị
        timeout: Thời gian chờ nếu > 0 (giây)
        
    Returns:
        bool: True nếu element tồn tại, False nếu không
    """
    if not driver:
        return False
    try:
        if timeout > 0:
            locator = (getattr(By, by.upper()), value)
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
            return True
        else:
            driver.find_element(getattr(By, by.upper()), value)
            return True
    except Exception:
        return False

def get_chrome_version(driver: webdriver.Chrome) -> str:
    """
    Lấy phiên bản Chrome đang chạy qua CDP.
    
    Args:
        driver: WebDriver instance đang chạy
        
    Returns:
        str: Chuỗi chứa phiên bản Chrome, hoặc "unknown" nếu không thể lấy
    """
    try:
        if not verify_driver_connection(driver, check_level="BASIC"):
            return "unknown (driver disconnected)"
        version_info = driver.execute_cdp_cmd("Browser.getVersion", {})
        if not version_info:
            return "unknown (no response)"
        product = version_info.get("product", "")
        user_agent = version_info.get("userAgent", "")
        if product and "Chrome/" in product:
            return product
        elif user_agent and "Chrome/" in user_agent:
            import re
            match = re.search(r'Chrome/(\d+\.\d+\.\d+\.\d+)', user_agent)
            if match:
                return f"Chrome/{match.group(1)}"
        return "unknown (couldn't parse)"
    except Exception as e:
        driver_logger.debug(f"Lỗi khi lấy phiên bản Chrome: {str(e)}")
        return "unknown (error)"

def close_all_tabs(driver: webdriver.Chrome):
    """
    Đóng tất cả các tab ngoại trừ tab hiện tại.
    
    Args:
        driver: WebDriver instance cần xử lý
    """
    _close_browser_tabs(driver)

# -------------------------------
# End of Module
# -------------------------------