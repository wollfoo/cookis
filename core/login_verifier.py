"""
login_verifier.py - Module xác thực trạng thái đăng nhập Azure/Microsoft
- Kiểm tra chính xác trạng thái đăng nhập thông qua DOM, URL patterns
- Đảm bảo xử lý lỗi "No Such Execution Context" và các vấn đề WebDriver
- Cung cấp cơ chế retry và xử lý lỗi mạnh mẽ
- Tương thích với WebDriverPool và các components khác
"""

import logging
import time
import re
from datetime import datetime
from typing import Tuple, Dict, Any, Optional, List, Union
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, InvalidSessionIdException,
    NoSuchElementException, StaleElementReferenceException
)

# Import từ các module khác trong project
from global_state import is_shutdown_requested

from webdriver_pool import retry_with_backoff

# Nếu không, định nghĩa tại đây
# def retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2, jitter=True):
#     """
#     Decorator tối ưu với kiểm tra shutdown trước và sau mỗi lần retry
#     """
#     import random
#     from functools import wraps
    
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             # Kiểm tra shutdown trước khi bắt đầu
#             if is_shutdown_requested():
#                 logger = logging.getLogger()
#                 logger.warning(f"Shutdown đã được yêu cầu trước khi thực thi {func.__name__}")
#                 raise RuntimeError("Shutdown requested")
            
#             retry_count = 0
#             delay = initial_delay
#             last_exception = None
            
#             while retry_count < max_retries:
#                 try:
#                     return func(*args, **kwargs)
#                 except (WebDriverException, TimeoutException, StaleElementReferenceException) as e:
#                     # Kiểm tra shutdown ngay sau khi bắt exception
#                     if is_shutdown_requested():
#                         logger = logging.getLogger()
#                         logger.warning(f"Shutdown được yêu cầu sau exception, dừng retry {func.__name__}")
#                         raise RuntimeError("Shutdown requested during retry") from e
                    
#                     retry_count += 1
#                     last_exception = e
#                     if retry_count >= max_retries:
#                         break
                        
#                     # Tính thời gian chờ với backoff
#                     wait_time = delay * (backoff_factor ** (retry_count - 1))
#                     if jitter:
#                         wait_time *= random.uniform(0.8, 1.2)
                    
#                     logger = logging.getLogger()
#                     logger.warning(
#                         f"Lỗi {type(e).__name__} khi thực thi {func.__name__} "
#                         f"(lần thử {retry_count}/{max_retries}): {str(e)}. "
#                         f"Thử lại sau {wait_time:.2f}s"
#                     )
                    
#                     # Kiểm tra lần nữa trước khi sleep
#                     if is_shutdown_requested():
#                         logger.warning(f"Shutdown được yêu cầu trước khi sleep, dừng retry {func.__name__}")
#                         raise RuntimeError("Shutdown requested before retry sleep") from e
                    
#                     # Sử dụng sleep với kiểm tra cờ shutdown
#                     for _ in range(int(wait_time * 10)):  # Chia nhỏ sleep thành 0.1s
#                         if is_shutdown_requested():
#                             logger.warning(f"Shutdown được yêu cầu trong khi sleep, dừng retry {func.__name__}")
#                             raise RuntimeError("Shutdown requested during retry sleep") from e
#                         time.sleep(0.1)
                    
#             # Nếu hết lần retry mà vẫn lỗi, raise exception
#             if last_exception:
#                 raise last_exception
                
#         return wrapper
#     return decorator

class NoSuchExecutionContextException(WebDriverException):
    """Ngoại lệ riêng cho trường hợp execution context không tồn tại"""
    pass

class LoginVerifier:
    """
    Lớp chuyên trách xác thực đăng nhập và kiểm tra trạng thái người dùng.
    Cải tiến để khắc phục lỗi 'No Such Execution Context' và các vấn đề liên quan.
    """

    # URL Patterns để xác định trạng thái đăng nhập
    LOGGED_IN_URL_PATTERNS = ["portal.azure.com/#home", "portal.azure.com/#blade"]
    # Mở rộng patterns để đảm bảo bắt được nhiều trường hợp đăng nhập
    ADDITIONAL_LOGGED_IN_PATTERNS = ["portal.azure.com/#view", "portal.azure.com/dashboard"]
    
    # Page elements tiêu chuẩn của Azure Portal
    AZURE_PORTAL_SELECTORS = [
        {"type": By.TAG_NAME, "value": "h1", "name": "h1_element"},
        {"type": By.TAG_NAME, "value": "h2", "name": "h2_element"},
        {"type": By.CSS_SELECTOR, "value": "div.fxs-blade-content", "name": "blade_content"},
        {"type": By.CSS_SELECTOR, "value": "div.fxs-portal", "name": "portal_content"},
        {"type": By.CSS_SELECTOR, "value": "div.azc-grid", "name": "grid_content"},
        # Selector mới cho menu sidebar
        {"type": By.CSS_SELECTOR, "value": "nav.fxs-sidebar", "name": "sidebar_menu"},
        # Selector cho user menu dropdown
        {"type": By.CSS_SELECTOR, "value": "div.fxs-avatarmenu", "name": "user_menu"}
    ]
    
    def __init__(self, config, logger=None):
        """
        Khởi tạo LoginVerifier
        
        Args:
            config: Đối tượng cấu hình chứa URL kiểm tra đăng nhập và các thiết lập timeout
            logger: Logger để ghi nhật ký (tùy chọn)
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        # Thêm các thuộc tính để tracking lỗi
        self.error_counts = {
            "execution_context": 0,
            "navigation": 0,
            "page_load_timeout": 0,
            "no_such_window": 0,
            "other_errors": 0
        }
        # Biến theo dõi lần kiểm tra cuối cùng
        self.last_verification_time = 0
        # Thêm thuộc tính để tracking successful patterns
        self.successful_patterns = {}
    
    def _check_execution_context(self, driver: webdriver.Chrome) -> bool:
        """
        Kiểm tra execution context của driver trước khi thực hiện các thao tác.
        
        Args:
            driver: WebDriver instance cần kiểm tra
            
        Returns:
            bool: True nếu execution context còn tồn tại, False nếu đã mất.
        """
        if not driver:
            return False
            
        try:
            # Thực hiện script đơn giản để kiểm tra execution context
            result = driver.execute_script("return window.navigator.userAgent")
            if not result:
                self.logger.debug("Execution context không ổn định (userAgent trống)")
                return False
            return True
        except Exception as e:
            error_msg = str(e).lower()
            
            if "no such execution context" in error_msg:
                self.logger.warning(f"Phát hiện lỗi 'no such execution context'")
                self.error_counts["execution_context"] += 1
                return False
            elif "no such window" in error_msg:
                self.logger.warning(f"Phát hiện lỗi 'no such window'")
                self.error_counts["no_such_window"] += 1
                return False
            else:
                self.logger.debug(f"Lỗi khác khi kiểm tra execution context: {str(e)}")
                self.error_counts["other_errors"] += 1
                return False

    def _safe_execute_script(self, driver: webdriver.Chrome, script: str, default_value=None):
        """
        Thực thi JavaScript một cách an toàn, xử lý các lỗi execution context.
        
        Args:
            driver: WebDriver instance
            script: JavaScript cần thực thi
            default_value: Giá trị trả về mặc định nếu có lỗi
            
        Returns:
            Kết quả script hoặc default_value nếu có lỗi
        """
        try:
            return driver.execute_script(script)
        except Exception as e:
            error_msg = str(e).lower()
            if "no such execution context" in error_msg:
                self.error_counts["execution_context"] += 1
                self.logger.warning("Lỗi execution context khi thực thi script")
            elif "no such window" in error_msg:
                self.error_counts["no_such_window"] += 1
                self.logger.warning("Lỗi no such window khi thực thi script")
            else:
                self.logger.debug(f"Lỗi khi thực thi script: {str(e)}")
            return default_value

    @retry_with_backoff(max_retries=3, initial_delay=2, backoff_factor=2)
    def verify_login_status(self, driver: webdriver.Chrome) -> Tuple[bool, dict]:
        """
        Kiểm tra xem người dùng đã đăng nhập thành công hay chưa.
        Cải tiến để xử lý lỗi 'No Such Execution Context'.
        
        Args:
            driver: WebDriver instance để kiểm tra trạng thái đăng nhập
            
        Returns:
            Tuple[bool, dict]: (đăng nhập thành công, chi tiết kiểm tra)
        """
        details = {
            "login_status": False,
            "login_method": None,
            "current_url": None,
            "error": None,
            "page_element_found": None,
            "execution_context_verified": False
        }
        error_url = "https://portal.azure.com/error/"
        extra_retry_done = False
        
        # Cập nhật thời gian kiểm tra
        self.last_verification_time = time.time()

        try:
            # STEP 1: Kiểm tra execution context ngay từ đầu
            if not self._check_execution_context(driver):
                self.logger.warning("Execution context không tồn tại trước khi bắt đầu kiểm tra đăng nhập")
                details["login_method"] = "execution_context_error"
                details["error"] = "Execution context không tồn tại"
                raise WebDriverException("No such execution context at start of verification")
            
            details["execution_context_verified"] = True
            
            # STEP 2: Kiểm tra driver còn kết nối không
            if not self._verify_driver_connection(driver):
                self.logger.warning("Driver không còn kết nối. Cần khởi tạo lại.")
                details["login_method"] = "driver_disconnected"
                details["error"] = "WebDriver không còn kết nối"
                return False, details
                
            # STEP 3: Đóng tất cả tab ngoại trừ tab hiện tại - với xử lý lỗi tốt hơn
            try:
                # Kiểm tra xem có window handles không 
                if not driver.window_handles or len(driver.window_handles) == 0:
                    self.logger.warning("Driver không có window handles")
                    details["login_method"] = "no_window_handles"
                    details["error"] = "WebDriver không có cửa sổ nào"
                    return False, details
                    
                # Nếu có nhiều tab, cố gắng đóng các tab dư thừa
                if len(driver.window_handles) > 1:
                    try:
                        self._close_all_tabs(driver)
                    except WebDriverException as e:
                        self.logger.warning(f"Không thể đóng tất cả tab: {str(e)}")
                        # Thử chuyển đến tab đầu tiên nếu không đóng được
                        try:
                            driver.switch_to.window(driver.window_handles[0])
                        except Exception as tab_e:
                            self.logger.warning(f"Không thể chuyển đến tab đầu tiên: {str(tab_e)}")
            except WebDriverException as e:
                error_msg = str(e).lower()
                if "no such window" in error_msg or "no such execution context" in error_msg:
                    self.logger.warning(f"Lỗi execution context khi kiểm tra tabs: {str(e)}")
                    details["login_method"] = "execution_context_error"
                    details["error"] = f"Lỗi execution context: {str(e)}"
                    raise WebDriverException(f"Window/context error: {str(e)}")
                else:
                    self.logger.warning(f"Lỗi WebDriver khi xử lý tabs: {str(e)}")
                    # Tiếp tục xử lý dù không đóng được tabs

            # STEP 4: Kiểm tra URL hiện tại trước khi điều hướng
            # Nếu đã ở Azure Portal và URL thỏa mãn pattern, có thể không cần chuyển hướng
            try:
                # Kiểm tra lại execution context trước khi lấy URL
                if not self._check_execution_context(driver):
                    self.logger.warning("Execution context không tồn tại trước khi lấy URL")
                    details["login_method"] = "execution_context_error"
                    details["error"] = "Execution context không tồn tại khi lấy URL"
                    raise WebDriverException("No such execution context when getting URL")
                
                current_url = driver.current_url.lower()
                details["current_url"] = current_url
                self.logger.debug(f"URL hiện tại trước khi điều hướng: {current_url}")
                
                # Nếu URL hiện tại đã khớp với pattern đăng nhập, không cần điều hướng
                already_logged_in = False
                for pattern in self.LOGGED_IN_URL_PATTERNS + self.ADDITIONAL_LOGGED_IN_PATTERNS:
                    if pattern in current_url:
                        already_logged_in = True
                        self.logger.info(f"Đã ở trang đăng nhập Azure Portal: {current_url}")
                        details["login_status"] = True
                        details["login_method"] = "url_pattern_already"
                        # Ghi nhận pattern thành công
                        if pattern not in self.successful_patterns:
                            self.successful_patterns[pattern] = 0
                        self.successful_patterns[pattern] += 1
                        
                        # Xác nhận thêm bằng cách kiểm tra DOM nếu đã đăng nhập
                        self._verify_portal_dom_elements(driver, details)
                        
                        return True, details
            except WebDriverException as e:
                error_msg = str(e).lower()
                if "no such window" in error_msg or "no such execution context" in error_msg:
                    self.logger.warning(f"Lỗi execution context khi kiểm tra URL hiện tại: {str(e)}")
                    details["login_method"] = "execution_context_error"
                    details["error"] = f"Lỗi execution context: {str(e)}"
                    raise WebDriverException(f"Window/context error: {str(e)}")
                else:
                    self.logger.warning(f"Lỗi WebDriver khi kiểm tra URL hiện tại: {str(e)}")
                    # Tiếp tục xử lý dù không lấy được URL

            # STEP 5: Truy cập URL kiểm tra với xử lý lỗi execution context
            check_url = self.config.login_check_url
            try:
                # Kiểm tra lại execution context và shutdown request trước khi điều hướng
                if is_shutdown_requested():
                    raise RuntimeError("Shutdown requested - dừng driver.get")
                
                if not self._check_execution_context(driver):
                    self.logger.warning("Execution context không tồn tại trước khi điều hướng")
                    details["login_method"] = "execution_context_error"
                    details["error"] = "Execution context không tồn tại trước driver.get"
                    raise WebDriverException("No such execution context before navigation")
                    
                self.logger.info(f"verify_login_status: Truy cập URL = {check_url}")
                
                # Đặt page load timeout trước khi điều hướng
                original_timeout = driver.timeouts.page_load
                page_load_timeout = self.config.get_timeout("login_check", 15)
                try:
                    driver.set_page_load_timeout(page_load_timeout)
                except Exception as e:
                    self.logger.warning(f"Không thể đặt page load timeout: {str(e)}")
                    
                # Thực hiện điều hướng với script thay vì driver.get() để tránh block
                # synchronous và cho phép kiểm tra lỗi execution context
                navigation_script = f"""
                try {{
                    window.location.href = "{check_url}";
                    return true;
                }} catch (e) {{
                    return "Error: " + e.message;
                }}
                """
                nav_result = self._safe_execute_script(driver, navigation_script)
                
                if isinstance(nav_result, str) and nav_result.startswith("Error"):
                    self.logger.warning(f"Lỗi khi điều hướng bằng script: {nav_result}")
                    # Thử phương pháp thông thường nếu script không hoạt động
                    driver.get(check_url)
                    
                # Khôi phục timeout ban đầu
                try:
                    driver.set_page_load_timeout(original_timeout)
                except Exception:
                    pass
                    
            except TimeoutException as e:
                self.logger.warning(f"Timeout khi điều hướng đến {check_url}: {str(e)}")
                self.error_counts["page_load_timeout"] += 1
                details["login_method"] = "navigation_timeout"
                details["error"] = f"Timeout khi điều hướng: {str(e)}"
                # Không return False ở đây, thử tiếp tục
                
            except WebDriverException as e:
                error_msg = str(e).lower()
                if "no such execution context" in error_msg:
                    self.logger.warning(f"Lỗi execution context khi điều hướng đến {check_url}: {str(e)}")
                    self.error_counts["execution_context"] += 1
                    details["login_method"] = "execution_context_error"
                    details["error"] = f"Lỗi execution context: {str(e)}"
                    raise WebDriverException(f"No such execution context: {str(e)}")
                elif "no such window" in error_msg:
                    self.logger.warning(f"Lỗi no such window khi điều hướng đến {check_url}: {str(e)}")
                    self.error_counts["no_such_window"] += 1
                    details["login_method"] = "no_such_window"
                    details["error"] = f"Lỗi no such window: {str(e)}"
                    raise WebDriverException(f"No such window: {str(e)}")
                else:
                    self.logger.warning(f"Lỗi khi điều hướng đến {check_url}: {str(e)}")
                    self.error_counts["navigation"] += 1
                    details["login_method"] = "navigation_error"
                    details["error"] = f"Lỗi điều hướng: {str(e)}"
                    return False, details
                
            self.logger.info("verify_login_status: Đang chờ trang load...")

            # STEP 6: Đợi trang tải với kiểm tra execution context trong quá trình
            try:
                # Thử đợi lần đầu (đợi ngắn hơn so với trước đây)
                page_load_timeout = self.config.get_timeout("login_check", 10)
                page_load_result = self._wait_for_page_elements(driver, page_load_timeout, error_url)
                
                if page_load_result == "execution_context_error":
                    self.logger.warning("Phát hiện lỗi execution context khi đợi trang tải")
                    details["login_method"] = "execution_context_error"
                    details["error"] = "Execution context bị mất khi đợi trang tải"
                    raise WebDriverException("No such execution context during page load wait")
                
                elif page_load_result == "error_url_detected":
                    # Phát hiện error_url trong _wait_for_page_elements
                    if not extra_retry_done:
                        self.logger.warning(f"Phát hiện URL lỗi {error_url}, thực hiện retry bổ sung")
                        extra_retry_done = True
                        time.sleep(2)
                        return self.verify_login_status(driver)
                    else:
                        self.logger.warning("Đã retry nhưng vẫn gặp error_url")
                        details["login_method"] = "error_url_persistent"
                        details["error"] = f"URL lỗi {error_url} vẫn xuất hiện sau khi retry"
                        return False, details
                
                elif page_load_result == "no_elements_found":
                    # Không tìm thấy phần tử h1, h2 - retry nếu chưa thử
                    if not extra_retry_done:
                        self.logger.warning("Không phát hiện phần tử trang, thực hiện retry bổ sung")
                        extra_retry_done = True
                        time.sleep(2)
                        return self.verify_login_status(driver)
                    else:
                        self.logger.warning("Đã retry nhưng vẫn không tìm thấy phần tử trang")
                        details["login_method"] = "elements_not_found"
                        details["error"] = "Không tìm thấy phần tử trang sau khi retry"
                        return False, details
                
                elif not page_load_result:
                    # Timeout khi đợi trang tải - không fail ngay, tiếp tục kiểm tra URL
                    self.logger.warning(f"Timeout {page_load_timeout}s khi chờ trang load")
                    details["login_method"] = "page_load_timeout"
                    details["error"] = "Timeout khi chờ trang tải"
                    # Không return ở đây, tiếp tục kiểm tra URL
                
            except WebDriverException as e:
                error_msg = str(e).lower()
                if "no such execution context" in error_msg:
                    self.logger.warning(f"Lỗi execution context khi đợi trang tải: {str(e)}")
                    self.error_counts["execution_context"] += 1
                    details["login_method"] = "execution_context_error"
                    details["error"] = f"Lỗi execution context: {str(e)}"
                    raise WebDriverException(f"No such execution context: {str(e)}")
                else:
                    self.logger.warning(f"Lỗi WebDriver khi đợi trang tải: {str(e)}")
                    details["login_method"] = "page_load_error"
                    details["error"] = f"Lỗi khi đợi trang tải: {str(e)}"
                    return False, details
                    
            # STEP 7: Kiểm tra URL một lần nữa để xác định đăng nhập
            try:
                # Kiểm tra lại execution context 
                if not self._check_execution_context(driver):
                    self.logger.warning("Execution context không tồn tại khi kiểm tra URL sau tải trang")
                    details["login_method"] = "execution_context_error"
                    details["error"] = "Execution context không tồn tại khi kiểm tra URL"
                    raise WebDriverException("No such execution context when checking URL")
                
                current_url = driver.current_url.lower()
                details["current_url"] = current_url
                self.logger.info(f"verify_login_status: current_url = {current_url}")
                
                # Kiểm tra error_url một lần nữa
                if error_url in current_url:
                    if not extra_retry_done:
                        self.logger.warning(f"Phát hiện URL lỗi {error_url}, thực hiện retry bổ sung")
                        extra_retry_done = True
                        time.sleep(2)
                        return self.verify_login_status(driver)
                    else:
                        self.logger.warning("Đã retry nhưng vẫn gặp error_url")
                        details["login_method"] = "error_url_persistent"
                        details["error"] = f"URL lỗi {error_url} vẫn xuất hiện sau khi retry"
                        return False, details
                        
                # Thêm các loại URL lỗi khác
                error_patterns = ["error.aspx", "login.microsoftonline.com/error", "portal.azure.com/error"]
                for error_pattern in error_patterns:
                    if error_pattern in current_url:
                        self.logger.warning(f"Phát hiện URL lỗi khác: {current_url}")
                        details["login_method"] = "other_error_url"
                        details["error"] = f"URL lỗi: {current_url}"
                        return False, details
                        
                # Kiểm tra xem URL có phải trang đăng nhập không
                login_patterns = ["login.microsoftonline.com", "login.live.com", "login.windows.net"]
                for login_pattern in login_patterns:
                    if login_pattern in current_url:
                        self.logger.info(f"Người dùng chưa đăng nhập, đang ở trang login: {current_url}")
                        details["login_method"] = "login_page"
                        details["error"] = "Người dùng chưa đăng nhập (cookie không hợp lệ)"
                        return False, details
                        
                # CHỈ kiểm tra URL hiện tại theo LOGGED_IN_URL_PATTERNS để xác định đăng nhập
                # Mở rộng pattern kiểm tra
                all_patterns = self.LOGGED_IN_URL_PATTERNS + self.ADDITIONAL_LOGGED_IN_PATTERNS
                for pattern in all_patterns:
                    if pattern in current_url:
                        details["login_status"] = True
                        details["login_method"] = "url_pattern"
                        self.logger.info(f"Kiểm tra đăng nhập thành công: URL chứa mẫu '{pattern}'")
                        
                        # Ghi nhận pattern thành công
                        if pattern not in self.successful_patterns:
                            self.successful_patterns[pattern] = 0
                        self.successful_patterns[pattern] += 1
                        
                        # Xác nhận thêm bằng cách kiểm tra DOM (không bắt buộc)
                        self._verify_portal_dom_elements(driver, details)
                        
                        return True, details
                        
                # Nếu không khớp với bất kỳ mẫu nào, kiểm tra thêm bằng DOM
                # Thử xác định dựa trên DOM nếu URL không khớp pattern
                dom_verified = self._verify_portal_dom_elements(driver, details)
                if dom_verified:
                    self.logger.info(f"Kiểm tra đăng nhập thành công qua DOM mặc dù URL không khớp pattern")
                    details["login_status"] = True
                    details["login_method"] = "dom_verified"
                    return True, details
                
                # Nếu vẫn không khớp, coi là chưa đăng nhập
                self.logger.info(f"Kiểm tra đăng nhập thất bại: URL không chứa mẫu đăng nhập")
                details["login_status"] = False
                details["login_method"] = "url_mismatch"
                return False, details
                    
            except WebDriverException as e:
                error_msg = str(e).lower()
                if "no such execution context" in error_msg:
                    self.logger.warning(f"Lỗi execution context khi kiểm tra URL cuối cùng: {str(e)}")
                    self.error_counts["execution_context"] += 1
                    details["login_method"] = "execution_context_error"
                    details["error"] = f"Lỗi execution context: {str(e)}"
                    raise WebDriverException(f"No such execution context: {str(e)}")
                else:
                    self.logger.warning(f"Lỗi khi kiểm tra URL: {str(e)}")
                    details["login_method"] = "url_check_error"
                    details["error"] = str(e)
                    return False, details
                
        except InvalidSessionIdException as e:
            self.logger.warning(f"Phiên WebDriver không hợp lệ: {str(e)}")
            details["login_method"] = "invalid_session"
            details["error"] = str(e)
            return False, details

        except WebDriverException as e:
            error_msg = str(e).lower()
            if "no such execution context" in error_msg:
                self.error_counts["execution_context"] += 1
                self.logger.warning(f"Lỗi execution context khi xác thực đăng nhập: {str(e)}")
                details["login_method"] = "execution_context_error"
                details["error"] = f"Lỗi execution context: {str(e)}"
            elif "no such window" in error_msg:
                self.error_counts["no_such_window"] += 1
                self.logger.warning(f"Lỗi no such window khi xác thực đăng nhập: {str(e)}")
                details["login_method"] = "no_such_window"
                details["error"] = f"Lỗi no such window: {str(e)}"
            else:
                self.logger.warning(f"Lỗi WebDriver khi xác thực đăng nhập: {str(e)}")
                details["login_method"] = "webdriver_error"
                details["error"] = str(e)
            
            # Ghi lại lỗi và trả về False
            self.error_counts["other_errors"] += 1
            return False, details

        except Exception as e:
            self.logger.error(f"Lỗi không mong đợi: {type(e).__name__}: {str(e)}")
            details["login_method"] = "unexpected_error"
            details["error"] = str(e)
            self.error_counts["other_errors"] += 1
            return False, details

    def _verify_portal_dom_elements(self, driver, details=None):
        """
        Kiểm tra các phần tử DOM đặc trưng của Azure Portal khi đã đăng nhập.
        
        Args:
            driver: WebDriver instance
            details: Dictionary để cập nhật chi tiết (optional)
            
        Returns:
            bool: True nếu xác định đã đăng nhập qua DOM
        """
        try:
            # Kiểm tra execution context
            if not self._check_execution_context(driver):
                return False
                
            # Danh sách selector quan trọng của Azure Portal khi đã đăng nhập
            portal_selectors = [
                {"type": By.CSS_SELECTOR, "value": "div.fxs-avatarmenu", "name": "user_avatar_menu"},
                {"type": By.CSS_SELECTOR, "value": "nav.fxs-sidebar", "name": "sidebar_nav"},
                {"type": By.CSS_SELECTOR, "value": "div.fxs-topbar", "name": "top_bar"}
            ]
            
            # Tìm kiếm các phần tử với timeout ngắn
            success_elements = []
            for selector in portal_selectors:
                try:
                    element = driver.find_element(selector["type"], selector["value"])
                    if element.is_displayed():
                        success_elements.append(selector["name"])
                except Exception:
                    continue
                    
            # Cập nhật chi tiết nếu có
            if details is not None:
                details["portal_elements_found"] = success_elements
                
            # Xác định đăng nhập nếu tìm thấy ít nhất 1 phần tử
            return len(success_elements) >= 1
            
        except Exception as e:
            self.logger.debug(f"Lỗi khi kiểm tra DOM Azure Portal: {str(e)}")
            return False

    def _wait_for_page_elements(self, driver, timeout=10, error_url=None):
        """
        Đợi các phần tử quan trọng xuất hiện trên trang.
        Cải tiến: Kiểm tra execution context trước và trong khi đợi.
        
        Args:
            driver: WebDriver instance
            timeout: Thời gian tối đa chờ đợi (giây)
            error_url: URL lỗi cần kiểm tra (nếu có)
            
        Returns:
            str/bool: Kết quả kiểm tra (execution_context_error, error_url_detected, 
                     no_elements_found, True nếu thành công, False nếu thất bại)
        """
        try:
            # Kiểm tra execution context trước khi bắt đầu
            if not self._check_execution_context(driver):
                self.logger.warning("Mất execution context trước khi bắt đầu đợi phần tử")
                return "execution_context_error"
            
            # Kiểm tra URL lỗi nếu có
            if error_url and error_url in driver.current_url.lower():
                self.logger.warning(f"Phát hiện URL lỗi: {error_url}")
                return "error_url_detected"
            
            self.logger.info(f"_wait_for_page_elements: Đang đợi phần tử tiêu đề xuất hiện (timeout={timeout}s)")
            
            # Triển khai cơ chế đợi an toàn với kiểm tra context định kỳ
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    # Kiểm tra execution context định kỳ
                    if not self._check_execution_context(driver):
                        self.logger.warning("Mất execution context trong quá trình đợi phần tử")
                        return "execution_context_error"
                    
                    # Kiểm tra URL lỗi định kỳ
                    if error_url and error_url in driver.current_url.lower():
                        self.logger.warning(f"Phát hiện URL lỗi trong quá trình đợi: {error_url}")
                        return "error_url_detected"
                    
                    # Thử tìm một phần tử bất kỳ từ các selector
                    for selector in self.AZURE_PORTAL_SELECTORS:
                        try:
                            # Giảm timeout cho mỗi lần thử để không bị treo
                            element = WebDriverWait(driver, 2).until(
                                EC.presence_of_element_located((selector["type"], selector["value"]))
                            )
                            if element and element.is_displayed():
                                return True  # Tìm thấy phần tử, trả về thành công
                        except (TimeoutException, WebDriverException, StaleElementReferenceException):
                            # Lỗi nhỏ khi tìm phần tử, tiếp tục với selector khác
                            continue
                    
                    # Ngủ ngắn trước khi thử lại
                    time.sleep(0.5)
                except Exception as e:
                    error_msg = str(e).lower()
                    if "no such execution context" in error_msg:
                        self.logger.warning(f"Phát hiện lỗi execution context khi đợi phần tử: {str(e)}")
                        return "execution_context_error"
                    elif "no such window" in error_msg:
                        self.logger.warning(f"Phát hiện lỗi no such window khi đợi phần tử: {str(e)}")
                        return False
                    else:
                        self.logger.warning(f"Lỗi khi đợi phần tử: {type(e).__name__}: {str(e)}")
            
            # Hết thời gian timeout
            self.logger.warning("Hết thời gian đợi các phần tử trang xuất hiện")
            return "no_elements_found"
            
        except Exception as e:
            error_msg = str(e).lower()
            if "no such execution context" in error_msg:
                self.logger.warning(f"Phát hiện lỗi execution context khi đợi phần tử: {str(e)}")
                return "execution_context_error"
            elif "no such window" in error_msg:
                self.logger.warning(f"Phát hiện lỗi no such window khi đợi phần tử: {str(e)}")
                return False
            else:
                self.logger.error(f"Lỗi không xác định khi đợi phần tử trang: {str(e)}")
                return False

    def _verify_driver_connection(self, driver) -> bool:
        """
        Xác minh WebDriver còn kết nối và hoạt động.
        
        Args:
            driver: WebDriver instance cần kiểm tra
            
        Returns:
            bool: True nếu driver còn kết nối, False nếu không
        """
        if not driver:
            return False
            
        try:
            # Kiểm tra execution context
            if not self._check_execution_context(driver):
                return False
                
            # Kiểm tra session_id
            if not hasattr(driver, 'session_id') or not driver.session_id:
                return False
                
            # Kiểm tra window_handles
            try:
                handles = driver.window_handles
                if not handles:
                    return False
            except Exception:
                return False
                
            return True
        except Exception as e:
            self.logger.debug(f"Lỗi khi xác minh kết nối driver: {str(e)}")
            return False

    def _close_all_tabs(self, driver) -> bool:
        """
        Đóng tất cả tab ngoại trừ tab đầu tiên.
        
        Args:
            driver: WebDriver instance
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            handles = driver.window_handles
            
            # Nếu không có cửa sổ nào
            if not handles:
                return False
                
            # Nếu chỉ có một cửa sổ, không cần đóng
            if len(handles) <= 1:
                return True
                
            # Chuyển về tab đầu tiên
            main_window = handles[0]
            driver.switch_to.window(main_window)
            
            # Đóng các tab khác
            for handle in handles[1:]:
                try:
                    driver.switch_to.window(handle)
                    driver.close()
                except Exception as e:
                    self.logger.debug(f"Không thể đóng tab {handle}: {str(e)}")
            
            # Chuyển lại về tab đầu tiên
            driver.switch_to.window(main_window)
            return True
        except Exception as e:
            self.logger.warning(f"Lỗi khi đóng tất cả tab: {str(e)}")
            return False

    def _adaptive_sleep(self, duration, driver) -> bool:
        """
        Sleep thích ứng với kiểm tra execution context định kỳ.
        
        Args:
            duration: Thời gian sleep (giây)
            driver: WebDriver instance để kiểm tra context
            
        Returns:
            bool: True nếu sleep đủ thời gian, False nếu bị ngắt do mất context
        """
        start_time = time.time()
        check_interval = 0.5  # Kiểm tra context mỗi 0.5 giây
        
        while time.time() - start_time < duration:
            # Ngắt sleep nếu phát hiện shutdown request
            if is_shutdown_requested():
                self.logger.debug("Phát hiện shutdown request trong adaptive_sleep")
                return False
                
            time.sleep(min(check_interval, duration - (time.time() - start_time)))
            
            # Kiểm tra execution context
            if not self._check_execution_context(driver):
                self.logger.debug("Mất execution context trong adaptive_sleep")
                return False
                
        return True

    def _determine_optimal_wait_time(self, driver, current_url) -> float:
        """
        Xác định thời gian chờ tối ưu dựa trên tình trạng trang.
        
        Args:
            driver: WebDriver instance
            current_url: URL hiện tại
            
        Returns:
            float: Thời gian chờ tối ưu (giây)
        """
        # Mặc định là 3 giây nếu đã ở Portal
        wait_time = 3.0
        
        try:
            # Nếu chưa ở Portal, cần thời gian dài hơn
            if "portal.azure.com" not in current_url.lower():
                wait_time = 4.0
                
            # Kiểm tra DOM và resource đã load chưa
            if self._check_execution_context(driver):
                dom_ready = self._safe_execute_script(driver, "return document.readyState === 'complete'", False)
                resources_loaded = self._safe_execute_script(
                    driver, 
                    "return window.performance && performance.timing && (performance.timing.loadEventEnd > 0)", 
                    False
                )
                
                # Điều chỉnh thời gian dựa trên trạng thái trang
                if dom_ready and resources_loaded:
                    # Trang đã load hoàn toàn - giảm thời gian chờ
                    wait_time *= 0.7
                elif not dom_ready:
                    # DOM chưa sẵn sàng - tăng thời gian chờ
                    wait_time *= 1.3
                    
                # Kiểm tra nếu localStorage và sessionStorage đã khởi tạo
                storage_ready = self._safe_execute_script(
                    driver,
                    "return window.localStorage && window.sessionStorage", 
                    False
                )
                
                if storage_ready:
                    # Storage đã sẵn sàng - giảm thời gian chờ
                    wait_time *= 0.8
                    
            # Giới hạn thời gian chờ trong khoảng hợp lý
            wait_time = min(max(1.0, wait_time), 5.0)
            
        except Exception as e:
            self.logger.debug(f"Lỗi khi xác định thời gian chờ: {str(e)}")
            
        return wait_time

    def _wait_for_page_load_safely(self, driver, timeout=15):
        """
        Đợi trang load với kiểm tra execution context liên tục.
        
        Args:
            driver: WebDriver instance
            timeout: Thời gian tối đa chờ đợi (giây)
            
        Returns:
            bool: True nếu trang đã load, False nếu timeout hoặc lỗi
        """
        self.logger.debug(f"Đợi trang load, timeout={timeout}s")
        
        # Không sử dụng wait_for_page_load trực tiếp để tránh mất context
        try:
            # Đợi document readyState
            ready_state_condition = lambda d: d.execute_script("return document.readyState") in ["complete", "interactive"]
            
            if self._safe_wait_for_condition(driver, ready_state_condition, timeout):
                self.logger.debug("document.readyState báo trang đã load")
                
                # Kiểm tra thêm các phần tử quan trọng của trang
                try:
                    for selector in ["body", "header", "nav", "main", "div", "script", "html"]:
                        element_present = lambda d: len(d.find_elements(By.TAG_NAME, selector)) > 0
                        if self._safe_wait_for_condition(driver, element_present, 2):
                            self.logger.debug(f"Phát hiện phần tử {selector} trên trang")
                            return True
                except Exception:
                    # Nếu không tìm thấy phần tử nào cũng không sao
                    pass
                    
                # Trang đã load theo readyState
                return True
            
            self.logger.debug("Timeout khi đợi trang load")
            return False
            
        except Exception as e:
            self.logger.warning(f"Lỗi khi đợi trang load: {str(e)}")
            return False

    def _safe_wait_for_condition(self, driver, condition, timeout=10):
        """
        Đợi một điều kiện với kiểm tra execution context liên tục.
        
        Args:
            driver: WebDriver instance
            condition: Hàm điều kiện, nhận driver và trả về Boolean
            timeout: Thời gian tối đa chờ đợi (giây)
            
        Returns:
            bool: True nếu điều kiện thỏa mãn, False nếu timeout
        """
        start_time = time.time()
        check_interval = 0.5
        
        while time.time() - start_time < timeout:
            # Kiểm tra execution context trước khi kiểm tra điều kiện
            if not self._check_execution_context(driver):
                self.logger.debug("Mất execution context khi đợi điều kiện")
                return False
                
            try:
                if condition(driver):
                    return True
            except Exception as e:
                self.logger.debug(f"Lỗi khi kiểm tra điều kiện: {str(e)}")
                
            time.sleep(check_interval)
            
        return False

    def get_error_metrics(self) -> Dict:
        """
        Lấy thông tin về các lỗi đã xảy ra trong quá trình xác thực.
        
        Returns:
            Dict: Thông tin chi tiết về các lỗi
        """
        return {
            "execution_context_errors": self.error_counts["execution_context"],
            "navigation_errors": self.error_counts["navigation"],
            "page_load_timeout": self.error_counts["page_load_timeout"],
            "no_such_window": self.error_counts["no_such_window"],
            "other_errors": self.error_counts["other_errors"],
            "last_verification_time": self.last_verification_time,
            "successful_patterns": self.successful_patterns
        }

    def reset_error_metrics(self):
        """
        Đặt lại các bộ đếm lỗi.
        """
        self.error_counts = {
            "execution_context": 0,
            "navigation": 0,
            "page_load_timeout": 0,
            "no_such_window": 0,
            "other_errors": 0
        }
        
    def is_healthy(self) -> bool:
        """
        Kiểm tra xem LoginVerifier có đang trong trạng thái hoạt động tốt không.
        
        Returns:
            bool: True nếu ít lỗi, False nếu quá nhiều lỗi.
        """
        # Nếu có quá nhiều lỗi execution_context, coi như không healthy
        if self.error_counts["execution_context"] > 10:
            return False
            
        # Nếu tổng số lỗi quá nhiều, coi như không healthy
        total_errors = sum(self.error_counts.values())
        if total_errors > 20:
            return False
            
        return True

