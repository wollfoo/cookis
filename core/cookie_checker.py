"""
cookie_checker.py - Module kiểm tra tính hợp lệ của cookie Microsoft/Azure

Module này cung cấp các lớp và hàm để kiểm tra tính hợp lệ của cookie Microsoft/Azure,
sử dụng Selenium WebDriver kết hợp với GenLogin. Quá trình kiểm tra bao gồm:
- Xác thực đăng nhập thành công vào Azure Portal
- Xác thực thông tin người dùng qua API và localStorage
- Tạo báo cáo chi tiết về tính hợp lệ của cookie

Tính năng chính:
- Sử dụng WebDriver Pool để tái sử dụng driver, tăng hiệu suất
- Adaptive batch processing để tối ưu hiệu suất
- Cơ chế retry thông minh và xử lý lỗi mạnh mẽ
- Xử lý lỗi "No Such Execution Context" hiệu quả

Thông tin cookie chỉ được coi là hợp lệ khi cả hai điều kiện đều đạt:
- Đăng nhập thành công (login_success=True)
- Xác thực API thành công (api_success=True)
Giá trị final_validation = login_success AND api_success là kết quả cuối cùng.
"""

import time
import logging
import threading
import traceback
import uuid
import re
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple, Generator, Callable
from contextlib import contextmanager

from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, InvalidSessionIdException,
    NoSuchElementException, StaleElementReferenceException
)

# Các module cần import từ dự án
from global_state import request_shutdown, is_shutdown_requested
from web_driver_utils.driver_maintenance import clear_browsing_data
from web_driver_utils.driver_lifecycle import verify_driver_connection
from cookie_utils import add_cookies_for_domain, parse_cookie_file, group_cookies_by_domain
from core.webdriver_pool import WebDriverPool
from core.login_verifier import LoginVerifier
from core.api_verifier import ApiVerifier
from core.cookie_processor import CookieProcessor
from core.task_scheduler import AdaptiveTaskScheduler
from core.result_manager import ResultManager

# Custom Exceptions
class CookieCheckerError(Exception):
    """Ngoại lệ chung cho CookieChecker."""
    pass

class LoginVerificationError(CookieCheckerError):
    """Ngoại lệ khi kiểm tra đăng nhập thất bại."""
    pass

class ResourceUnavailableError(CookieCheckerError):
    """Ngoại lệ khi tài nguyên không khả dụng."""
    pass

class WebDriverPoolError(CookieCheckerError):
    """Ngoại lệ liên quan đến WebDriver Pool."""
    pass

class ApiVerificationError(CookieCheckerError):
    """Ngoại lẹ khi xác thực API thất bại."""
    pass

class GenLoginError(CookieCheckerError):
    """Ngoại lệ khi tương tác với GenLogin thất bại."""
    pass

class CookieChecker:
    """
    Lớp kiểm tra cookie Microsoft/Azure với hiệu suất được tối ưu.
    Các cải tiến:
    - Sử dụng WebDriver Pool để tái sử dụng driver
    - Adaptive batch processing để tối ưu hiệu suất
    - Kiểm tra đăng nhập đa phương pháp
    - Xác thực thông tin người dùng qua API GetEarlyUserData
    - Tách biệt logic kiểm tra thành các thành phần riêng biệt
    - Cơ chế retry thông minh và xử lý lỗi mạnh mẽ
    """

    def __init__(
        self,
        config: Any,
        driver_pool: WebDriverPool,
        result_manager: ResultManager,
        logger_instance: Optional[logging.Logger] = None,
        auto_save_results: bool = True,
        cookie_processor: Optional[CookieProcessor] = None,
        login_verifier: Optional[LoginVerifier] = None,
        api_verifier: Optional[ApiVerifier] = None
    ):
        """
        Khởi tạo CookieChecker
        
        Args:
            config: Đối tượng cấu hình chứa thông tin cài đặt
            driver_pool: WebDriverPool để quản lý và tái sử dụng driver
            result_manager: ResultManager để xử lý kết quả kiểm tra
            logger_instance: Logger để ghi log (tùy chọn)
            auto_save_results: Tự động lưu kết quả nếu True
            cookie_processor: CookieProcessor để xử lý cookie (tùy chọn)
            login_verifier: LoginVerifier để kiểm tra đăng nhập (tùy chọn)
            api_verifier: ApiVerifier để xác thực API (tùy chọn)
        """
        self.config = config
        self.driver_pool = driver_pool
        self.result_manager = result_manager
        self.logger = logger_instance if logger_instance else logging.getLogger(__name__)
        self.auto_save_results = auto_save_results
        self.cookie_processor = cookie_processor or CookieProcessor(
            domain_url_map=self.config.domain_url_map,
            logger=self.logger
        )
        self.login_verifier = login_verifier or LoginVerifier(
            config=self.config, 
            logger=self.logger
        )
        self.api_verifier = api_verifier or ApiVerifier(
            config=self.config,
            logger=self.logger
        )
        self.instance_id = str(uuid.uuid4())[:8]
        self.session_stats = {
            "start_time": datetime.now(),
            "end_time": None,
            "total_checked": 0,
            "valid_count": 0,
            "invalid_count": 0,
            "error_count": 0,
        }
        self._resource_lock = threading.RLock()
        self.logger.debug(f"Khởi tạo CookieChecker (id={self.instance_id})")
        self.driver = None  # Sẽ được thiết lập từ driver_session

    @contextmanager
    def driver_session(self):
        """
        Context manager để quản lý phiên driver, đảm bảo trả driver về pool
        sau khi sử dụng.
        
        Yields:
            webdriver.Chrome: WebDriver để thực hiện kiểm tra
        """
        driver = gen = profile_id = None
        try:
            driver, gen, profile_id = self.driver_pool.get_driver()
            self.logger.debug(f"Đã lấy driver cho profile {profile_id}")
            self.driver = driver  # Set the driver attribute for other methods to use
            yield driver
        finally:
            if driver and gen and profile_id:
                self.logger.debug(f"Trả driver về pool cho profile {profile_id}")
                self.driver_pool.return_driver(driver, gen, profile_id)
                self.driver = None  # Clear the driver attribute

    def check_cookie_file(self, cookie_file: str) -> Tuple[bool, Dict]:
        """
        Kiểm tra file cookie và trả về (is_valid, details).
        Cookie chỉ được coi là hợp lệ khi cả hai điều kiện đều đạt:
        - Đăng nhập thành công (login_success)
        - Xác thực API thành công (api_success)
        Giá trị final_validation = login_success AND api_success là kết quả cuối cùng.
        
        Args:
            cookie_file: Đường dẫn tới file cookie cần kiểm tra
            
        Returns:
            Tuple[bool, Dict]: (is_valid, chi tiết kết quả) với is_valid dựa trên final_validation
        """
        file_path = Path(cookie_file)
        self.logger.info(f"=== [CHECK] Đang kiểm tra cookie: {file_path.name} ===")
        result_details = {
            "filename": file_path.name,
            "full_path": str(file_path.resolve()),
            "timestamp": datetime.now().isoformat(),
            "cookies_parsed": 0,
            "cookies_by_domain": {},
            "domains_added": 0,
            "login_check_url": self.config.login_check_url,
            "login_status": None,
            "login_method": None,
            "error": None,
            "error_phase": None,
            "page_title": None,
            "has_mfa_elements": False,
            "has_logout_button": False,
            "api_check_performed": False,
            "api_check_status": False,
            "user_id": None,
            "display_name": None,
            "domain_name": None,
            "api_error": None,
            "final_validation": False,
            "performance_metrics": {}
        }
        
        # Ghi lại thời gian bắt đầu để đo hiệu suất
        start_time = time.time()
        
        # Bước 1: Kiểm tra nhanh và xử lý file cookie
        try:
            # Thêm kiểm tra tồn tại file
            if not file_path.exists():
                msg = f"File không tồn tại: {file_path.name}"
                self.logger.error(msg)
                result_details["error"] = msg
                result_details["error_phase"] = "file_check"
                return False, result_details
                    
            # Kiểm tra kích thước file
            if file_path.stat().st_size == 0:
                msg = f"File trống (0 byte): {file_path.name}"
                self.logger.error(msg)
                result_details["error"] = msg
                result_details["error_phase"] = "file_check"
                return False, result_details
            
            # Xử lý file cookie với cơ chế bắt lỗi tốt hơn
            try:
                cookies_by_domain = self.cookie_processor.process_file(str(file_path))
                total_cookies = sum(len(domain_cookies) for domain_cookies in cookies_by_domain.values())
                result_details["cookies_parsed"] = total_cookies
                
                for domain, domain_cookies in cookies_by_domain.items():
                    result_details["cookies_by_domain"][domain] = len(domain_cookies)
                    
                if not cookies_by_domain or total_cookies == 0:
                    msg = f"Không parse được cookie từ file {file_path.name}"
                    self.logger.warning(msg)
                    result_details["error"] = msg
                    result_details["error_phase"] = "cookie_parsing"
                    return False, result_details
                    
                # Kiểm tra xem file có chứa domain quan trọng không
                required_domains = set(self.config.domain_url_map.keys())
                found_domains = set(cookies_by_domain.keys())
                
                if len(found_domains.intersection(required_domains)) == 0:
                    msg = f"File không chứa cookie cho bất kỳ domain quan trọng nào: {', '.join(required_domains)}"
                    self.logger.warning(msg)
                    result_details["error"] = msg
                    result_details["error_phase"] = "cookie_validation"
                    return False, result_details
                    
            except Exception as e:
                error_msg = f"Lỗi khi xử lý file cookie: {type(e).__name__}: {str(e)}"
                self.logger.error(error_msg)
                result_details["error"] = error_msg
                result_details["error_phase"] = "cookie_parsing"
                return False, result_details

            # Bước 2: Tương tác WebDriver và kiểm tra đăng nhập
            driver = None
            gen = None
            profile_id = None
            driver_acquisition_time = time.time()
            
            try:
                # Lấy driver từ pool với cơ chế retry tăng cường
                max_driver_retries = 3  # Tăng số lần retry
                driver_retry_count = 0
                
                while driver_retry_count <= max_driver_retries:
                    try:
                        # Gọi get_driver với timeout hợp lý
                        driver, gen, profile_id = self.driver_pool.get_driver()
                        driver_acquisition_time = time.time() - driver_acquisition_time
                        result_details["performance_metrics"]["driver_acquisition"] = driver_acquisition_time
                        
                        # Kiểm tra execution context trước khi sử dụng
                        if not self._verify_execution_context(driver):
                            raise WebDriverException("Driver không có execution context hợp lệ")
                            
                        self.logger.info(f"Đã lấy driver với profile {profile_id}")
                        break
                        
                    except (WebDriverException, WebDriverPoolError) as e:
                        driver_retry_count += 1
                        error_msg = str(e).lower()
                        
                        # Xử lý thông minh hơn dựa trên loại lỗi
                        if "no such execution context" in error_msg:
                            self.logger.warning(f"Phát hiện lỗi execution context ({driver_retry_count}/{max_driver_retries})")
                        elif "no such window" in error_msg:
                            self.logger.warning(f"Phát hiện lỗi window đã đóng ({driver_retry_count}/{max_driver_retries})")
                        else:
                            self.logger.warning(f"Thử lại lấy driver ({driver_retry_count}/{max_driver_retries}): {str(e)}")
                        
                        # Trả về driver hiện tại nếu có
                        if driver and gen and profile_id:
                            try:
                                self.driver_pool.return_driver(driver, gen, profile_id)
                            except Exception:
                                pass
                            driver = gen = profile_id = None
                        
                        # Chờ trước khi thử lại với backoff tăng dần
                        backoff_time = 1 * (1.5 ** (driver_retry_count - 1))
                        time.sleep(backoff_time)
                        
                        # Nếu đã hết số lần thử, báo lỗi
                        if driver_retry_count > max_driver_retries:
                            error_msg = f"Lỗi lấy driver sau {max_driver_retries} lần thử: {str(e)}"
                            self.logger.error(error_msg)
                            result_details["error"] = error_msg
                            result_details["error_phase"] = "driver_acquisition"
                            return False, result_details
                
                # Context manager tự viết
                try:
                    # Bước 2.1: Xóa dữ liệu trình duyệt
                    clear_start_time = time.time()
                    try:
                        # Kiểm tra execution context trước khi thực hiện
                        if not self._verify_execution_context(driver):
                            raise WebDriverException("Driver không có execution context trước khi xóa dữ liệu trình duyệt")

                        # Xác định tham số cấu hình xóa dữ liệu
                        use_settings_page = getattr(self.config, 'clear_data_use_settings', True)
                        time_range = getattr(self.config, 'clear_data_time_range', 'all_time')
                        
                        self.logger.info(
                            f"Xóa dữ liệu trình duyệt: "
                            f"{'Phương pháp Settings' if use_settings_page else 'Phương pháp CDP'}, "
                            f"time_range={time_range}"
                        )
                        
                        # Xóa dữ liệu với timeout giảm để tránh treo
                        clear_timeout = min(15, self.config.get_timeout("page_load", 30) / 2)
                        clear_success = clear_browsing_data(
                            driver, 
                            self.config.domain_url_map, 
                            use_settings_page=use_settings_page,
                            time_range=time_range,
                            verify_connection=True,
                            timeout=clear_timeout
                        )
                        
                        result_details["performance_metrics"]["clear_data"] = time.time() - clear_start_time
                        
                        if not clear_success:
                            self.logger.warning("Có lỗi khi xóa dữ liệu trình duyệt. Tiếp tục xử lý...")
                            
                    except Exception as e:
                        error_msg = str(e).lower()
                        
                        # Phân loại lỗi cụ thể
                        if "no such execution context" in error_msg:
                            self.logger.warning("Lỗi execution context khi xóa dữ liệu trình duyệt")
                            # Thử phục hồi execution context
                            if not self._try_restore_execution_context(driver):
                                raise WebDriverException("Không thể phục hồi execution context")
                        elif "no such window" in error_msg:
                            self.logger.warning("Cửa sổ đã đóng khi xóa dữ liệu trình duyệt")
                            raise WebDriverException("Cửa sổ đã đóng")
                        else:
                            # Đối với lỗi khác, ghi log nhưng vẫn tiếp tục
                            self.logger.warning(f"Lỗi khi xóa dữ liệu trình duyệt: {type(e).__name__}: {str(e)}")

                    # Bước 2.2: Tiêm cookie với xử lý lỗi tốt hơn
                    inject_start_time = time.time()
                    try:
                        # Kiểm tra execution context trước khi tiêm cookie
                        if not self._verify_execution_context(driver):
                            raise WebDriverException("Driver không có execution context trước khi tiêm cookie")
                            
                        success_domains = self._add_cookies_adaptive(driver, cookies_by_domain)
                        result_details["domains_added"] = success_domains
                        result_details["performance_metrics"]["cookie_injection"] = time.time() - inject_start_time
                        
                        if success_domains == 0:
                            msg = "Không thêm được cookie cho bất kỳ domain nào"
                            self.logger.warning(msg)
                            result_details["error"] = msg
                            result_details["error_phase"] = "cookie_injection"
                            return False, result_details
                            
                    except Exception as e:
                        error_msg = str(e).lower()
                        
                        if "no such execution context" in error_msg:
                            error_log = "Lỗi execution context khi tiêm cookie"
                            self.logger.error(error_log)
                            result_details["error"] = error_log
                            result_details["error_phase"] = "cookie_injection"
                            return False, result_details
                        elif "no such window" in error_msg:
                            error_log = "Cửa sổ đã đóng khi tiêm cookie"
                            self.logger.error(error_log)
                            result_details["error"] = error_log
                            result_details["error_phase"] = "cookie_injection"
                            return False, result_details
                        else:
                            error_log = f"Lỗi khi tiêm cookie: {type(e).__name__}: {str(e)}"
                            self.logger.error(error_log)
                            result_details["error"] = error_log
                            result_details["error_phase"] = "cookie_injection"
                            return False, result_details

                    # Bước 2.3: Lấy page title (không quan trọng)
                    try:
                        if self._verify_execution_context(driver):
                            result_details["page_title"] = driver.title
                    except Exception:
                        pass

                    # Bước 2.4: Kiểm tra đăng nhập
                    login_start_time = time.time()
                    try:
                        # Kiểm tra execution context trước khi kiểm tra đăng nhập
                        if not self._verify_execution_context(driver):
                            raise WebDriverException("Driver không có execution context trước khi kiểm tra đăng nhập")
                            
                        # Đảm bảo timeout login check hợp lý để tránh treo
                        login_check_timeout = min(30, max(15, self.config.get_timeout("login_check", 20)))
                        
                        # Khi gọi verify_login_status, đảm bảo có timeout hợp lý
                        # Cần sửa mã trong LoginVerifier để chấp nhận tham số timeout
                        login_success, login_details = self.login_verifier.verify_login_status(driver)
                        result_details.update(login_details)
                        result_details["performance_metrics"]["login_verification"] = time.time() - login_start_time

                        if not login_success:
                            self.logger.info(f"Kiểm tra đăng nhập thất bại: {login_details.get('login_method', 'unknown')}")
                            return False, result_details
                            
                    except Exception as e:
                        error_msg = str(e).lower()
                        
                        if "no such execution context" in error_msg:
                            error_log = "Lỗi execution context khi kiểm tra đăng nhập"
                            self.logger.error(error_log)
                            result_details["error"] = error_log
                            result_details["error_phase"] = "login_verification"
                            return False, result_details
                        elif "no such window" in error_msg:
                            error_log = "Cửa sổ đã đóng khi kiểm tra đăng nhập"
                            self.logger.error(error_log)
                            result_details["error"] = error_log 
                            result_details["error_phase"] = "login_verification"
                            return False, result_details
                        else:
                            error_log = f"Lỗi khi kiểm tra đăng nhập: {type(e).__name__}: {str(e)}"
                            self.logger.error(error_log)
                            result_details["error"] = error_log
                            result_details["error_phase"] = "login_verification"
                            return False, result_details

                    # Bước 2.5: Xác thực API
                    api_start_time = time.time()
                    try:
                        # Kiểm tra execution context trước khi xác thực API
                        if not self._verify_execution_context(driver):
                            raise WebDriverException("Driver không có execution context trước khi xác thực API")
                            
                        self.logger.info("Đăng nhập thành công, tiến hành kiểm tra API")
                        result_details["api_check_performed"] = True
                        
                        # Khi gọi verify_user_data, đảm bảo có timeout hợp lý
                        api_success, api_details = self.api_verifier.verify_user_data(driver)
                        result_details.update(api_details)
                        result_details["performance_metrics"]["api_verification"] = time.time() - api_start_time
                        
                        # Mấu chốt: final_validation = login_success AND api_success
                        final_validation = login_success and api_success
                        result_details["final_validation"] = final_validation
                        
                        if api_success:
                            self.logger.info(f"Kiểm tra API thành công: ID={api_details.get('user_id', 'unknown')}, Name={api_details.get('display_name', 'unknown')}")
                        else:
                            self.logger.warning(f"Kiểm tra API thất bại: {api_details.get('api_error', 'Unknown error')}")
                        
                        # Ghi nhận tổng thời gian xử lý
                        result_details["performance_metrics"]["total_time"] = time.time() - start_time
                        
                        return final_validation, result_details
                        
                    except Exception as e:
                        error_msg = str(e).lower()
                        
                        if "no such execution context" in error_msg:
                            error_log = "Lỗi execution context khi xác thực API"
                            self.logger.error(error_log)
                            result_details["error"] = error_log
                            result_details["error_phase"] = "api_verification"
                            result_details["api_error"] = error_log
                            
                            # Logic xác minh - API thất bại = cookie không hợp lệ
                            result_details["final_validation"] = False
                            return False, result_details
                        elif "no such window" in error_msg:
                            error_log = "Cửa sổ đã đóng khi xác thực API"
                            self.logger.error(error_log)
                            result_details["error"] = error_log
                            result_details["error_phase"] = "api_verification"
                            result_details["api_error"] = error_log
                            
                            # Logic xác minh - API thất bại = cookie không hợp lệ
                            result_details["final_validation"] = False
                            return False, result_details
                        else:
                            error_log = f"Lỗi khi xác thực API: {type(e).__name__}: {str(e)}"
                            self.logger.error(error_log)
                            result_details["error"] = error_log
                            result_details["error_phase"] = "api_verification"
                            result_details["api_error"] = error_log
                            
                            # Logic xác minh - API thất bại = cookie không hợp lệ
                            result_details["final_validation"] = False
                            return False, result_details
                            
                finally:
                    # Đảm bảo trả driver về pool với thông tin về trạng thái health
                    if driver and gen and profile_id:
                        try:
                            # Kiểm tra driver health trước khi trả về pool
                            driver_healthy = self._verify_execution_context(driver)
                            self.driver_pool.return_driver(driver, gen, profile_id, 
                                                        priority=-10 if not driver_healthy else 0)
                            self.logger.debug(f"Đã trả driver với profile {profile_id} về pool" + 
                                            "" if driver_healthy else " (không khỏe)")
                        except Exception as e:
                            self.logger.warning(f"Lỗi khi trả driver về pool: {str(e)}")
                        
            except InvalidSessionIdException as e:
                error_msg = f"Phiên WebDriver không hợp lệ: {str(e)}"
                self.logger.error(error_msg)
                result_details["error"] = error_msg
                result_details["error_phase"] = "webdriver_session"
                
                # Đảm bảo trả driver về pool nếu có thể
                if driver and gen and profile_id:
                    try:
                        self.driver_pool.return_driver(driver, gen, profile_id, priority=-50)
                    except Exception:
                        pass
                        
                return False, result_details
                    
            except Exception as e:
                error_msg = f"Lỗi khi xử lý WebDriver session: {type(e).__name__}: {str(e)}"
                self.logger.error(error_msg)
                result_details["error"] = error_msg
                result_details["error_phase"] = "webdriver_session"
                
                # Đảm bảo trả driver về pool nếu có thể
                if driver and gen and profile_id:
                    try:
                        self.driver_pool.return_driver(driver, gen, profile_id, priority=-50)
                    except Exception:
                        pass
                        
                return False, result_details
                    
        except Exception as e:
            # Xử lý bất kỳ ngoại lệ nào ở cấp cao nhất
            error_msg = f"Lỗi chung khi kiểm tra cookie: {type(e).__name__}: {str(e)}"
            self.logger.error(error_msg)
            result_details["error"] = error_msg
            result_details["error_phase"] = "general_exception"
            result_details["final_validation"] = False
            return False, result_details

        # Nếu đến được đây, đây là lỗi logic
        self.logger.warning("Không rõ kết quả kiểm tra cookie - đây là một lỗi logic")
        result_details["error"] = "Không rõ kết quả - lỗi logic trong mã"
        result_details["error_phase"] = "logic_error"
        result_details["final_validation"] = False
        return False, result_details

    def _verify_execution_context(self, driver) -> bool:
        """
        Kiểm tra execution context của driver có hợp lệ không bằng cách 
        thực thi một JavaScript đơn giản và an toàn.
        
        Args:
            driver: WebDriver instance cần kiểm tra
            
        Returns:
            bool: True nếu execution context còn hoạt động, False nếu đã mất
        """
        if not driver:
            return False
            
        try:
            # Sử dụng một script JavaScript đơn giản để kiểm tra execution context
            result = driver.execute_script("return navigator.userAgent")
            # Kiểm tra thêm kết quả có hợp lệ không
            if not result or not isinstance(result, str):
                self.logger.debug("Execution context không ổn định (userAgent không hợp lệ)")
                return False
            return True
        except Exception as e:
            error_msg = str(e).lower()
            if "no such execution context" in error_msg:
                self.logger.debug("Phát hiện lỗi 'no such execution context'")
            elif "no such window" in error_msg:
                self.logger.debug("Phát hiện lỗi 'no such window'")
            else:
                self.logger.debug(f"Lỗi kiểm tra execution context: {str(e)}")
            return False

    def _try_restore_execution_context(self, driver) -> bool:
        """
        Thử phục hồi execution context bằng cách mở một tab mới hoặc thực hiện điều hướng an toàn.
        
        Args:
            driver: WebDriver instance cần phục hồi
            
        Returns:
            bool: True nếu phục hồi thành công, False nếu thất bại
        """
        if not driver:
            return False
            
        try:
            # Kiểm tra window handles hiện tại
            try:
                handles = driver.window_handles
                if not handles:
                    self.logger.debug("Không có window handles, không thể phục hồi")
                    return False
            except Exception:
                self.logger.debug("Không thể lấy window handles")
                return False
                
            # Chiến lược 1: Thử chuyển đến tab đầu tiên
            try:
                driver.switch_to.window(driver.window_handles[0])
            except Exception as e:
                self.logger.debug(f"Không thể chuyển đến tab đầu tiên: {str(e)}")
                return False
                
            # Chiến lược 2: Thử điều hướng đến about:blank
            try:
                driver.get("about:blank")
                time.sleep(0.5)  # Chờ ngắn để đảm bảo trang đã load
            except Exception as e:
                self.logger.debug(f"Không thể điều hướng đến about:blank: {str(e)}")
                return False
                
            # Kiểm tra lại execution context
            try:
                result = driver.execute_script("return document.readyState")
                if result in ["complete", "interactive"]:
                    self.logger.info("Đã phục hồi execution context thành công")
                    return True
            except Exception:
                pass
                
            return False
        except Exception as e:
            self.logger.debug(f"Không thể phục hồi execution context: {type(e).__name__}: {str(e)}")
            return False

    def _add_cookies_adaptive(self, driver, cookies_by_domain: Dict[str, List[Dict]]) -> int:
        """
        Thêm cookie với chiến lược thích ứng để tối ưu hiệu suất.
        Phân chia thành các batch nhỏ, ưu tiên cookie xác thực,
        và tự động điều chỉnh kích thước batch.
        
        Args:
            driver: WebDriver instance
            cookies_by_domain: Dict chứa cookie theo domain
            
        Returns:
            int: Số domain đã thêm cookie thành công
        """
        # Kiểm tra shutdown ngay từ đầu hàm
        if is_shutdown_requested():
            raise RuntimeError("Shutdown requested - dừng thêm cookie")

        success_domains = 0
        batch_size = self.config.initial_batch_size
        min_batch_size = self.config.min_batch_size
        max_batch_size = self.config.max_batch_size
        prioritize_auth = self.config.prioritize_auth

        for domain, cookie_list in cookies_by_domain.items():
            # Kiểm tra shutdown trước mỗi loop lớn
            if is_shutdown_requested():
                raise RuntimeError("Shutdown requested - dừng processing domain")

            if domain not in self.config.domain_url_map:
                self.logger.warning(f"Domain {domain} không có trong cấu hình, bỏ qua")
                continue

            domain_url = self.config.domain_url_map[domain]
            self.logger.info(f" - Mở {domain_url} để thêm {len(cookie_list)} cookie (domain={domain})")

            # Nếu số lượng cookie nhỏ hơn batch size tối thiểu, thêm một lần
            if len(cookie_list) <= min_batch_size:
                if add_cookies_for_domain(
                    driver=driver,
                    domain=domain,
                    cookie_list=cookie_list,
                    domain_url_map=self.config.domain_url_map,
                    timeout=self.config.get_timeout("cookie_inject", 3),
                    max_retries=self.config.max_retries
                ):
                    success_domains += 1
                continue

            # Sắp xếp cookie theo ưu tiên nếu cần
            if prioritize_auth:
                cookie_list = self._prioritize_auth_cookies(cookie_list)

            # Chia thành các batch
            cookie_batches = [cookie_list[i:i+batch_size] for i in range(0, len(cookie_list), batch_size)]
            batch_success_times = []
            batch_success = False

            for i, batch in enumerate(cookie_batches):
                batch_start = time.time()
                self.logger.debug(f"   Batch {i+1}/{len(cookie_batches)}: Thêm {len(batch)} cookies")
                
                success = add_cookies_for_domain(
                    driver=driver,
                    domain=domain,
                    cookie_list=batch,
                    domain_url_map=self.config.domain_url_map,
                    timeout=self.config.get_timeout("cookie_inject", 3),
                    max_retries=1
                )
                
                batch_end = time.time()
                
                if success:
                    batch_success = True
                    batch_time = batch_end - batch_start
                    batch_success_times.append(batch_time)
                    
                    # Điều chỉnh kích thước batch dựa trên hiệu suất
                    if len(batch_success_times) >= 2:
                        avg_time = sum(batch_success_times[-2:]) / 2
                        if avg_time < 1.0:
                            batch_size = min(max_batch_size, int(batch_size * 1.2))
                        elif avg_time > 3.0:
                            batch_size = max(min_batch_size, int(batch_size * 0.8))
                        self.logger.debug(f"   Thời gian batch: {avg_time:.2f}s -> Điều chỉnh batch size = {batch_size}")
                        
            if batch_success:
                success_domains += 1
        return success_domains

    def _prioritize_auth_cookies(self, cookie_list: List[Dict]) -> List[Dict]:
        """
        Sắp xếp cookie theo ưu tiên, đặt cookie xác thực lên đầu.
        
        Args:
            cookie_list: Danh sách cookie cần sắp xếp
            
        Returns:
            List[Dict]: Danh sách cookie đã sắp xếp
        """
        def get_cookie_priority(cookie):
            name = cookie.get('name', '').lower()
            # Các từ khóa liên quan đến xác thực
            auth_keywords = ['auth', 'token', 'session', 'signin', 'login', 'credential', 
                            'access', '.auth', 'jwt', 'refresh', 'id', 'estsauth', 'aad']
            
            # Điểm ưu tiên ban đầu
            priority = 0
            
            # Ưu tiên cao cho cookie xác thực
            for keyword in auth_keywords:
                if keyword in name:
                    priority += 100
                    break
                    
            # Ưu tiên httpOnly và secure cookies
            if cookie.get('httpOnly', False):
                priority += 20
                
            if cookie.get('secure', False):
                priority += 10
                
            # Ưu tiên cookie có thời hạn dài
            if 'expiry' in cookie:
                expiry = cookie['expiry']
                now = int(time.time())
                if expiry > now:
                    # Thời gian còn lại (ngày)
                    days_left = (expiry - now) / (24 * 3600)
                    if days_left > 30:
                        priority += 5
            else:
                # Cookie không có expiry (session cookie) có ưu tiên thấp hơn
                priority -= 10
                
            return priority
            
        # Sắp xếp theo thứ tự ưu tiên giảm dần
        return sorted(cookie_list, key=get_cookie_priority, reverse=True)

    def process_batch(self, files: List[Path], worker_id: int = 0) -> Dict[str, Dict]:
        """
        Xử lý một batch các file cookie.
        Cookie chỉ được coi là hợp lệ khi final_validation = True
        (tức là đạt cả login_success và api_success)
        
        Args:
            files: Danh sách các file cần kiểm tra
            worker_id: ID của worker đang xử lý
            
        Returns:
            Dict[str, Dict]: Kết quả kiểm tra cho mỗi file
        """
        results = {}
        batch_stats = {
            "total": len(files),
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "error": 0,
            "start_time": time.time()
        }
        # Kiểm tra cờ shutdown toàn cục
        if is_shutdown_requested():
            self.logger.info(f"Worker {worker_id}: Phát hiện yêu cầu shutdown, dừng xử lý batch")
            return results
        try:
            for i, file_path in enumerate(files):
                batch_msg = f"[Worker {worker_id}] File {i+1}/{len(files)}: {file_path.name}"
                self.logger.info(f"Kiểm tra {batch_msg}")
                print(f"Đang kiểm tra {batch_msg}...")
                
                try:
                    # Tính thời gian xử lý
                    start_time = time.time()
                    is_valid, details = self.check_cookie_file(file_path)
                    duration = time.time() - start_time
                    
                    # Đánh dấu trạng thái cuối cùng dựa trên final_validation
                    final_validation = details.get("final_validation", False)
                    final_status = is_valid and final_validation
                    
                    # Di chuyển file cookie nếu hợp lệ theo logic mới
                    new_path = None
                    if final_status and self.auto_save_results:
                        new_path = self.result_manager.move_valid_cookie(file_path, True, details)
                        
                    # Cập nhật kết quả
                    results[file_path.name] = {
                        "final_valid": final_status,  # Thêm tên mới cho kết quả cuối cùng
                        "valid": final_status,  # Chỉ True khi cả is_valid và final_validation đều True
                        "details": details,
                        "timestamp": datetime.now().isoformat(),
                        "worker_id": worker_id,
                        "moved_to": str(new_path) if new_path else None,
                        "duration": duration,
                        "login_success": details.get("login_status", False),
                        "api_success": details.get("api_check_status", False),
                        "final_validation": final_validation
                    }
                    
                    # Cập nhật thống kê batch
                    batch_stats["processed"] += 1
                    if final_status:
                        batch_stats["valid"] += 1
                    elif "error" in details or "api_error" in details:
                        batch_stats["error"] += 1
                    else:
                        batch_stats["invalid"] += 1
                    
                    # Hiển thị kết quả chi tiết
                    self._display_result_status(file_path.name, is_valid, details, final_validation)
                    
                    # Ghi log kết quả
                    log_status = "OK" if final_status else "FAIL"
                    if is_valid and not final_validation:
                        log_status = "LOGIN_OK_API_FAIL"
                    self.logger.info(f"Kết quả {batch_msg} => {log_status} [{duration:.2f}s]")

                    # Nếu đăng nhập thành công nhưng API thất bại, hiển thị cảnh báo
                    if is_valid and details.get("login_status", False) and not details.get("api_check_status", False):
                        self.logger.warning(f"Cookie {file_path.name} KHÔNG được di chuyển: API verification thất bại mặc dù đăng nhập thành công"
                                            f" (ID={details.get('user_id', 'unknown')}, Name={details.get('display_name', 'unknown')})")
                            
                    # Thêm kết quả vào result_manager
                    self.result_manager.add_result(file_path.name, results[file_path.name])
                    
                except InvalidSessionIdException:
                    self.logger.error(f"Phiên driver không hợp lệ: {file_path.name}")
                    results[file_path.name] = {
                        "valid": False,
                        "error": "Phiên WebDriver không hợp lệ",
                        "timestamp": datetime.now().isoformat(),
                        "worker_id": worker_id,
                        "final_validation": False
                    }
                    self.result_manager.add_result(file_path.name, results[file_path.name])
                    batch_stats["error"] += 1
                    batch_stats["processed"] += 1
                    break
                    
                except Exception as e:
                    error_msg = f"Lỗi: {type(e).__name__}: {str(e)}"
                    self.logger.error(f"Lỗi kiểm tra {batch_msg}: {error_msg}")
                    results[file_path.name] = {
                        "valid": False,
                        "error": error_msg,
                        "timestamp": datetime.now().isoformat(),
                        "worker_id": worker_id,
                        "final_validation": False
                    }
                    self.result_manager.add_result(file_path.name, results[file_path.name])
                    batch_stats["error"] += 1
                    batch_stats["processed"] += 1
                    
            # Tính toán tổng thống kê cuối cùng của batch
            batch_stats["total_time"] = time.time() - batch_stats["start_time"]
            batch_stats["avg_time"] = batch_stats["total_time"] / batch_stats["processed"] if batch_stats["processed"] > 0 else 0
            batch_stats["valid_percentage"] = (batch_stats["valid"] / batch_stats["processed"] * 100) if batch_stats["processed"] > 0 else 0
            
            # Ghi log tổng kết batch
            self.logger.info(
                f"Batch [Worker {worker_id}] hoàn tất: {batch_stats['processed']}/{batch_stats['total']} files, "
                f"Hợp lệ: {batch_stats['valid']} ({batch_stats['valid_percentage']:.1f}%), "
                f"Thời gian: {batch_stats['total_time']:.2f}s, "
                f"Trung bình: {batch_stats['avg_time']:.2f}s/file"
            )
            
        except Exception as e:
            self.logger.error(f"Lỗi phiên WebDriver (worker {worker_id}): {type(e).__name__}: {str(e)}")
            
        return results

    def _display_result_status(self, filename: str, is_valid: bool, details: Dict, final_validation: bool):
        """
        Hiển thị trạng thái kết quả kiểm tra cookie với logic mới.
        
        Args:
            filename: Tên file cookie
            is_valid: Kết quả kiểm tra cơ bản
            details: Chi tiết kết quả
            final_validation: Kết quả xác thực cuối cùng
        """
        login_success = details.get("login_status", False)
        api_success = details.get("api_check_status", False)
        api_performed = details.get("api_check_performed", False)
        
        # Biểu tượng đúng/sai
        valid_symbol = "✓" if is_valid and final_validation else "✗"
        
        # Thông tin chi tiết để hiển thị
        display_name = details.get("display_name", "Unknown")
        domain_name = details.get("domain_name", "")
        user_info = f"{display_name}" if not domain_name else f"{display_name}@{domain_name}"
        
        error = details.get("error", "")
        api_error = details.get("api_error", "")
        error_msg = error or api_error
        
        # Hiển thị kết quả theo logic mới
        if is_valid and final_validation:
            # Cookie hoàn toàn hợp lệ
            print(f"{valid_symbol} File {filename} -> Hoàn toàn hợp lệ (Login OK, API OK: {user_info})")
        elif is_valid and login_success and api_performed and not api_success:
            # Đăng nhập OK nhưng API thất bại
            print(f"{valid_symbol} File {filename} -> Không hợp lệ (Login OK, API FAILED)")
            if api_error:
                print(f"  Lỗi API: {api_error}")
        elif is_valid and login_success and not api_performed:
            # Đăng nhập OK nhưng không thực hiện API
            print(f"{valid_symbol} File {filename} -> Không hợp lệ (Login OK, API không thực hiện)")
        elif not is_valid and login_success:
            # Đăng nhập OK nhưng cookie không hợp lệ vì lý do khác
            print(f"{valid_symbol} File {filename} -> Không hợp lệ (Login OK, lỗi khác)")
            if error_msg:
                print(f"  Lỗi: {error_msg}")
        else:
            # Cookie không hợp lệ (không đăng nhập được)
            print(f"{valid_symbol} File {filename} -> Không hợp lệ (Login thất bại)")
            if error_msg:
                print(f"  Lỗi: {error_msg}")

    def check_multiple_cookie_files_adaptive(
        self,
        cookie_files: List[Path],
        task_scheduler: AdaptiveTaskScheduler,
        batch_size: int = 10
    ) -> None:
        """
        Kiểm tra nhiều file cookie với tự động điều chỉnh số lượng worker.
        
        Args:
            cookie_files: Danh sách các file cookie cần kiểm tra
            task_scheduler: Đối tượng AdaptiveTaskScheduler để quản lý workers
            batch_size: Kích thước batch mặc định
        """
        # Kiểm tra shutdown ngay từ đầu hàm
        if is_shutdown_requested():
            self.logger.warning("Yêu cầu shutdown đã được phát hiện, không khởi động quá trình kiểm tra")
            return

        if not cookie_files:
            self.logger.warning("Không có file cookie nào để kiểm tra")
            return

        # Lọc các file hợp lệ
        valid_files = [f for f in cookie_files if f.exists() and f.is_file()]
        invalid_files = [f for f in cookie_files if f not in valid_files]
        
        # Xử lý các file không hợp lệ
        for f in invalid_files:
            error_result = {
                "valid": False,
                "error": "File không tồn tại hoặc không phải file",
                "timestamp": datetime.now().isoformat()
            }
            self.result_manager.add_result(f.name, error_result)
            self.logger.warning(f"File không hợp lệ: {f}")

        if not valid_files:
            self.logger.error("Không có file cookie hợp lệ để kiểm tra")
            return

        # Thiết lập phiên làm việc
        self.logger.info(f"=== Bắt đầu kiểm tra {len(valid_files)} file cookie ===")
        self.session_stats = {
            "start_time": datetime.now(),
            "end_time": None,
            "total_checked": len(valid_files),
            "valid_count": 0,
            "invalid_count": 0,
            "error_count": 0,
        }

        # Chia file thành các batch
        batches = [valid_files[i:i+batch_size] for i in range(0, len(valid_files), batch_size)]
        total_batches = len(batches)
        self.logger.info(f"Chia thành {total_batches} batch, mỗi batch tối đa {batch_size} file")
        
        # Biến theo dõi tiến độ
        processed_batches = 0
        start_time = time.time()
        lock = threading.RLock()
        
        # Sử dụng semaphore để kiểm soát số lượng worker đồng thời
        worker_semaphore = threading.BoundedSemaphore(value=task_scheduler.max_workers)
        shutdown_event = threading.Event()  # Để báo hiệu khi cần dừng các worker

        # Theo dõi các batch đang được xử lý và đã hoàn thành
        active_batches = set()
        completed_results = {}
        
        def dynamic_rate_limiter():
            """Điều chỉnh số lượng worker hoạt động dựa trên optimal_workers hiện tại"""
            # Kiểm tra cờ shutdown 
            if is_shutdown_requested():
                return 1  # Giảm xuống 1 worker nếu đang shutdown
                
            current_optimal = task_scheduler.get_optimal_workers()
            # Đặt giá trị mới cho semaphore thông qua release/acquire
            current_value = worker_semaphore._value  # Cẩn thận: thuộc tính nội bộ
            
            if current_value < current_optimal:
                # Cần tăng số lượng worker
                for _ in range(current_optimal - current_value):
                    try:
                        worker_semaphore.release()
                    except ValueError:
                        # Semaphore đã đạt giá trị tối đa
                        break
            elif current_value > current_optimal:
                # Cần giảm số lượng worker
                for _ in range(current_value - current_optimal):
                    # Không block, chỉ thử acquire nếu có sẵn
                    if not worker_semaphore.acquire(blocking=False):
                        break
            
            return current_optimal
        
        def process_batch_worker(batch_idx, batch_files):
            """Hàm worker xử lý một batch"""
            # Kiểm tra cờ shutdown trước khi thực hiện acquire semaphore
            if is_shutdown_requested() or shutdown_event.is_set():
                return None
                
            with worker_semaphore:
                # Kiểm tra cờ shutdown sau khi acquire semaphore
                if is_shutdown_requested() or shutdown_event.is_set():
                    return None
                
                # Ghi nhận batch đang được xử lý
                with lock:
                    active_batches.add(batch_idx)
                    
                worker_id = batch_idx + 1
                task_id = f"batch_{batch_idx}"
                task_scheduler.record_task_start(task_id)
                
                try:
                    # Xử lý batch file
                    batch_results = self.process_batch(batch_files, worker_id)
                    
                    # Kiểm tra cờ shutdown sau khi xử lý batch
                    if is_shutdown_requested() or shutdown_event.is_set():
                        with lock:
                            if batch_idx in active_batches:
                                active_batches.remove(batch_idx)
                        self.logger.info(f"Batch {batch_idx} hoàn thành nhưng phát hiện yêu cầu shutdown")
                        task_scheduler.record_task_complete(task_id, True)
                        return None
                    
                    # Cập nhật tiến độ
                    nonlocal processed_batches
                    with lock:
                        processed_batches += 1
                        progress = processed_batches / total_batches
                        elapsed = time.time() - start_time
                        eta = (elapsed / progress - elapsed) if progress > 0 else 0
                        
                        # In tiến độ
                        print(
                            f"\n--- Tiến độ: {processed_batches}/{total_batches} batch "
                            f"({progress*100:.1f}%) - ETA: {eta/60:.1f} phút - Workers: {dynamic_rate_limiter()} ---\n"
                        )
                        
                        # Lưu kết quả
                        completed_results[batch_idx] = True
                        active_batches.remove(batch_idx)
                        
                    # Báo hoàn thành cho scheduler
                    task_scheduler.record_task_complete(task_id, True)
                    return batch_idx
                    
                except Exception as e:
                    error_msg = f"Lỗi xử lý batch {batch_idx} (Worker {worker_id}): {type(e).__name__}: {str(e)}"
                    self.logger.error(f"{error_msg}\n{traceback.format_exc()}")
                    
                    with lock:
                        completed_results[batch_idx] = False
                        if batch_idx in active_batches:
                            active_batches.remove(batch_idx)
                        
                    task_scheduler.record_task_complete(task_id, False)
                    return None
        
        try:
            # Sử dụng ThreadPoolExecutor với kích thước pool lớn nhất có thể
            # Số lượng concurrent sẽ được kiểm soát bằng semaphore
            max_possible_workers = min(32, os.cpu_count() * 5)  # Giới hạn hợp lý 
            
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=max_possible_workers) as executor:
                # Submit tất cả các batch vào executor
                futures = {executor.submit(process_batch_worker, idx, batch): idx 
                        for idx, batch in enumerate(batches)}
                
                try:
                    # Theo dõi và xử lý kết quả khi hoàn thành
                    for future in as_completed(futures):
                        # Kiểm tra cờ shutdown trước khi xử lý future 
                        if is_shutdown_requested():
                            self.logger.warning("Phát hiện yêu cầu shutdown, dừng xử lý batches tiếp theo")
                            # Hủy các future đang chờ
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            # Dừng executor
                            executor.shutdown(wait=False)
                            break
                        
                        batch_idx = futures[future]
                        try:
                            result = future.result()
                            # Không cần làm gì thêm vì đã xử lý trong worker function
                        except Exception as e:
                            self.logger.error(
                                f"Exception không xử lý trong batch {batch_idx}: {type(e).__name__}: {str(e)}"
                            )
                        
                        # Cứ 5 batch, hiển thị báo cáo hiệu suất
                        if processed_batches > 0 and processed_batches % 5 == 0:
                            # Kiểm tra cờ shutdown trước khi hiển thị metrics
                            if is_shutdown_requested():
                                break
                                
                            perf_report = task_scheduler.get_performance_report()
                            self.logger.info(
                                f"Metrics hiệu suất: {perf_report['tasks_completed']} tasks, "
                                f"avg_time={perf_report['avg_task_time']:.2f}s, workers={perf_report['current_workers']}"
                            )
                            
                            # Điều chỉnh semaphore theo giá trị optimal mới
                            dynamic_rate_limiter()
                
                except KeyboardInterrupt:
                    self.logger.warning("Phát hiện KeyboardInterrupt trong loop xử lý futures, dừng xử lý...")
                    
                    # Đặt cờ shutdown toàn cục và sự kiện shutdown cục bộ
                    request_shutdown()
                    shutdown_event.set()
                    
                    # Thông báo cho người dùng
                    print("\nPhát hiện Ctrl+C - Đang dừng các tiến trình và dọn dẹp tài nguyên...")
                    
                    # Hủy các future đang chờ
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    
                    # Dừng executor ngay lập tức
                    executor.shutdown(wait=False)
                    
                    # Đợi ngắn cho việc thông báo
                    time.sleep(0.5)
                    
                    # Re-raise để main xử lý
                    raise
        
        except KeyboardInterrupt:
            # Xử lý khi người dùng dừng bằng Ctrl+C
            self.logger.warning("Phát hiện KeyboardInterrupt, đang dừng xử lý...")
            
            # Đặt cờ shutdown và sự kiện
            request_shutdown()
            shutdown_event.set()
            
            # Thông báo cho người dùng
            print("\nĐang dừng xử lý và đóng tài nguyên...")
            
            # In thông tin về các batch đang xử lý
            with lock:
                if active_batches:
                    active_batch_list = sorted(list(active_batches))
                    self.logger.info(f"Đang hủy {len(active_batches)} batch đang xử lý: {active_batch_list}")
            
            # Dừng ngay, không re-raise vì đã ra khỏi context manager của executor
        
        except Exception as e:
            # Xử lý các ngoại lệ khác
            self.logger.error(f"Lỗi không mong đợi khi xử lý batches: {type(e).__name__}: {str(e)}")
            traceback.print_exc()
            
            # Đặt cờ shutdown
            request_shutdown()
            shutdown_event.set()
        
        finally:
            # Kiểm tra và đóng các tài nguyên còn đang sử dụng
            try:
                # Hiển thị thông tin các batch đang xử lý nếu có
                with lock:
                    if active_batches and not is_shutdown_requested():
                        self.logger.info(f"Đang đợi {len(active_batches)} batch đang xử lý hoàn thành...")
            except:
                pass
            
            # Ghi nhận thống kê cuối cùng
            try:
                self.session_stats["end_time"] = datetime.now()
                total_time = (self.session_stats["end_time"] - self.session_stats["start_time"]).total_seconds()
                success_count = sum(1 for success in completed_results.values() if success)
                
                # Thêm thông tin về shutdown nếu có
                shutdown_msg = " (bị dừng do yêu cầu shutdown)" if is_shutdown_requested() else ""
                
                self.logger.info(
                    f"Hoàn tất kiểm tra {processed_batches}/{total_batches} batch "
                    f"({success_count} thành công){shutdown_msg} trong {total_time:.1f} giây"
                )
            except Exception as e:
                self.logger.error(f"Lỗi khi ghi nhận thống kê: {str(e)}")

    def get_stats(self) -> Dict[str, Any]:
        """
        Lấy thống kê hiện tại về quá trình kiểm tra.
        
        Returns:
            Dict: Thống kê về quá trình kiểm tra
        """
        with self._resource_lock:
            stats = self.session_stats.copy()
            stats["current_time"] = datetime.now()
            
            if stats["start_time"]:
                stats["elapsed_seconds"] = (stats["current_time"] - stats["start_time"]).total_seconds()
                
                # Tính tốc độ xử lý
                if stats["elapsed_seconds"] > 0 and stats["total_checked"] > 0:
                    processed = stats["valid_count"] + stats["invalid_count"] + stats["error_count"]
                    stats["processing_rate"] = processed / stats["elapsed_seconds"]
                else:
                    stats["processing_rate"] = 0
            
            return stats
