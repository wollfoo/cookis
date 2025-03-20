"""
api_verifier.py - Module xác thực thông tin tài khoản qua dữ liệu lưu trữ trình duyệt
- Trích xuất và xác minh thông tin người dùng từ localStorage và sessionStorage
- Chuyên biệt trong xác thực tài khoản Azure AD thông qua browser storage
- Triển khai cơ chế xử lý lỗi và phục hồi mạnh mẽ
- Tối ưu cho môi trường production với logging toàn diện
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

# Import from global_state
from global_state import is_shutdown_requested

from webdriver_pool import retry_with_backoff

# Import driver utilities (giả định đã được triển khai trong project)
from web_driver_utils.driver_maintenance import verify_driver_connection, wait_for_page_load


# Decorator retry với exponential backoff
# def retry_with_backoff(max_retries=2, initial_delay=1, backoff_factor=2, jitter=True):
#     """
#     Decorator cho phép retry với chiến lược exponential backoff.
#     Tối ưu hóa với kiểm tra trạng thái shutdown trước và sau mỗi lần retry.
    
#     Args:
#         max_retries: Số lần retry tối đa
#         initial_delay: Thời gian chờ ban đầu giữa các lần retry (giây)
#         backoff_factor: Hệ số tăng thời gian chờ cho các lần retry tiếp theo
#         jitter: Thêm độ ngẫu nhiên vào thời gian chờ để tránh "thundering herd"
        
#     Returns:
#         Decorator function cho retry logic
#     """
#     import random
#     from functools import wraps
    
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             # Kiểm tra shutdown request trước khi bắt đầu
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
#                     # Kiểm tra shutdown request sau khi bắt exception
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
                    
#                     # Chia nhỏ sleep thành các đoạn để kiểm tra shutdown
#                     for _ in range(int(wait_time * 10)):  # 0.1s mỗi lần kiểm tra
#                         if is_shutdown_requested():
#                             logger.warning(f"Shutdown được yêu cầu trong khi sleep, dừng retry {func.__name__}")
#                             raise RuntimeError("Shutdown requested during retry sleep") from e
#                         time.sleep(0.1)
                    
#             # Nếu hết lần retry mà vẫn lỗi, raise exception
#             if last_exception:
#                 raise last_exception
                
#         return wrapper
#     return decorator


class ApiVerificationError(Exception):
    """Exception được raise khi có lỗi xác thực API"""
    pass


class ApiVerifier:
    """
    Lớp chuyên trách xác thực thông tin người dùng qua API và local/session storage.
    Tối ưu hóa cho việc trích xuất và xác thực thông tin tài khoản Azure AD thông qua
    dữ liệu lưu trữ của trình duyệt.
    
    Tính năng:
    - Trích xuất dữ liệu nâng cao từ browser storage
    - Xử lý lỗi và phục hồi mạnh mẽ
    - Xác thực toàn diện thông tin người dùng
    - Logging và chẩn đoán chi tiết
    """
    def __init__(self, config, logger=None):
        """
        Khởi tạo ApiVerifier với cấu hình và logger.
        
        Args:
            config: Đối tượng cấu hình chứa URL và timeouts
            logger: Logger instance (không bắt buộc)
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        
        # Thống kê cho việc theo dõi các lần xác thực
        self.verification_stats = {
            "attempts": 0,
            "successes": 0,
            "failures": 0,
            "execution_context_errors": 0,
            "api_errors": {},
            "data_sources": {}
        }

    @staticmethod
    def _check_execution_context(driver: webdriver.Chrome) -> bool:
        """
        Kiểm tra execution context của driver có hợp lệ không bằng cách
        thực thi một JavaScript đơn giản.
        
        Args:
            driver: WebDriver instance cần kiểm tra
            
        Returns:
            bool: True nếu execution context còn tồn tại, False nếu đã mất
        """
        if not driver:
            return False
            
        try:
            # Thực hiện script đơn giản để kiểm tra execution context
            result = driver.execute_script("return window.navigator.userAgent")
            if not result:
                return False
            return True
        except Exception as e:
            error_msg = str(e).lower()
            
            if "no such execution context" in error_msg:
                return False
            elif "no such window" in error_msg:
                return False
            else:
                # Các lỗi khác cũng coi như context không hợp lệ
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
                self.verification_stats["execution_context_errors"] += 1
                self.logger.warning("Lỗi execution context khi thực thi script")
            elif "no such window" in error_msg:
                self.logger.warning("Lỗi no such window khi thực thi script")
            else:
                self.logger.debug(f"Lỗi khi thực thi script: {str(e)}")
            return default_value

    def _is_on_azure_portal(self, driver: webdriver.Chrome) -> bool:
        """
        Kiểm tra xem driver có đang ở trang Azure Portal không.
        
        Args:
            driver: WebDriver instance
            
        Returns:
            bool: True nếu đang ở Azure Portal, False nếu không
        """
        try:
            if not self._check_execution_context(driver):
                return False
                
            current_url = driver.current_url.lower()
            return "portal.azure.com" in current_url
        except Exception:
            return False

    @retry_with_backoff(max_retries=2, initial_delay=1, backoff_factor=2, jitter=True)
    def verify_user_data(self, driver: webdriver.Chrome) -> Tuple[bool, Dict]:
        """
        Xác thực thông tin người dùng bằng cách trích xuất dữ liệu từ localStorage và sessionStorage.
        Tối ưu hóa để không tải lại trang nếu đã ở Azure Portal.
        
        Logic xử lý:
        1. Kiểm tra nếu đã ở Azure Portal, tránh điều hướng không cần thiết
        2. Điều hướng đến Azure Portal nếu cần
        3. Trích xuất thông tin người dùng từ storage
        4. Xác thực định dạng user ID (UUID) và sự hiện diện của các dữ liệu cần thiết
        
        Args:
            driver: WebDriver instance sử dụng cho việc xác thực
            
        Returns:
            Tuple[bool, Dict]: (verification_success, details)
        """
        details = {
            "api_check_status": False,
            "user_id": None,
            "display_name": None,
            "domain_name": None,
            "data_source": None,
            "api_error": None,
            "execution_steps": []
        }
        
        # Lưu trữ original_level để khôi phục sau khi xử lý
        original_level = self.logger.level
        
        try:
            # Tăng log level tạm thời để xem chi tiết hơn
            self.logger.setLevel(logging.DEBUG)
            details["execution_steps"].append("logger_level_set_to_debug")
            
            # Ghi nhận thời gian bắt đầu
            start_time = time.time()
            self.logger.info(f"verify_user_data: Bắt đầu xác thực người dùng ({start_time})")
            details["execution_steps"].append(f"start:{int(start_time*1000)}")
            
            # Tăng biến đếm số lần xác thực
            self.verification_stats["attempts"] += 1
            
            # Kiểm tra driver còn kết nối không
            if not verify_driver_connection(driver):
                self.logger.warning("Driver không còn kết nối khi kiểm tra API")
                details["api_error"] = "WebDriver không còn kết nối"
                self.verification_stats["failures"] += 1
                return False, details
            
            # Ghi nhận bắt đầu quá trình xác thực
            self.logger.info("Bắt đầu quá trình xác thực API")
            details["execution_steps"].append("verification_started")
            
            # Kiểm tra URL hiện tại trước khi điều hướng
            portal_url = "https://portal.azure.com/"
            current_url = ""
            
            try:
                # Kiểm tra execution context trước khi lấy URL
                if not self._check_execution_context(driver):
                    self.logger.warning("Execution context không tồn tại trước khi lấy URL")
                    details["api_error"] = "Execution context không tồn tại"
                    self.verification_stats["execution_context_errors"] += 1
                    self.verification_stats["failures"] += 1
                    return False, details
                
                current_url = driver.current_url
                details["current_url"] = current_url
                
                # Kiểm tra xem đã ở trang Azure Portal chưa
                already_on_portal = "portal.azure.com" in current_url.lower()
                
                if already_on_portal:
                    self.logger.info(f"Đã ở trang Azure Portal: {current_url}")
                    details["execution_steps"].append("already_on_portal")
                else:
                    self.logger.info(f"Chưa ở trang Azure Portal, đang ở: {current_url}")
                    details["execution_steps"].append(f"not_on_portal:{current_url}")
                    
                    # Điều hướng đến Azure Portal nếu chưa ở đó
                    self.logger.info(f"Truy cập Azure Portal: {portal_url}")
                    details["execution_steps"].append(f"accessing_portal:{portal_url}")
                    
                    try:
                        if is_shutdown_requested():
                            raise RuntimeError("Shutdown requested - dừng driver.get")
                            
                        driver.get(portal_url)
                        details["execution_steps"].append("portal_access_started")
                    except WebDriverException as e:
                        self.logger.warning(f"Lỗi khi điều hướng đến {portal_url}: {str(e)}")
                        details["api_error"] = f"Lỗi điều hướng: {str(e)}"
                        self.verification_stats["failures"] += 1
                        return False, details
                    
                    # Chờ trang load
                    self.logger.info("Đang chờ trang load...")
                    page_load_timeout = self.config.get_timeout("page_load", 15)
                    
                    loaded = wait_for_page_load(
                        driver, 
                        timeout=page_load_timeout,
                        check_js_complete=True,
                        check_network_idle=True
                    )
                    
                    if not loaded:
                        self.logger.warning(f"Timeout {page_load_timeout}s khi chờ trang load")
                        details["api_error"] = f"Trang không load xong trong {page_load_timeout}s"
                        self.verification_stats["failures"] += 1
                        return False, details
            except WebDriverException as e:
                self.logger.warning(f"Lỗi khi kiểm tra URL hiện tại: {str(e)}")
                details["api_error"] = f"Lỗi kiểm tra URL: {str(e)}"
                self.verification_stats["failures"] += 1
                return False, details
            
            # Kiểm tra các phần tử tiêu đề xuất hiện - dù ở trang Portal sẵn hay mới chuyển đến
            self.logger.info("Đang kiểm tra các phần tử chính của trang...")
            details["execution_steps"].append("checking_dom_elements")
            
            # Đợi các phần tử h1, h2 xuất hiện
            element_found = False
            try:
                # Đợi các phần tử h1 hoặc h2 với timeout ngắn hơn nếu đã ở trang Portal
                timeout = 5 if "portal.azure.com" in current_url.lower() else 10
                shorter_wait = WebDriverWait(driver, timeout)
                
                # Thử tìm h1 trước
                try:
                    h1_element = shorter_wait.until(
                        EC.presence_of_element_located((By.TAG_NAME, "h1"))
                    )
                    element_found = True
                    self.logger.info(f"Đã tìm thấy phần tử h1: {h1_element.text}")
                    details["execution_steps"].append("h1_element_found")
                    details["page_h1_text"] = h1_element.text
                except Exception as h1_error:
                    self.logger.debug(f"Không tìm thấy phần tử h1: {str(h1_error)}")
                    
                    # Nếu không tìm thấy h1, thử tìm h2
                    if not element_found:
                        try:
                            h2_element = shorter_wait.until(
                                EC.presence_of_element_located((By.TAG_NAME, "h2"))
                            )
                            element_found = True
                            self.logger.info(f"Đã tìm thấy phần tử h2: {h2_element.text}")
                            details["execution_steps"].append("h2_element_found")
                            details["page_h2_text"] = h2_element.text
                        except Exception as h2_error:
                            self.logger.debug(f"Không tìm thấy phần tử h2: {str(h2_error)}")
                
                # Nếu không tìm thấy h1 hoặc h2, thử tìm bất kỳ phần tử nào khác
                if not element_found:
                    try:
                        # Thử tìm bất kỳ phần tử quan trọng nào
                        any_element = shorter_wait.until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.fxs-blade-content"))
                        )
                        element_found = True
                        self.logger.info("Đã tìm thấy phần tử nội dung trang Azure Portal")
                        details["execution_steps"].append("content_element_found")
                    except Exception as any_error:
                        self.logger.warning(f"Không tìm thấy phần tử nội dung chính: {str(any_error)}")
                
            except Exception as element_error:
                self.logger.warning(f"Lỗi khi kiểm tra các phần tử DOM: {str(element_error)}")
                details["execution_steps"].append("dom_element_check_error")
            
            # Lấy thông tin trang hiện tại sau khi đã điều hướng hoặc kiểm tra
            try:
                # Kiểm tra execution context trước khi lấy thông tin trang
                if self._check_execution_context(driver):
                    current_url = driver.current_url
                    page_title = driver.title
                    details["current_url"] = current_url
                    details["page_title"] = page_title
                    self.logger.info(f"Trang hiện tại: URL={current_url}, Title={page_title}")
            except Exception as e:
                self.logger.debug(f"Không lấy được thông tin trang: {str(e)}")
            
            # Đợi thêm thời gian ngắn (1s) nếu đã ở trang portal, nếu không thì đợi lâu hơn (2s)
            if "portal.azure.com" in current_url.lower():
                time.sleep(1)
            else:
                time.sleep(2)
            details["execution_steps"].append("portal_verified")
            
            # Ghi nhận bắt đầu quá trình xác thực qua storage
            self.logger.info("=== Bắt đầu kiểm tra API bằng dữ liệu storage ===")
            details["execution_steps"].append("storage_verification_start")
            verification_start = time.time()
            
            # Thực hiện xác thực từ storage
            api_success, api_details = self._get_data_from_storage(driver)
            
            # Ghi nhận kết quả
            verification_duration = time.time() - verification_start
            details["verification_duration_ms"] = verification_duration * 1000
            self.logger.info(f"Kiểm tra API đã thực thi trong {verification_duration:.3f}s")
            details["execution_steps"].append(f"verification_complete:{verification_duration:.3f}s")
            
            # Cập nhật kết quả
            details.update(api_details)
            elapsed = time.time() - start_time
            
            if api_success:
                self.logger.info(f"Kiểm tra API thành công ({elapsed:.2f}s)")
                details["execution_steps"].append("verification_success")
                self.verification_stats["successes"] += 1
            else:
                self.logger.warning(f"Kiểm tra API thất bại: {api_details.get('api_error', 'Không rõ lý do')}")
                details["execution_steps"].append("verification_failed")
                self.verification_stats["failures"] += 1
                
                # Theo dõi loại lỗi để phân tích
                error_type = api_details.get('api_error', 'Unknown error')
                if error_type:
                    short_error = error_type[:50]  # Cắt ngắn thông báo lỗi dài
                    self.verification_stats["api_errors"][short_error] = self.verification_stats["api_errors"].get(short_error, 0) + 1
            
            # Ghi nhận tổng thời gian xử lý
            details["total_execution_time_ms"] = elapsed * 1000
            details["execution_steps"].append(f"complete:{int(time.time()*1000)}")
            
            return api_success, details
                
        except InvalidSessionIdException as e:
            error_msg = f"Phiên webdriver không hợp lệ: {str(e)}"
            self.logger.warning(error_msg)
            details["api_error"] = error_msg
            details["execution_steps"].append("invalid_session_error")
            self.verification_stats["failures"] += 1
            return False, details
            
        except Exception as e:
            error_msg = f"Lỗi kiểm tra API: {e.__class__.__name__}: {str(e)}"
            self.logger.error(error_msg)
            details["api_error"] = error_msg
            details["execution_steps"].append(f"unexpected_error:{e.__class__.__name__}")
            self.verification_stats["failures"] += 1
            return False, details
        
        finally:
            # Khôi phục logger level ban đầu
            try:
                self.logger.setLevel(original_level)
            except:
                pass

    def _get_data_from_storage(self, driver: webdriver.Chrome) -> Tuple[bool, Dict]:
        """
        Lấy dữ liệu người dùng từ localStorage và sessionStorage.
        Tối ưu hóa cho việc trích xuất thông tin tài khoản Azure AD.
        
        Chiến lược:
        1. Kiểm tra truy cập storage
        2. Trích xuất dữ liệu bằng script JavaScript tối ưu
        3. Xác minh tính toàn vẹn và định dạng dữ liệu
        4. Trả về kết quả xác thực
        
        Args:
            driver: WebDriver instance để trích xuất dữ liệu
            
        Returns:
            Tuple[bool, Dict]: (verification_success, details)
        """
        details = {
            "api_check_status": False,
            "user_id": None,
            "display_name": None,
            "domain_name": None,
            "data_source": None,
            "is_valid_account": False,
            "api_error": None,
            "debug_info": {}
        }
        
        start_time = time.time()
        self.logger.debug("Bắt đầu trích xuất dữ liệu từ storage")
        
        try:
            # Tăng script timeout để tránh lỗi timeout
            driver.set_script_timeout(10)  # Tăng timeout từ 5s lên 10s
            
            # Kiểm tra truy cập storage
            try:
                storage_access_script = """
                return {
                    hasLocalStorage: typeof localStorage !== 'undefined',
                    hasSessionStorage: typeof sessionStorage !== 'undefined',
                    localStorage: typeof localStorage !== 'undefined' ? Object.keys(localStorage).length : 0,
                    sessionStorage: typeof sessionStorage !== 'undefined' ? Object.keys(sessionStorage).length : 0,
                    url: window.location.href
                }
                """
                storage_access = self._safe_execute_script(driver, storage_access_script)
                
                details["debug_info"]["storage_access"] = storage_access
                
                if not storage_access:
                    details["api_error"] = "Script trả về null khi kiểm tra storage access"
                    self.logger.warning("Script trả về null khi kiểm tra storage access")
                    return False, details
                
                if not storage_access.get('hasLocalStorage') or not storage_access.get('hasSessionStorage'):
                    details["api_error"] = "Không thể truy cập storage (localStorage hoặc sessionStorage không khả dụng)"
                    self.logger.warning(f"Storage không khả dụng: {storage_access}")
                    return False, details
                    
                # Ghi log thông tin storage
                self.logger.debug(
                    f"Storage truy cập OK: localStorage ({storage_access.get('localStorage', 0)} key), "
                    f"sessionStorage ({storage_access.get('sessionStorage', 0)} key)"
                )
                
            except Exception as storage_error:
                details["api_error"] = f"Lỗi kiểm tra khả năng truy cập storage: {str(storage_error)}"
                self.logger.warning(f"Lỗi kiểm tra storage: {str(storage_error)}")
                return False, details
            
            # Script tối ưu - CHỈ TRUY CẬP localStorage và sessionStorage
            optimized_script = r"""
            function extractCriticalAzureData() {
                // Kết quả
                const result = {
                    user: {
                        userId: null,
                        displayName: null,
                        email: null
                    },
                    tenant: {
                        id: null,
                        domain: null
                    },
                    isValid: false,
                    dataSources: [],
                    idp: null,
                    debug: {
                        localStorage: {},
                        sessionStorage: {},
                        errors: []
                    }
                };

                try {
                    // Log các key quan trọng
                    const importantLocalStorageKeys = ['SavedDefaultDirectory', 'AzurePortalDiscriminator', 'preferred-domain'];
                    const importantSessionStorageKeys = ['msal.account.keys', 'msal.token.keys'];
                    
                    // Lưu keys đã tìm thấy để debug
                    importantLocalStorageKeys.forEach(key => {
                        const value = localStorage.getItem(key);
                        result.debug.localStorage[key] = value ? "found" : "not found";
                    });
                    
                    importantSessionStorageKeys.forEach(key => {
                        // Tìm tất cả keys khớp với pattern
                        Object.keys(sessionStorage).forEach(storageKey => {
                            if (storageKey.includes(key)) {
                                result.debug.sessionStorage[storageKey] = "found";
                            }
                        });
                    });
                    
                    // 1. Lấy tenantId từ localStorage
                    result.tenant.id = localStorage.getItem('SavedDefaultDirectory') || null;
                    if (result.tenant.id) {
                        result.dataSources.push('localStorage.SavedDefaultDirectory');
                    }

                    // 2. Lấy thông tin từ tài khoản trong sessionStorage
                    let accountKeys = [];
                    try {
                        const accountKeysStr = sessionStorage.getItem('msal.account.keys');
                        if (accountKeysStr) {
                            accountKeys = JSON.parse(accountKeysStr);
                            result.debug.sessionStorage['msal.account.keys_parsed'] = true;
                        } else {
                            // Tìm kiếm các key có chứa 'account.keys'
                            Object.keys(sessionStorage).forEach(key => {
                                if (key.includes('account.keys')) {
                                    try {
                                        const keysData = JSON.parse(sessionStorage.getItem(key));
                                        if (Array.isArray(keysData) && keysData.length > 0) {
                                            accountKeys = keysData;
                                            result.debug.sessionStorage['alternative_account_keys'] = key;
                                        }
                                    } catch (e) {
                                        result.debug.errors.push("Error parsing alternative account keys: " + e.message);
                                    }
                                }
                            });
                        }
                    } catch (e) {
                        result.debug.errors.push("Error parsing account keys: " + e.message);
                    }

                    if (accountKeys.length > 0) {
                        const accountKey = accountKeys[0];
                        try {
                            const accountData = JSON.parse(sessionStorage.getItem(accountKey) || '{}');
                            result.debug.sessionStorage['account_data_parsed'] = true;
                            
                            // Check IdP - xác định tài khoản cá nhân
                            if (accountData.idTokenClaims && accountData.idTokenClaims.idp) {
                                result.idp = accountData.idTokenClaims.idp;
                            }
                            
                            // Trích xuất userId - ưu tiên oid từ idTokenClaims
                            if (accountData.idTokenClaims && accountData.idTokenClaims.oid) {
                                result.user.userId = accountData.idTokenClaims.oid;
                                result.dataSources.push('accountData.idTokenClaims.oid');
                            } else if (accountData.localAccountId) {
                                result.user.userId = accountData.localAccountId;
                                result.dataSources.push('accountData.localAccountId');
                            }
                            
                            // Trích xuất displayName - ưu tiên name từ idTokenClaims
                            if (accountData.idTokenClaims && accountData.idTokenClaims.name) {
                                result.user.displayName = accountData.idTokenClaims.name;
                                result.dataSources.push('accountData.idTokenClaims.name');
                            } else if (accountData.name) {
                                result.user.displayName = accountData.name;
                                result.dataSources.push('accountData.name');
                            }
                            
                            // Trích xuất email/username
                            if (accountData.username) {
                                result.user.email = accountData.username;
                                result.dataSources.push('accountData.username');
                                
                                // Lấy domain từ email nếu chưa có
                                if (!result.tenant.domain && result.user.email.includes('@')) {
                                    result.tenant.domain = result.user.email.split('@')[1];
                                    result.dataSources.push('email_domain');
                                }
                            }
                        } catch (e) {
                            result.debug.errors.push("Error processing account data: " + e.message);
                        }
                    } else {
                        result.debug.errors.push("No account keys found");
                    }
                    
                    // 3. Tìm thông tin từ token - kiểm tra tất cả các key có thể
                    let idTokenKeys = [];
                    
                    // Tìm tất cả các msal.token.keys có thể có
                    Object.keys(sessionStorage).forEach(key => {
                        if (key.includes('msal.token.keys')) {
                            try {
                                const tokenKeysData = JSON.parse(sessionStorage.getItem(key));
                                if (tokenKeysData && tokenKeysData.idToken && tokenKeysData.idToken.length > 0) {
                                    idTokenKeys.push(...tokenKeysData.idToken);
                                    result.debug.sessionStorage['token_keys_source'] = key;
                                }
                            } catch (e) {
                                result.debug.errors.push("Error parsing token keys from " + key + ": " + e.message);
                            }
                        }
                    });
                    
                    // Phân tích JWT từ ID token
                    if (idTokenKeys.length > 0) {
                        result.debug.sessionStorage['id_token_keys_found'] = idTokenKeys.length;
                        
                        for (const tokenKey of idTokenKeys) {
                            try {
                                const tokenData = JSON.parse(sessionStorage.getItem(tokenKey) || '{}');
                                if (tokenData.secret) {
                                    const jwt = parseJwt(tokenData.secret);
                                    result.debug.sessionStorage['jwt_parsed'] = true;
                                    
                                    // Kiểm tra IdP nếu chưa có
                                    if (!result.idp && jwt.idp) {
                                        result.idp = jwt.idp;
                                    }
                                    
                                    // Cập nhật thông tin nếu chưa có
                                    if (!result.user.userId && jwt.oid) {
                                        result.user.userId = jwt.oid;
                                        result.dataSources.push('jwt.oid');
                                    }
                                    
                                    if (!result.user.displayName && jwt.name) {
                                        result.user.displayName = jwt.name;
                                        result.dataSources.push('jwt.name');
                                    }
                                    
                                    if (!result.user.email && (jwt.upn || jwt.email || jwt.preferred_username)) {
                                        result.user.email = jwt.upn || jwt.email || jwt.preferred_username;
                                        result.dataSources.push('jwt.email');
                                    }
                                    
                                    if (!result.tenant.id && jwt.tid) {
                                        result.tenant.id = jwt.tid;
                                        result.dataSources.push('jwt.tid');
                                    }
                                    
                                    // Thêm tìm kiếm domain từ email trong token
                                    if (!result.tenant.domain && jwt.email && jwt.email.includes('@')) {
                                        result.tenant.domain = jwt.email.split('@')[1];
                                        result.dataSources.push('jwt.email_domain');
                                    } else if (!result.tenant.domain && jwt.upn && jwt.upn.includes('@')) {
                                        result.tenant.domain = jwt.upn.split('@')[1];
                                        result.dataSources.push('jwt.upn_domain');
                                    }
                                    
                                    break; // Chỉ cần phân tích 1 token
                                }
                            } catch (e) {
                                result.debug.errors.push("Error processing token: " + e.message);
                            }
                        }
                    } else {
                        result.debug.errors.push("No token keys found");
                    }
                    
                    // 4. Kiểm tra tính hợp lệ của userId (định dạng UUID)
                    const isValidUUID = result.user.userId && 
                                    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(result.user.userId);
                    result.debug.isValidUUID = isValidUUID;
                                    
                    // 5. Kiểm tra displayName
                    const hasDisplayName = result.user.displayName !== undefined && 
                                        result.user.displayName !== null;
                    result.debug.hasDisplayName = hasDisplayName;
                                    
                    // Đánh dấu tài khoản hợp lệ - CHỈ cần có userId dạng UUID VÀ displayName xác định
                    result.isValid = isValidUUID && hasDisplayName;
                    
                    // Thêm thông tin URL
                    result.debug.url = window.location.href;
                    result.debug.title = document.title;
                    
                } catch (error) {
                    result.debug.errors.push("Critical error: " + error.message);
                }
                
                return result;
            }

            // Hàm phân tích JWT token
            function parseJwt(token) {
                try {
                    const base64Url = token.split('.')[1];
                    if (!base64Url) return {};
                    
                    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
                    const jsonPayload = decodeURIComponent(atob(base64).split('').map(function(c) {
                        return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
                    }).join(''));
                    return JSON.parse(jsonPayload);
                } catch (e) {
                    return {};
                }
            }

            // Thực thi trích xuất
            return extractCriticalAzureData();
            """
            
            # Thực thi script được tối ưu
            self.logger.debug("Thực thi script JavaScript để trích xuất dữ liệu từ storage")
            storage_result = self._safe_execute_script(driver, optimized_script)
            script_time = time.time() - start_time
            self.logger.debug(f"Thời gian thực thi script: {script_time:.3f}s")
            
            # Kiểm tra kết quả từ script
            if storage_result is None:
                details["api_error"] = "Script JavaScript không trả về kết quả (null)"
                self.logger.warning("Script trả về null, không có dữ liệu để xử lý")
                end_time = time.time() - start_time
                self.logger.info(f"Kiểm tra API đã thực thi trong {end_time:.3f}s")
                return False, details
                
            # Lưu debug info
            if 'debug' in storage_result:
                debug_info = storage_result.get('debug', {})
                details["debug_info"].update(debug_info)
                
                # Ghi log lỗi JavaScript nếu có
                js_errors = debug_info.get('errors', [])
                if js_errors:
                    self.logger.debug(f"JavaScript errors: {', '.join(js_errors[:3])}")
                    
            # Xử lý kết quả trả về từ storage
            details["user_id"] = storage_result.get('user', {}).get('userId')
            details["display_name"] = storage_result.get('user', {}).get('displayName')
            details["domain_name"] = storage_result.get('tenant', {}).get('domain')
            details["tenant_id"] = storage_result.get('tenant', {}).get('id')
            details["is_valid_account"] = storage_result.get('isValid', False)
            details["email"] = storage_result.get('user', {}).get('email')
            details["data_source"] = ", ".join(storage_result.get('dataSources', [])[:3])
            details["idp"] = storage_result.get('idp')
            
            # Theo dõi các nguồn dữ liệu cho thống kê
            if 'dataSources' in storage_result:
                for source in storage_result['dataSources']:
                    self.verification_stats["data_sources"][source] = self.verification_stats["data_sources"].get(source, 0) + 1
            
            # Ghi log chi tiết về dữ liệu tìm được
            if details["user_id"] or details["display_name"] or details["domain_name"]:
                self.logger.debug(
                    f"Dữ liệu từ storage: userId={details['user_id']}, "
                    f"displayName={details['display_name']}, domain={details['domain_name']}"
                )
            else:
                self.logger.debug("Không tìm thấy dữ liệu người dùng trong storage")
            
            # Kiểm tra định dạng UUID chuẩn
            is_valid_uuid = bool(details["user_id"] and re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', details["user_id"]))
            
            # Kiểm tra đủ thông tin cần thiết VÀ userId có định dạng UUID chuẩn
            api_check_status = bool(details["user_id"] and details["display_name"] and details["domain_name"] and is_valid_uuid)
            details["api_check_status"] = api_check_status
            
            # Lưu thông tin tổng hợp để debug
            details["debug_info"]["final_check"] = {
                "has_user_id": bool(details["user_id"]),
                "has_display_name": bool(details["display_name"]),
                "has_domain_name": bool(details["domain_name"]),
                "is_valid_uuid": is_valid_uuid,
                "api_check_status": api_check_status
            }
            
            # Hoàn tất xử lý và trả về kết quả
            end_time = time.time() - start_time
            
            # Nếu tài khoản có đủ thông tin và userId đúng định dạng UUID - trả về thành công
            if api_check_status:
                self.logger.info(f"Lấy thông tin thành công: {details['display_name']}, {details['domain_name']}")
                self.logger.info(f"Kiểm tra API đã thực thi trong {end_time:.3f}s")
                return True, details
            
            # Nếu có thông tin cơ bản nhưng userId không đúng định dạng UUID
            elif details["user_id"] and details["display_name"] and details["domain_name"]:
                error_msg = f"userId không phải UUID chuẩn: {details['user_id']}"
                self.logger.warning(error_msg)
                details["api_error"] = error_msg
                self.logger.info(f"Kiểm tra API đã thực thi trong {end_time:.3f}s")
                return False, details
            
            # Trường hợp thiếu thông tin
            elif details["user_id"] or details["display_name"] or details["domain_name"]:
                missing = []
                if not details["user_id"]: missing.append("user_id")
                if not details["display_name"]: missing.append("display_name")
                if not details["domain_name"]: missing.append("domain_name")
                error_msg = f"Thiếu thông tin: {', '.join(missing)}"
                self.logger.debug(error_msg)
                details["api_error"] = error_msg
                self.logger.info(f"Kiểm tra API đã thực thi trong {end_time:.3f}s")
                return False, details
            
            # Không tìm thấy thông tin
            else:
                error_msg = "Không tìm thấy thông tin người dùng trong storage"
                self.logger.debug(error_msg)
                details["api_error"] = error_msg
                self.logger.info(f"Kiểm tra API đã thực thi trong {end_time:.3f}s")
                return False, details
                
        except WebDriverException as e:
            end_time = time.time() - start_time
            error_msg = str(e)
            details["api_error"] = f"WebDriver lỗi: {error_msg}"
            
            if "no such window" in error_msg.lower():
                self.logger.warning("Cửa sổ đã đóng khi truy cập storage")
            elif "no such execution context" in error_msg.lower():
                self.logger.warning("Execution context đã mất khi truy cập storage")
                self.verification_stats["execution_context_errors"] += 1
            else:
                self.logger.warning(f"Lỗi WebDriver khi lấy dữ liệu từ storage: {error_msg}")
                
            self.logger.info(f"Kiểm tra API đã thực thi trong {end_time:.3f}s")
            return False, details
            
        except Exception as e:
            end_time = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"
            details["api_error"] = error_msg
            self.logger.warning(f"Lỗi khi lấy dữ liệu từ storage: {error_msg}")
            self.logger.info(f"Kiểm tra API đã thực thi trong {end_time:.3f}s")
            return False, details

    def get_verification_stats(self) -> Dict[str, Any]:
        """
        Lấy thống kê về các lần xác thực.
        
        Returns:
            Dict: Thống kê về số lần xác thực, tỷ lệ thành công, v.v.
        """
        total_attempts = self.verification_stats["attempts"]
        success_rate = 0
        
        if total_attempts > 0:
            success_rate = (self.verification_stats["successes"] / total_attempts) * 100
            
        return {
            "total_attempts": total_attempts,
            "successes": self.verification_stats["successes"],
            "failures": self.verification_stats["failures"],
            "success_rate": success_rate,
            "execution_context_errors": self.verification_stats["execution_context_errors"],
            "top_errors": dict(sorted(
                self.verification_stats["api_errors"].items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:5]),
            "data_sources": dict(sorted(
                self.verification_stats["data_sources"].items(),
                key=lambda x: x[1],
                reverse=True
            ))
        }
        
    def reset_stats(self) -> None:
        """Đặt lại thống kê xác thực."""
        self.verification_stats = {
            "attempts": 0,
            "successes": 0,
            "failures": 0,
            "execution_context_errors": 0,
            "api_errors": {},
            "data_sources": {}
        }
        self.logger.info("Đã reset thống kê ApiVerifier")
        
    def is_healthy(self) -> bool:
        """
        Kiểm tra xem ApiVerifier có đang trong trạng thái hoạt động tốt không.
        
        Returns:
            bool: True nếu ít lỗi và tỷ lệ thành công cao, False nếu ngược lại.
        """
        # Nếu có quá nhiều lỗi execution_context, coi như không healthy
        if self.verification_stats["execution_context_errors"] > 10:
            return False
            
        # Kiểm tra tỷ lệ thành công nếu có đủ dữ liệu
        total_attempts = self.verification_stats["attempts"] 
        if total_attempts >= 5:
            success_rate = (self.verification_stats["successes"] / total_attempts) * 100
            if success_rate < 30:  # Nếu tỷ lệ thành công quá thấp (<30%)
                return False
                
        return True
