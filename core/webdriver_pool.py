# webdriver_pool.py

import os
import time
import logging
import threading
import random
import psutil
import re
from typing import Optional, Tuple, Dict, Any, List
from functools import wraps
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, InvalidSessionIdException,
    NoSuchElementException, StaleElementReferenceException
)

# Import từ web_driver_utils
from web_driver_utils.driver_lifecycle import init_driver_with_genlogin
from web_driver_utils.driver_maintenance import (
    verify_driver_connection,
    close_current_driver
)

from web_driver_utils.browser_cleaner import clear_browsing_data

# Import từ global_state
from global_state import is_shutdown_requested, request_shutdown

# Định nghĩa các lớp ngoại lệ
class WebDriverPoolError(Exception):
    """Ngoại lệ khi WebDriver Pool gặp vấn đề."""
    pass

class ProfileRunningError(Exception):
    """Ngoại lệ khi profile đang chạy trên thiết bị khác."""
    pass

class DriverInitError(Exception):
    """Ngoại lệ khi không thể khởi tạo driver."""
    pass

# Cơ chế retry với exponential backoff
def retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2, jitter=True):
    """
    Decorator tối ưu với kiểm tra shutdown trước và sau mỗi lần retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Kiểm tra shutdown trước khi bắt đầu
            if is_shutdown_requested():
                logger = logging.getLogger()
                logger.warning(f"Shutdown đã được yêu cầu trước khi thực thi {func.__name__}")
                raise RuntimeError("Shutdown requested")
            
            retry_count = 0
            delay = initial_delay
            last_exception = None
            
            while retry_count < max_retries:
                try:
                    return func(*args, **kwargs)
                except (WebDriverException, TimeoutException, StaleElementReferenceException) as e:
                    # Kiểm tra shutdown ngay sau khi bắt exception
                    if is_shutdown_requested():
                        logger = logging.getLogger()
                        logger.warning(f"Shutdown được yêu cầu sau exception, dừng retry {func.__name__}")
                        raise RuntimeError("Shutdown requested during retry") from e
                    
                    retry_count += 1
                    last_exception = e
                    if retry_count >= max_retries:
                        break
                        
                    # Tính thời gian chờ với backoff
                    wait_time = delay * (backoff_factor ** (retry_count - 1))
                    if jitter:
                        wait_time *= random.uniform(0.8, 1.2)
                    
                    logger = logging.getLogger()
                    logger.warning(
                        f"Lỗi {type(e).__name__} khi thực thi {func.__name__} "
                        f"(lần thử {retry_count}/{max_retries}): {str(e)}. "
                        f"Thử lại sau {wait_time:.2f}s"
                    )
                    
                    # Kiểm tra lần nữa trước khi sleep
                    if is_shutdown_requested():
                        logger.warning(f"Shutdown được yêu cầu trước khi sleep, dừng retry {func.__name__}")
                        raise RuntimeError("Shutdown requested before retry sleep") from e
                    
                    # Sử dụng sleep với kiểm tra cờ shutdown
                    for _ in range(int(wait_time * 10)):  # Chia nhỏ sleep thành 0.1s
                        if is_shutdown_requested():
                            logger.warning(f"Shutdown được yêu cầu trong khi sleep, dừng retry {func.__name__}")
                            raise RuntimeError("Shutdown requested during retry sleep") from e
                        time.sleep(0.1)
                    
            # Nếu hết lần retry mà vẫn lỗi, raise exception
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator

class WebDriverPool:
    """
    Pool quản lý các WebDriver instance để tái sử dụng.
    Cải tiến để khắc phục lỗi 'No Such Execution Context' và các vấn đề liên quan.
    
    Cải tiến chính:
    - Kiểm tra sức khỏe driver mạnh mẽ hơn với kiểm tra execution context
    - Cơ chế tạo và quản lý driver chống lỗi
    - Giám sát tình trạng pool và tự động phục hồi
    - Cơ chế cách ly và xử lý driver lỗi
    """
    def __init__(
        self,
        config,
        max_size: int = 3,
        ttl: int = 600,
        logger: Optional[logging.Logger] = None,
        warm_pool: bool = True
    ):
        self.config = config
        self.max_size = max_size
        self.ttl = ttl  # Thời gian sống (giây)
        self.pool = []  # [(driver, gen, profile_id, timestamp, priority, health_score), ...]
        self.lock = threading.RLock()
        self.check_interval = 30  # Giảm xuống 30 giây thay vì 60 giây
        self.last_check = time.time()
        self.logger = logger or logging.getLogger(__name__)
        self.shutdown_flag = False
        self.active_drivers = 0  # Số driver đang được sử dụng
        self.driver_counters = {
            "created": 0, 
            "reused": 0, 
            "failed": 0, 
            "closed": 0,
            "execution_context_errors": 0,
            "recovered": 0
        }
        self.health_metrics = []
        self.auto_replenish = True  # Bật mặc định để tự động phục hồi pool
        self.min_pool_size = 1
        self.max_driver_age = 3600  # 1 giờ, sau đó ưu tiên thay thế driver
        self.execution_context_check_interval = 5  # Giây, kiểm tra execution context mỗi 5 giây
        self.last_execution_context_check = {}  # {profile_id: timestamp}
        
        # Thêm theo dõi lỗi
        self.error_history = {
            "execution_context": {},  # {profile_id: count}
            "no_such_window": {},
            "connection_errors": {},
            "other_errors": {}
        }
        
        # Start health check thread với interval ngắn hơn
        self.health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.health_thread.start()
        
        # Warm up pool nếu cần
        if warm_pool:
            self.warm_up()

    def _try_replenish_pool(self, target_count: int = None):
        """
        Cố gắng bổ sung thêm driver vào pool cho đến khi đạt số lượng mục tiêu.
        Cải tiến với cơ chế handling lỗi tốt hơn.
        """
        if self.shutdown_flag:
            return
            
        target = target_count if target_count is not None else self.min_pool_size
        current_count = len(self.pool)
        
        if current_count >= target:
            return
            
        to_add = min(target - current_count, self.max_size - current_count)
        
        if to_add <= 0:
            return
            
        self.logger.debug(f"Cố gắng bổ sung {to_add} driver vào pool")
        
        for _ in range(to_add):
            try:
                # Kiểm tra cờ shutdown trước mỗi lần tạo driver mới
                if self.shutdown_flag or is_shutdown_requested():
                    self.logger.info("Phát hiện shutdown, dừng bổ sung driver")
                    return
                    
                driver, gen, profile_id = self._create_new_driver()
                
                # Kiểm tra kĩ execution context trước khi thêm vào pool
                if not self._check_execution_context(driver, profile_id):
                    self.logger.warning(f"Driver mới tạo có execution context không ổn định, bỏ qua")
                    close_current_driver(driver, gen, profile_id)
                    self.driver_counters["failed"] += 1
                    continue
                    
                with self.lock:
                    if len(self.pool) < self.max_size and not self.shutdown_flag:
                        # Thêm health_score = 100 (tốt nhất) cho driver mới
                        self.pool.append((driver, gen, profile_id, time.time(), 0, 100))
                        self.driver_counters["created"] += 1
                        self.logger.info(f"Đã bổ sung driver mới với profile {profile_id}")
                    else:
                        self.logger.debug("Pool đã đầy hoặc đang shutdown, đóng driver vừa tạo")
                        close_current_driver(driver, gen, profile_id)
                        self.driver_counters["closed"] += 1
            except Exception as e:
                self.logger.warning(f"Không thể tạo driver mới khi bổ sung pool: {type(e).__name__}: {str(e)}")
                self.driver_counters["failed"] += 1

    def warm_up(self, count: int = 1):
        """
        Khởi tạo trước một số driver để sẵn sàng sử dụng.
        Cải tiến với kiểm tra execution context kỹ lưỡng.
        """
        self.logger.info(f"Khởi tạo trước {count} driver cho pool...")
        successful_drivers = 0
        max_attempts = count * 2  # Cho phép nhiều lần thử hơn để đảm bảo đủ driver
        
        for attempt in range(max_attempts):
            # Dừng nếu đã khởi tạo đủ số lượng driver cần thiết
            if successful_drivers >= count:
                break
                
            try:
                if self.shutdown_flag or is_shutdown_requested():
                    self.logger.info("Phát hiện shutdown, dừng warm up")
                    break
                    
                driver, gen, profile_id = self._create_new_driver()
                
                # Kiểm tra execution context mạnh mẽ
                context_ok = self._check_execution_context(driver, profile_id)
                if not context_ok:
                    self.logger.warning(f"Driver warm-up với profile {profile_id} có execution context không ổn định")
                    close_current_driver(driver, gen, profile_id)
                    continue
                    
                with self.lock:
                    if len(self.pool) < self.max_size:
                        self.pool.append((driver, gen, profile_id, time.time(), 0, 100))  # health_score = 100
                        self.logger.info(f"Đã khởi tạo trước driver với profile {profile_id}")
                        self.driver_counters["created"] += 1
                        successful_drivers += 1
                    else:
                        self.logger.debug("Pool đã đầy, đóng driver vừa tạo")
                        close_current_driver(driver, gen, profile_id)
                        self.driver_counters["closed"] += 1
            except Exception as e:
                self.logger.warning(f"Không thể khởi tạo trước driver (lần thử {attempt+1}/{max_attempts}): {type(e).__name__}: {str(e)}")
                self.driver_counters["failed"] += 1
                
        self.logger.info(f"Warm up hoàn tất: {successful_drivers}/{count} driver đã sẵn sàng")

    def _health_check_loop(self):
        """
        Kiểm tra định kỳ sức khỏe của driver trong pool.
        Cải tiến với kiểm tra execution context thường xuyên.
        """
        while not self.shutdown_flag:
            try:
                # Kiểm tra cờ shutdown
                if is_shutdown_requested():
                    self.logger.info("Phát hiện cờ đóng, dừng health_check_loop")
                    break

                current_time = time.time()
                # Chạy health check đều đặn theo interval
                if current_time - self.last_check > self.check_interval:
                    self._perform_health_check()
                    self.last_check = current_time
                    
                    # Nếu pool đang thiếu driver, bổ sung thêm
                    if len(self.pool) < self.min_pool_size:
                        self._try_replenish_pool()
            except Exception as e:
                self.logger.error(f"Lỗi health check: {type(e).__name__}: {str(e)}")
                try:
                    # Ghi traceback để debug
                    import traceback
                    self.logger.debug(f"Traceback health check: {traceback.format_exc()}")
                except:
                    pass
            # Giảm thời gian sleep để phản ứng nhanh hơn với các vấn đề
            time.sleep(3)

    def _perform_health_check(self):
        """
        Thực hiện kiểm tra sức khỏe của tất cả driver trong pool.
        Cải tiến với phân loại và xử lý các loại lỗi khác nhau.
        """
        with self.lock:
            initial_count = len(self.pool)
            if initial_count == 0:
                return  # Pool trống, không cần check
                
            # Tạo danh sách mới chỉ chứa các driver hợp lệ và danh sách các driver cần đóng
            valid_drivers = []
            invalid_drivers = []
            
            # Kiểm tra từng driver trong pool
            for item in self.pool:
                driver, gen, profile_id, timestamp, priority, health_score = item
                current_time = time.time()
                
                # Kiểm tra TTL và sức khỏe
                expired = current_time - timestamp >= self.ttl
                health_check_result, new_health_score = self._check_driver_health(driver, profile_id, health_score)
                
                if expired or not health_check_result:
                    reason = "expired" if expired else "unhealthy"
                    invalid_drivers.append((item, reason))
                else:
                    # Cập nhật health_score
                    valid_drivers.append((driver, gen, profile_id, timestamp, priority, new_health_score))
            
            # Ghi nhận số lượng driver bị loại bỏ
            removed = len(invalid_drivers)
            
            # Đóng các driver không hợp lệ
            for (driver, gen, profile_id, _, _, _), reason in invalid_drivers:
                try:
                    self.logger.debug(f"Đóng driver không hợp lệ ({reason}) cho profile {profile_id}")
                    close_current_driver(driver, gen, profile_id)
                    self.driver_counters["closed"] += 1
                except Exception as e:
                    self.logger.warning(f"Lỗi khi đóng driver không hợp lệ: {str(e)}")
            
            # Cập nhật pool với chỉ các driver hợp lệ
            self.pool = valid_drivers
                
            # Ghi log kết quả
            if removed > 0:
                self.logger.debug(f"Đã loại bỏ {removed} driver hết hạn/không hoạt động")
                
            # Lưu metric
            current_time = time.time()
            self.health_metrics.append({
                "timestamp": current_time,
                "active_drivers": self.active_drivers,
                "pool_size": len(self.pool),
                "removed_drivers": removed,
                "created": self.driver_counters["created"],
                "reused": self.driver_counters["reused"],
                "failed": self.driver_counters["failed"],
                "closed": self.driver_counters["closed"],
                "execution_context_errors": self.driver_counters["execution_context_errors"],
                "check_duration": current_time - (self.health_metrics[-1]["timestamp"] if self.health_metrics else current_time)
            })
            
            # Giới hạn lịch sử metric
            if len(self.health_metrics) > 100:
                self.health_metrics = self.health_metrics[-100:]
            
            # Khởi tạo driver mới nếu cần
            if self.auto_replenish and len(self.pool) < self.min_pool_size and not self.shutdown_flag:
                self._try_replenish_pool(target_count=self.min_pool_size)

    def _check_driver_health(self, driver, profile_id, current_health_score=100):
        """
        Kiểm tra sức khỏe driver với đánh giá chi tiết và cập nhật health score.
        
        Returns:
            Tuple[bool, int]: (driver_healthy, new_health_score)
        """
        if not driver:
            return False, 0
            
        try:
            # Khởi tạo health score mới, bắt đầu từ điểm hiện tại
            new_health_score = current_health_score
            
            # 1. Kiểm tra cơ bản kết nối driver
            if not verify_driver_connection(driver):
                self.logger.debug(f"Driver của profile {profile_id} mất kết nối cơ bản")
                return False, 0
                
            # 2. Kiểm tra window handles
            try:
                window_handles = driver.window_handles
                if not window_handles or len(window_handles) == 0:
                    self.logger.debug(f"Driver của profile {profile_id} không có cửa sổ nào")
                    return False, 0
            except Exception as e:
                self.logger.debug(f"Lỗi kiểm tra window handles cho profile {profile_id}: {str(e)}")
                self._track_error(profile_id, e)
                return False, 0
                
            # 3. Kiểm tra execution context quan trọng nhất
            if not self._check_execution_context(driver, profile_id):
                return False, 0
                
            # 4. Thử lấy thông tin trang (URL hiện tại)
            try:
                current_url = driver.current_url
                # Giảm health score nếu URL không phải HTTP/HTTPS
                if not (current_url.startswith('http://') or current_url.startswith('https://')):
                    new_health_score = max(new_health_score - 10, 50)
            except Exception as e:
                self.logger.debug(f"Không thể lấy URL hiện tại cho profile {profile_id}: {str(e)}")
                self._track_error(profile_id, e)
                new_health_score = max(new_health_score - 20, 30)
                
            # 5. Thử lấy document.readyState
            try:
                ready_state = driver.execute_script("return document.readyState")
                if ready_state not in ['interactive', 'complete']:
                    new_health_score = max(new_health_score - 10, 40)
            except Exception as e:
                self.logger.debug(f"Không thể kiểm tra readyState cho profile {profile_id}: {str(e)}")
                self._track_error(profile_id, e)
                new_health_score = max(new_health_score - 15, 30)
                
            # 6. Kiểm tra process ChromeDriver còn chạy không
            try:
                if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                    if driver.service.process is None or driver.service.process.poll() is not None:
                        self.logger.debug(f"Process ChromeDriver của profile {profile_id} không còn chạy")
                        return False, 0
            except Exception:
                # Bỏ qua nếu không thể kiểm tra process
                pass
                
            # Nếu health score thấp hơn ngưỡng, coi như driver không khỏe
            if new_health_score < 50:
                self.logger.debug(f"Driver profile {profile_id} có health score thấp: {new_health_score}")
                return False, new_health_score
                
            return True, new_health_score
                
        except Exception as e:
            self.logger.debug(f"Lỗi không xác định khi kiểm tra driver profile {profile_id}: {str(e)}")
            self._track_error(profile_id, e)
            return False, 0

    def _check_execution_context(self, driver, profile_id):
        """
        Kiểm tra riêng execution context để phát hiện sớm lỗi "no such execution context".
        
        Returns:
            bool: True nếu execution context hoạt động tốt, False nếu lỗi
        """
        if not driver:
            return False
            
        # Thêm đánh dấu thời gian kiểm tra
        current_time = time.time()
        last_check = self.last_execution_context_check.get(profile_id, 0)
        
        # Chỉ kiểm tra theo interval để tránh kiểm tra quá thường xuyên
        if current_time - last_check < self.execution_context_check_interval:
            return True  # Giả định context còn tốt nếu mới kiểm tra gần đây
        
        self.last_execution_context_check[profile_id] = current_time
        
        try:
            # Thực hiện một script đơn giản để kiểm tra execution context
            result = driver.execute_script("return navigator.userAgent")
            if not result:
                self.logger.debug(f"Execution context cho profile {profile_id} không ổn định (userAgent trống)")
                return False
            return True
        except Exception as e:
            error_msg = str(e).lower()
            
            # Ghi nhận chi tiết để có thể quản lý tốt hơn
            if "no such execution context" in error_msg:
                self.logger.warning(f"Phát hiện lỗi 'no such execution context' cho profile {profile_id}")
                self.driver_counters["execution_context_errors"] += 1
                
                # Cập nhật lịch sử lỗi
                if profile_id not in self.error_history["execution_context"]:
                    self.error_history["execution_context"][profile_id] = 0
                self.error_history["execution_context"][profile_id] += 1
                
            elif "no such window" in error_msg:
                self.logger.warning(f"Phát hiện lỗi 'no such window' cho profile {profile_id}")
                
                # Cập nhật lịch sử lỗi
                if profile_id not in self.error_history["no_such_window"]:
                    self.error_history["no_such_window"][profile_id] = 0
                self.error_history["no_such_window"][profile_id] += 1
                
            else:
                self.logger.debug(f"Lỗi execution context cho profile {profile_id}: {str(e)}")
                
                # Cập nhật lịch sử lỗi
                if profile_id not in self.error_history["other_errors"]:
                    self.error_history["other_errors"][profile_id] = 0
                self.error_history["other_errors"][profile_id] += 1
                
            return False

    def _track_error(self, profile_id, exception):
        """
        Theo dõi các loại lỗi để phân tích và xử lý.
        """
        error_type = type(exception).__name__
        error_msg = str(exception).lower()
        
        if "execution context" in error_msg:
            if profile_id not in self.error_history["execution_context"]:
                self.error_history["execution_context"][profile_id] = 0
            self.error_history["execution_context"][profile_id] += 1
            self.driver_counters["execution_context_errors"] += 1
            
        elif "no such window" in error_msg:
            if profile_id not in self.error_history["no_such_window"]:
                self.error_history["no_such_window"][profile_id] = 0
            self.error_history["no_such_window"][profile_id] += 1
            
        elif "connection" in error_msg or "timeout" in error_msg:
            if profile_id not in self.error_history["connection_errors"]:
                self.error_history["connection_errors"][profile_id] = 0
            self.error_history["connection_errors"][profile_id] += 1
            
        else:
            if profile_id not in self.error_history["other_errors"]:
                self.error_history["other_errors"][profile_id] = 0
            self.error_history["other_errors"][profile_id] += 1

    def get_driver(self, priority: int = 0) -> Tuple[webdriver.Chrome, Any, str]:
        """
        Lấy driver khả dụng từ pool hoặc tạo mới nếu cần.
        Cải tiến với kiểm tra execution context kỹ lưỡng trước khi trả về.
        
        Args:
            priority: Mức độ ưu tiên (cao hơn được xử lý trước)
            
        Returns:
            Tuple[driver, gen, profile_id]
        """
        with self.lock:
            if self.shutdown_flag:
                raise WebDriverPoolError("WebDriver Pool đang trong quá trình shutdown")

            # Loại bỏ driver hết hạn hoặc không hoạt động
            now = time.time()
            initial_count = len(self.pool)
            
            # Kiểm tra sức khỏe và cập nhật health_score cho tất cả driver trong pool
            validated_pool = []
            for d, g, p, t, pri, health_score in self.pool:
                # Kiểm tra TTL
                if now - t >= self.ttl:
                    try:
                        self.logger.debug(f"Driver của profile {p} hết hạn TTL ({self.ttl}s), đóng")
                        close_current_driver(d, g, p)
                        self.driver_counters["closed"] += 1
                    except Exception as e:
                        self.logger.warning(f"Lỗi đóng driver hết hạn: {str(e)}")
                    continue
                
                # Kiểm tra sức khỏe và cập nhật health_score
                health_check_result, new_health_score = self._check_driver_health(d, p, health_score)
                if health_check_result:
                    validated_pool.append((d, g, p, t, pri, new_health_score))
                else:
                    try:
                        self.logger.debug(f"Driver của profile {p} không khỏe (score: {new_health_score}), đóng")
                        close_current_driver(d, g, p)
                        self.driver_counters["closed"] += 1
                    except Exception as e:
                        self.logger.warning(f"Lỗi đóng driver không khỏe: {str(e)}")
            
            removed = initial_count - len(validated_pool)
            if removed > 0:
                self.logger.debug(f"Đã loại bỏ {removed} driver hết hạn/không khỏe")
                
            self.pool = validated_pool
            
            # Nếu pool hiện tại trống, tạo driver mới
            if not self.pool:
                self.logger.info("Pool trống, tạo driver mới")
                try:
                    driver, gen, profile_id = self._create_new_driver()
                    
                    # Kiểm tra execution context ngay sau khi tạo
                    context_ok = self._check_execution_context(driver, profile_id)
                    if not context_ok:
                        self.logger.warning("Driver mới tạo có execution context không ổn định, tạo lại")
                        close_current_driver(driver, gen, profile_id)
                        
                        # Tạo lại driver một lần nữa
                        driver, gen, profile_id = self._create_new_driver()
                        if not self._check_execution_context(driver, profile_id):
                            self.logger.error("Driver mới tạo lần 2 vẫn lỗi execution context")
                            close_current_driver(driver, gen, profile_id)
                            self.driver_counters["failed"] += 1
                            raise WebDriverPoolError("Không thể tạo driver mới với execution context ổn định")
                    
                    self.logger.info(f"Đã tạo driver mới cho profile {profile_id}")
                    self.active_drivers += 1
                    self.driver_counters["created"] += 1
                    return driver, gen, profile_id
                except Exception as e:
                    self.logger.error(f"Không thể tạo driver mới: {type(e).__name__}: {str(e)}")
                    self.driver_counters["failed"] += 1
                    raise WebDriverPoolError(f"Không thể tạo driver mới: {str(e)}") from e

            # Trả về driver hiện có nếu có, ưu tiên theo priority và health_score
            if self.pool:
                # Sắp xếp theo priority (cao đến thấp) và health_score (cao đến thấp)
                self.pool.sort(key=lambda x: (x[4], x[5]), reverse=True)
                
                # Lấy driver từ đầu pool
                driver, gen, profile_id, _, _, health_score = self.pool.pop(0)
                
                # Kiểm tra execution context một lần nữa trước khi trả về
                context_ok = self._check_execution_context(driver, profile_id)
                if not context_ok:
                    self.logger.warning(f"Driver lấy từ pool ({profile_id}) có lỗi execution context, đóng")
                    try:
                        close_current_driver(driver, gen, profile_id)
                        self.driver_counters["closed"] += 1
                    except Exception as e:
                        self.logger.warning(f"Lỗi đóng driver lỗi context: {str(e)}")
                    
                    # Nếu còn driver khác trong pool, thử lại
                    if self.pool:
                        self.logger.info("Thử lấy driver khác từ pool")
                        driver, gen, profile_id, _, _, health_score = self.pool.pop(0)
                        context_ok = self._check_execution_context(driver, profile_id)
                        if not context_ok:
                            self.logger.warning(f"Driver thứ 2 từ pool ({profile_id}) cũng lỗi context, tạo mới")
                            try:
                                close_current_driver(driver, gen, profile_id)
                                self.driver_counters["closed"] += 1
                            except Exception:
                                pass
                            
                            # Tạo driver mới
                            driver, gen, profile_id = self._create_new_driver()
                            if not self._check_execution_context(driver, profile_id):
                                self.logger.error("Driver mới tạo vẫn lỗi execution context")
                                close_current_driver(driver, gen, profile_id)
                                self.driver_counters["failed"] += 1
                                raise WebDriverPoolError("Không thể tạo driver với execution context ổn định")
                    else:
                        # Tạo driver mới
                        driver, gen, profile_id = self._create_new_driver()
                        if not self._check_execution_context(driver, profile_id):
                            self.logger.error("Driver mới tạo vẫn lỗi execution context")
                            close_current_driver(driver, gen, profile_id)
                            self.driver_counters["failed"] += 1
                            raise WebDriverPoolError("Không thể tạo driver với execution context ổn định")
                
                self.logger.debug(f"Tái sử dụng driver với profile {profile_id} (health: {health_score})")
                self.active_drivers += 1
                self.driver_counters["reused"] += 1
                
                # Cập nhật thời gian kiểm tra execution context
                self.last_execution_context_check[profile_id] = time.time()
                
                return driver, gen, profile_id

            # Nếu đến đây mà không có driver, tạo mới
            try:
                driver, gen, profile_id = self._create_new_driver()
                
                # Kiểm tra execution context
                if not self._check_execution_context(driver, profile_id):
                    self.logger.warning("Driver mới tạo lỗi execution context, thử lại")
                    close_current_driver(driver, gen, profile_id)
                    
                    # Thử lại lần nữa
                    driver, gen, profile_id = self._create_new_driver()
                    if not self._check_execution_context(driver, profile_id):
                        self.logger.error("Driver mới tạo lần 2 vẫn lỗi execution context")
                        close_current_driver(driver, gen, profile_id)
                        self.driver_counters["failed"] += 1
                        raise WebDriverPoolError("Không thể tạo driver mới với execution context ổn định")
                
                self.logger.info(f"Đã tạo driver mới cho profile {profile_id}")
                self.active_drivers += 1
                self.driver_counters["created"] += 1
                
                # Cập nhật thời gian kiểm tra execution context
                self.last_execution_context_check[profile_id] = time.time()
                
                return driver, gen, profile_id
            except Exception as e:
                self.logger.error(f"Không thể tạo driver mới: {type(e).__name__}: {str(e)}")
                self.driver_counters["failed"] += 1
                raise WebDriverPoolError(f"Không thể tạo driver mới: {str(e)}") from e

    @retry_with_backoff(max_retries=3, initial_delay=2, backoff_factor=2)
    def _create_new_driver(self) -> Tuple[webdriver.Chrome, Any, str]:
        """
        Tạo và khởi tạo driver mới với cơ chế retry mạnh mẽ.
        Cải tiến với kiểm tra execution context sau khi tạo.
        """
        # Kiểm tra cờ shutdown trước khi tạo driver mới
        if is_shutdown_requested() or self.shutdown_flag:
            raise WebDriverPoolError("Yêu cầu shutdown, không thể tạo driver mới")

        driver, gen, profile_id = init_driver_with_genlogin(
            chrome_driver_path=self.config.chrome_driver_path,
            profile_id=None,
            page_load_timeout=self.config.get_timeout("page_load", 30),
            max_retries=2,
            force_stop_running=self.config.force_stop_running
        )

        if not driver or not gen or not profile_id:
            self.logger.warning(
                f"Khởi tạo không đầy đủ: driver={bool(driver)}, gen={bool(gen)}, profile_id={bool(profile_id)}"
            )
            if driver or gen or profile_id:
                close_current_driver(driver, gen, profile_id)
            raise WebDriverPoolError("Khởi tạo driver không đầy đủ")

        # Kiểm tra kết nối cơ bản và window handles
        if not verify_driver_connection(driver):
            self.logger.warning("Driver mới không có kết nối hợp lệ")
            close_current_driver(driver, gen, profile_id)
            raise WebDriverPoolError("Driver mới không có kết nối hợp lệ")
            
        try:
            # Kiểm tra window handles
            if len(driver.window_handles) == 0:
                self.logger.warning("Driver mới không có window handles")
                close_current_driver(driver, gen, profile_id)
                raise WebDriverPoolError("Driver mới không có window handles")
        except Exception as e:
            self.logger.warning(f"Lỗi kiểm tra window handles cho driver mới: {str(e)}")
            try:
                close_current_driver(driver, gen, profile_id)
            except:
                pass
            raise WebDriverPoolError(f"Lỗi kiểm tra driver mới: {str(e)}")
            
        # Kiểm tra execution context bằng cách thử thực hiện một script đơn giản
        try:
            driver.execute_script("return navigator.userAgent")
        except Exception as e:
            self.logger.warning(f"Driver mới có lỗi execution context: {str(e)}")
            try:
                close_current_driver(driver, gen, profile_id)
            except:
                pass
            raise WebDriverPoolError(f"Driver mới có lỗi execution context: {str(e)}")

        # Thêm thời gian sleep ngắn để đảm bảo ChromeDriver đã ổn định
        time.sleep(0.5)
        
        # Kiểm tra lại execution context sau sleep
        try:
            result = driver.execute_script("return window.navigator.userAgent")
            if not result:
                self.logger.warning("Driver mới trả về userAgent rỗng")
                close_current_driver(driver, gen, profile_id)
                raise WebDriverPoolError("Driver mới không ổn định (userAgent rỗng)")
        except Exception as e:
            self.logger.warning(f"Driver mới không ổn định sau sleep: {str(e)}")
            try:
                close_current_driver(driver, gen, profile_id)
            except:
                pass
            raise WebDriverPoolError(f"Driver mới không ổn định sau sleep: {str(e)}")
            
        # Thử điều hướng đến about:blank - trang an toàn để kiểm tra
        try:
            driver.get("about:blank")
        except Exception as e:
            self.logger.warning(f"Driver mới không thể điều hướng đến about:blank: {str(e)}")
            try:
                close_current_driver(driver, gen, profile_id)
            except:
                pass
            raise WebDriverPoolError(f"Driver mới không thể điều hướng: {str(e)}")
            
        # Kiểm tra một lần nữa execution context sau navigation
        try:
            document_ready = driver.execute_script("return document.readyState")
            if document_ready not in ['interactive', 'complete']:
                self.logger.warning(f"Driver mới có readyState bất thường: {document_ready}")
                close_current_driver(driver, gen, profile_id)
                raise WebDriverPoolError(f"Driver mới có readyState bất thường: {document_ready}")
        except Exception as e:
            self.logger.warning(f"Driver mới không ổn định sau navigation: {str(e)}")
            try:
                close_current_driver(driver, gen, profile_id)
            except:
                pass
            raise WebDriverPoolError(f"Driver mới không ổn định sau navigation: {str(e)}")

        return driver, gen, profile_id

    def return_driver(self, driver, gen, profile_id, priority=0):
        """
        Trả driver về pool để tái sử dụng.
        Cải tiến với kiểm tra execution context kỹ lưỡng và phân loại health score.
        
        Args:
            priority: Mức độ ưu tiên cho lần sử dụng tiếp theo (cao hơn được xử lý trước)
        """
        with self.lock:
            self.active_drivers = max(0, self.active_drivers - 1)
            
            if self.shutdown_flag:
                self.logger.debug(f"Đóng driver cho profile {profile_id} do pool đang shutdown")
                close_current_driver(driver, gen, profile_id)
                self.driver_counters["closed"] += 1
                return

            # Kiểm tra execution context trước khi trả về pool
            # Nếu không còn execution context, đóng driver và không trả về pool
            if not self._check_execution_context(driver, profile_id):
                self.logger.warning(f"Driver {profile_id} có lỗi execution context, không trả về pool")
                close_current_driver(driver, gen, profile_id)
                self.driver_counters["closed"] += 1
                return
                
            # Kiểm tra sức khỏe tổng quát của driver
            health_result, health_score = self._check_driver_health(driver, profile_id)
            if not health_result:
                self.logger.warning(f"Driver {profile_id} không đủ khỏe (score: {health_score}), không trả về pool")
                close_current_driver(driver, gen, profile_id)
                self.driver_counters["closed"] += 1
                return

            if len(self.pool) < self.max_size:
                # Xóa cookie và dữ liệu trình duyệt trước khi trả về pool
                try:
                    if not self.shutdown_flag:
                        # Điều hướng về about:blank trước
                        try:
                            driver.get("about:blank")
                        except Exception as e:
                            self.logger.warning(f"Không thể điều hướng về about:blank: {str(e)}")
                            # Nếu lỗi execution context, không trả về pool
                            if "execution context" in str(e).lower():
                                close_current_driver(driver, gen, profile_id)
                                self.driver_counters["closed"] += 1
                                return
                                
                        # Kiểm tra lại execution context sau navigation
                        if not self._check_execution_context(driver, profile_id):
                            self.logger.warning(f"Driver {profile_id} mất execution context sau navigation, không trả về pool")
                            close_current_driver(driver, gen, profile_id)
                            self.driver_counters["closed"] += 1
                            return
                            
                        # Xóa dữ liệu browsing
                        try:
                            clear_browsing_data(driver, self.config.domain_url_map, multi_tab=True, verify_connection=True)
                        except Exception as e:
                            self.logger.warning(f"Lỗi xóa dữ liệu trình duyệt: {str(e)}")
                            # Nếu lỗi execution context, không trả về pool
                            if "execution context" in str(e).lower():
                                close_current_driver(driver, gen, profile_id)
                                self.driver_counters["closed"] += 1
                                return
                            
                        # Kiểm tra lại execution context sau clear browsing data
                        if not self._check_execution_context(driver, profile_id):
                            self.logger.warning(f"Driver {profile_id} mất execution context sau xóa dữ liệu, không trả về pool")
                            close_current_driver(driver, gen, profile_id)
                            self.driver_counters["closed"] += 1
                            return
                        
                except Exception as e:
                    self.logger.warning(f"Lỗi xóa dữ liệu trình duyệt: {str(e)}. Đóng driver")
                    close_current_driver(driver, gen, profile_id)
                    self.driver_counters["closed"] += 1
                    return
                    
                # Nếu driver quá cũ, đánh dấu ưu tiên thấp hơn
                current_age = time.time() - self.last_execution_context_check.get(profile_id, 0)
                if current_age > self.max_driver_age:
                    priority = max(priority - 10, -10)  # Giảm priority nhưng giữ giới hạn -10
                    health_score = max(health_score - 20, 50)  # Giảm health score cho driver cũ
                    self.logger.debug(f"Driver {profile_id} quá cũ ({current_age/3600:.1f}h), giảm priority và health score")
                
                # Thêm driver vào pool với health score đã cập nhật
                self.pool.append((driver, gen, profile_id, time.time(), priority, health_score))
                self.logger.debug(f"Đã trả driver cho profile {profile_id} về pool (health: {health_score})")
            else:
                self.logger.debug(f"Pool đầy, đóng driver cho profile {profile_id}")
                close_current_driver(driver, gen, profile_id)
                self.driver_counters["closed"] += 1

    def get_metrics(self) -> Dict[str, Any]:
        """Lấy thông tin metric chi tiết của WebDriverPool"""
        with self.lock:
            execution_context_errors_detail = {
                k: v for k, v in self.error_history["execution_context"].items() 
                if v > 0
            }
            
            # Tính % tỷ lệ thành công/thất bại
            total_operations = (self.driver_counters["created"] + 
                               self.driver_counters["reused"] + 
                               self.driver_counters["failed"] + 
                               self.driver_counters["closed"])
            
            success_rate = 0
            if total_operations > 0:
                success_rate = ((self.driver_counters["created"] + self.driver_counters["reused"]) / 
                               total_operations) * 100
                
            return {
                "active_drivers": self.active_drivers,
                "pool_size": len(self.pool),
                "pool_health": [health for _, _, _, _, _, health in self.pool],
                "total_created": self.driver_counters["created"],
                "total_reused": self.driver_counters["reused"],
                "total_failed": self.driver_counters["failed"],
                "total_closed": self.driver_counters["closed"],
                "execution_context_errors": self.driver_counters["execution_context_errors"],
                "recovered": self.driver_counters["recovered"],
                "success_rate": success_rate,
                "error_history": {
                    "execution_context_count": sum(self.error_history["execution_context"].values()),
                    "no_such_window_count": sum(self.error_history["no_such_window"].values()),
                    "connection_errors_count": sum(self.error_history["connection_errors"].values()),
                    "other_errors_count": sum(self.error_history["other_errors"].values())
                },
                "execution_context_errors_detail": execution_context_errors_detail,
                "history": self.health_metrics[-10:] if self.health_metrics else []
            }

    def shutdown(self):
        """Đóng tất cả driver trong pool với cơ chế an toàn hơn."""
        self.logger.info("Đang shutdown WebDriver Pool")
        self.shutdown_flag = True

        # Đóng tất cả driver đang hoạt động
        with self.lock:
            for driver, gen, profile_id, _, _, _ in self.pool:
                try:
                    self.logger.debug(f"Đóng driver cho profile {profile_id}")
                    
                    # Sử dụng kỹ thuật đóng timeout thấp
                    try:
                        driver.set_page_load_timeout(1)
                        driver.set_script_timeout(1)
                    except:
                        pass
                    
                    # Thoát ra khỏi các trang hiện tại
                    try:
                        # Đây là trang nhẹ để đảm bảo không phải đợi load
                        driver.get("about:blank")
                    except:
                        pass
                        
                    # Đóng driver
                    close_current_driver(driver, gen, profile_id, force_quit=True)
                except Exception as e:
                    self.logger.warning(f"Lỗi đóng driver: {str(e)}")
            
            # Xóa toàn bộ pool
            self.pool = []
            
            # Xóa các cờ và dữ liệu cache
            self.last_execution_context_check = {}
            
        self.logger.info("Đã đóng tất cả driver trong pool")
        
        # Đợi health check loop kết thúc
        if hasattr(self, 'health_thread') and self.health_thread.is_alive():
            self.logger.debug("Đợi health check thread kết thúc...")
            try:
                self.health_thread.join(timeout=2)
            except:
                pass
            self.logger.debug("Health check thread đã kết thúc")
