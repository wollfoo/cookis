"""
config.py - Quản lý cấu hình dự án
- Đọc và xử lý file config.ini
- Cung cấp giá trị mặc định khi cần thiết
- Hỗ trợ caching để tránh đọc file nhiều lần
- Tối ưu các tham số để tránh lỗi "No Such Execution Context"
"""

import os
import configparser
import logging
from typing import Dict, Any, Optional
from collections import OrderedDict


class ConfigError(Exception):
    """Exception raised for errors in the configuration."""
    pass


class SimpleConfig:
    """
    Lớp cấu hình đọc từ file config.ini.
    Hỗ trợ các mục: WebDriver, Timeouts, General, Performance, Domains,
    CookieInjection, ClearData
    
    Cải tiến:
    - Caching để tránh đọc file nhiều lần
    - Kiểm tra tính hợp lệ của cấu hình
    - Tối ưu các tham số để tránh lỗi "No Such Execution Context"
    """
    # Cache để lưu trữ cấu hình đã đọc
    _config_cache = {}
    
    def __init__(self, ini_path: str):
        """
        Khởi tạo đối tượng cấu hình từ file INI.
        
        Args:
            ini_path: Đường dẫn đến file cấu hình .ini
        """
        self.ini_path = ini_path
        self.logger = logging.getLogger(__name__)
        self.last_modified_time = 0
        self.load_config()

    def load_config(self):
        """
        Đọc và phân tích file cấu hình INI với caching.
        Chỉ đọc lại nếu file đã thay đổi.
        """
        try:
            # Kiểm tra file có tồn tại không
            if not os.path.exists(self.ini_path):
                raise ConfigError(f"File cấu hình không tồn tại: {self.ini_path}")
            
            # Lấy thời gian sửa đổi cuối cùng
            current_mtime = os.path.getmtime(self.ini_path)
            
            # Nếu file không thay đổi và đã có trong cache, sử dụng cache
            cache_key = self.ini_path
            if (cache_key in SimpleConfig._config_cache and 
                current_mtime <= SimpleConfig._config_cache[cache_key]['mtime']):
                
                # Lấy cấu hình từ cache
                cached_config = SimpleConfig._config_cache[cache_key]['config']
                
                # Sao chép các thuộc tính từ cache
                for key, value in cached_config.items():
                    setattr(self, key, value)
                
                self.logger.debug(f"Đã sử dụng cấu hình từ cache cho {self.ini_path}")
                return
            
            self.logger.info(f"Đọc cấu hình từ file: {self.ini_path}")
            
            cp = configparser.ConfigParser(inline_comment_prefixes=('#', ';'))
            try:
                with open(self.ini_path, "r", encoding="utf-8", errors="replace") as f:
                    cp.read_file(f)
            except Exception as e:
                raise ConfigError(f"Lỗi đọc file cấu hình {self.ini_path}: {type(e).__name__}: {str(e)}") from e

            # WebDriver settings
            self.chrome_driver_path = cp.get("WebDriver", "driver_path", fallback="chromedriver.exe")

            # Timeout settings - Tối ưu để tránh lỗi "No Such Execution Context"
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
                # Thêm các timeout mới để tối ưu
                "execution_context_check": cp.getint("Timeouts", "execution_context_check", fallback=5),
                "driver_health_check": cp.getint("Timeouts", "driver_health_check", fallback=10),
                "script_timeout": cp.getint("Timeouts", "script_timeout", fallback=10),
            }

            # General settings
            self.max_retries = cp.getint("General", "max_retries", fallback=3)
            self.login_check_url = cp.get("General", "login_check_url", fallback="https://portal.azure.com")
            self.force_stop_running = cp.getboolean("General", "force_stop_running", fallback=True)
            self.strict_validation = cp.getboolean("General", "strict_validation", fallback=True)

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
                # Thêm cấu hình mới cho execution context
                self.max_execution_context_errors = cp.getint("Performance", "max_execution_context_errors", fallback=10)
                self.min_pool_size = cp.getint("Performance", "min_pool_size", fallback=1)
                self.max_driver_age = cp.getint("Performance", "max_driver_age", fallback=3600)
            else:
                cpu_count = os.cpu_count() or 2
                self.max_workers = min(cpu_count, 4)
                self.driver_pool_size = 3
                self.max_memory_percent = 80
                self.max_cpu_percent = 70
                self.batch_size = 50
                self.max_execution_context_errors = 10
                self.min_pool_size = 1
                self.max_driver_age = 3600

            # WebDriverPool advanced settings
            if cp.has_section("WebDriverPool"):
                self.auto_replenish = cp.getboolean("WebDriverPool", "auto_replenish", fallback=True)
                self.check_interval = cp.getint("WebDriverPool", "check_interval", fallback=30)
                self.warm_pool = cp.getboolean("WebDriverPool", "warm_pool", fallback=True)
                self.execution_context_check_interval = cp.getint("WebDriverPool", "execution_context_check_interval", fallback=5)
            else:
                self.auto_replenish = True
                self.check_interval = 30
                self.warm_pool = True
                self.execution_context_check_interval = 5

            # Domain mappings
            if cp.has_section("Domains"):
                self.domain_url_map = OrderedDict(cp.items("Domains"))
            else:
                self.domain_url_map = OrderedDict()
                
            # Debug settings
            if cp.has_section("Debug"):
                self.debug_mode = cp.getboolean("Debug", "enabled", fallback=False)
                self.verbose_logging = cp.getboolean("Debug", "verbose_logging", fallback=False)
                self.save_artifacts = cp.getboolean("Debug", "save_artifacts", fallback=False)
            else:
                self.debug_mode = False
                self.verbose_logging = False
                self.save_artifacts = False
            
            # Lưu lại thời gian sửa đổi cuối cùng
            self.last_modified_time = current_mtime
            
            # Lưu vào cache
            SimpleConfig._config_cache[cache_key] = {
                'mtime': current_mtime,
                'config': self._get_all_attributes()
            }
            
            # Kiểm tra tính hợp lệ của cấu hình
            self._validate_config()
            
            self.logger.debug(f"Đã tải cấu hình từ file {self.ini_path} thành công")
            
        except ConfigError as e:
            self.logger.error(f"Lỗi cấu hình: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Lỗi không mong đợi khi tải cấu hình: {type(e).__name__}: {str(e)}")
            raise ConfigError(f"Lỗi tải cấu hình: {str(e)}") from e

    def _get_all_attributes(self) -> Dict[str, Any]:
        """
        Lấy tất cả thuộc tính của đối tượng cấu hình.
        
        Returns:
            Dict[str, Any]: Dictionary chứa tất cả thuộc tính
        """
        return {key: value for key, value in self.__dict__.items() 
                if not key.startswith('_') and key not in ['ini_path', 'logger', 'last_modified_time']}

    def _validate_config(self):
        """
        Kiểm tra tính hợp lệ của cấu hình.
        Đảm bảo các giá trị nằm trong phạm vi hợp lý.
        """
        # Kiểm tra chrome_driver_path
        if not os.path.exists(self.chrome_driver_path) and not self.chrome_driver_path.endswith('.exe'):
            self.logger.warning(f"ChromeDriver không tồn tại tại đường dẫn: {self.chrome_driver_path}")
        
        # Kiểm tra và điều chỉnh timeouts
        for key, value in self.timeouts.items():
            if value <= 0:
                self.logger.warning(f"Timeout {key} có giá trị không hợp lệ ({value}), điều chỉnh về giá trị mặc định")
                if key == "page_load":
                    self.timeouts[key] = 30
                elif key == "element_wait":
                    self.timeouts[key] = 10
                else:
                    self.timeouts[key] = 5
        
        # Kiểm tra max_workers
        cpu_count = os.cpu_count() or 2
        if self.max_workers <= 0:
            self.logger.warning(f"max_workers có giá trị không hợp lệ ({self.max_workers}), điều chỉnh về 1")
            self.max_workers = 1
        elif self.max_workers > cpu_count * 2:
            self.logger.warning(f"max_workers quá cao ({self.max_workers}) so với số CPU ({cpu_count}), có thể gây quá tải")
        
        # Kiểm tra driver_pool_size
        if self.driver_pool_size <= 0:
            self.logger.warning(f"driver_pool_size có giá trị không hợp lệ ({self.driver_pool_size}), điều chỉnh về 1")
            self.driver_pool_size = 1
        elif self.driver_pool_size > self.max_workers * 2:
            self.logger.warning(f"driver_pool_size ({self.driver_pool_size}) quá cao so với max_workers ({self.max_workers})")
        
        # Kiểm tra domain_url_map
        if not self.domain_url_map:
            self.logger.warning("Không có domain nào được cấu hình trong mục [Domains]")

    def get_timeout(self, key: str, fallback: Optional[int] = None) -> int:
        """
        Lấy giá trị timeout theo key.
        
        Args:
            key: Tên của timeout cần lấy
            fallback: Giá trị mặc định nếu không tìm thấy timeout
            
        Returns:
            int: Giá trị timeout
        """
        return self.timeouts.get(key, fallback if fallback is not None else 0)

    def reload_if_changed(self) -> bool:
        """
        Tải lại cấu hình nếu file đã thay đổi.
        
        Returns:
            bool: True nếu đã tải lại, False nếu không thay đổi
        """
        try:
            if not os.path.exists(self.ini_path):
                self.logger.warning(f"File cấu hình không tồn tại: {self.ini_path}")
                return False
                
            current_mtime = os.path.getmtime(self.ini_path)
            if current_mtime > self.last_modified_time:
                self.logger.info(f"File cấu hình {self.ini_path} đã thay đổi, tải lại")
                self.load_config()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Lỗi khi kiểm tra thay đổi file cấu hình: {str(e)}")
            return False

    @property
    def as_dict(self) -> Dict[str, Any]:
        """
        Trả về cấu hình dưới dạng dict để dễ serialize
        
        Returns:
            Dict[str, Any]: Dictionary chứa thông tin cấu hình
        """
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
            },
            "performance": {
                "max_memory_percent": self.max_memory_percent,
                "max_cpu_percent": self.max_cpu_percent
            },
            "driver_pool": {
                "auto_replenish": self.auto_replenish,
                "check_interval": self.check_interval,
                "warm_pool": self.warm_pool,
                "execution_context_check_interval": self.execution_context_check_interval
            },
            "validation": {
                "strict_validation": self.strict_validation
            }
        }
        
    def get_optimized_execution_context_settings(self) -> Dict[str, Any]:
        """
        Trả về các thiết lập tối ưu để tránh lỗi "No Such Execution Context".
        
        Returns:
            Dict[str, Any]: Các thiết lập được tối ưu
        """
        return {
            "execution_context_check_interval": self.execution_context_check_interval,
            "max_execution_context_errors": self.max_execution_context_errors,
            "driver_health_check_interval": self.check_interval,
            "script_timeout": self.timeouts.get("script_timeout", 10),
            "page_load_timeout": self.timeouts.get("page_load", 30),
            "auto_replenish": self.auto_replenish,
            "min_pool_size": self.min_pool_size,
            "max_driver_age": self.max_driver_age
        }
