"""
driver_init.py - Module khởi tạo và quản lý ChromeDriver
Version: 2.0.0

Chức năng chính:
- Kiểm tra và chuẩn hóa đường dẫn đến ChromeDriver
- Tạo ChromeDriver instance với các tùy chọn tối ưu
- Hỗ trợ khởi tạo driver tạm thời cho các tác vụ đơn giản
- Quản lý hiệu quả ChromeOptions và Service cho nhiều môi trường
- Cấu trúc hóa mã nguồn theo mô hình OOP để dễ bảo trì và mở rộng
"""

import os
import time
import platform
import logging
import subprocess
import socket
from pathlib import Path
from typing import Optional, Union, Dict, List, Any, Tuple
from dataclasses import dataclass, field

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    WebDriverException, SessionNotCreatedException, InvalidArgumentException
)

# Thiết lập logger
logger = logging.getLogger(__name__)

# -------------------------------
# Exceptions
# -------------------------------
class DriverInitError(Exception):
    """Ngoại lệ khi khởi tạo driver thất bại."""
    pass

# -------------------------------
# Configuration Classes
# -------------------------------
@dataclass
class TimeoutConfig:
    """Lớp cấu hình tập trung các thiết lập timeout."""
    page_load: int = 30
    script: int = 30
    implicit_wait: int = 10
    connection: int = 10
    
    def as_dict(self) -> Dict[str, int]:
        """Trả về tất cả cấu hình dưới dạng dict."""
        return {
            'page_load': self.page_load,
            'script': self.script,
            'implicit_wait': self.implicit_wait,
            'connection': self.connection
        }

@dataclass
class ChromeOptionsConfig:
    """Lớp cấu hình các tùy chọn cho trình duyệt Chrome."""
    headless: bool = False
    proxy: Optional[str] = None
    user_agent: Optional[str] = None
    disable_extensions: bool = True
    incognito: bool = False
    start_maximized: bool = False
    disable_images: bool = False
    disable_javascript: bool = False
    disable_gpu: bool = True
    no_sandbox: bool = True
    disable_dev_shm: bool = True
    disable_infobars: bool = True
    disable_notifications: bool = True
    additional_arguments: Optional[List[str]] = None
    experimental_options: Optional[Dict[str, Any]] = None
    debugger_address: Optional[str] = None
    
    def as_dict(self) -> Dict[str, Any]:
        """Trả về tất cả cấu hình dưới dạng dict."""
        return {
            'headless': self.headless,
            'proxy': self.proxy,
            'user_agent': self.user_agent,
            'disable_extensions': self.disable_extensions,
            'incognito': self.incognito,
            'start_maximized': self.start_maximized,
            'disable_images': self.disable_images,
            'disable_javascript': self.disable_javascript,
            'disable_gpu': self.disable_gpu,
            'no_sandbox': self.no_sandbox,
            'disable_dev_shm': self.disable_dev_shm,
            'disable_infobars': self.disable_infobars,
            'disable_notifications': self.disable_notifications,
            'additional_arguments': self.additional_arguments,
            'experimental_options': self.experimental_options,
            'debugger_address': self.debugger_address
        }

@dataclass
class ServiceConfig:
    """Lớp cấu hình cho ChromeDriver Service."""
    port: Optional[int] = None
    verbose: bool = False
    log_path: Optional[Union[str, Path]] = None
    env: Optional[Dict[str, str]] = None
    
    def as_dict(self) -> Dict[str, Any]:
        """Trả về tất cả cấu hình dưới dạng dict."""
        return {
            'port': self.port,
            'verbose': self.verbose,
            'log_path': self.log_path,
            'env': self.env
        }

@dataclass
class DriverConfig:
    """Lớp cấu hình tổng hợp cho ChromeDriver."""
    chrome_driver_path: Union[str, Path]
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    chrome_options: ChromeOptionsConfig = field(default_factory=ChromeOptionsConfig)
    service: ServiceConfig = field(default_factory=ServiceConfig)
    disable_cache: bool = True
    window_size: str = "1920,1080"
    
    def as_dict(self) -> Dict[str, Any]:
        """Trả về tất cả cấu hình dưới dạng dict."""
        return {
            'chrome_driver_path': str(self.chrome_driver_path),
            'timeouts': self.timeouts.as_dict(),
            'chrome_options': self.chrome_options.as_dict(),
            'service': self.service.as_dict(),
            'disable_cache': self.disable_cache,
            'window_size': self.window_size
        }

# -------------------------------
# Constants - Giữ lại để tương thích ngược
# -------------------------------
DEFAULT_PAGE_LOAD_TIMEOUT = 30
DEFAULT_SCRIPT_TIMEOUT = 30
DEFAULT_IMPLICIT_WAIT = 10

# -------------------------------
# Utility Functions
# -------------------------------
def check_driver_path(driver_path: Union[str, Path]) -> str:
    """
    Kiểm tra và chuẩn hóa đường dẫn đến ChromeDriver. Nếu không tìm thấy theo đường dẫn tĩnh,
    sẽ tìm kiếm trong thư mục hiện tại và trong PATH.
    
    Args:
        driver_path (Union[str, Path]): Đường dẫn tới ChromeDriver
        
    Returns:
        str: Đường dẫn đầy đủ đến ChromeDriver đã được xác minh
        
    Raises:
        DriverInitError: Nếu không tìm thấy ChromeDriver
    """
    path = Path(driver_path)
    if path.exists():
        return str(path.resolve())

    current_dir = Path.cwd()
    system = platform.system().lower()
    if system == "windows":
        driver_names = ["chromedriver.exe"]
    elif system in ["linux", "darwin"]:
        driver_names = ["chromedriver"]
    else:
        driver_names = ["chromedriver", "chromedriver.exe"]

    # Nếu tên file được cung cấp, ưu tiên dùng nó
    if path.name:
        driver_names.insert(0, path.name)

    for name in driver_names:
        possible_path = current_dir / name
        if possible_path.exists():
            logger.info(f"Đã tìm thấy ChromeDriver tại: {possible_path}")
            return str(possible_path.resolve())

    # Tìm trong PATH hệ thống
    for name in driver_names:
        if system == "windows":
            result = subprocess.run(["where", name], capture_output=True, text=True)
        else:
            result = subprocess.run(["which", name], capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            found_path = result.stdout.strip().splitlines()[0]
            logger.info(f"Đã tìm thấy ChromeDriver trong PATH: {found_path}")
            return found_path

    raise DriverInitError(f"ChromeDriver không tồn tại: {driver_path}. Đã tìm kiếm: {', '.join(driver_names)}")

def check_port(host: str = "localhost", port: int = 9222, timeout: float = 1.0) -> Tuple[bool, int]:
    """
    Kiểm tra xem cổng có khả dụng hay không và trạng thái kết nối.
    
    Args:
        host (str): Hostname để kiểm tra, mặc định là "localhost"
        port (int): Số cổng cần kiểm tra
        timeout (float): Thời gian timeout cho kết nối (giây)
        
    Returns:
        Tuple[bool, int]: (True nếu cổng khả dụng, mã kết quả kết nối)
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            result = s.connect_ex((host, port))
            return (result != 0), result  # Nếu kết quả khác 0, cổng không được sử dụng (khả dụng)
    except Exception as e:
        logger.debug(f"Lỗi khi kiểm tra cổng {port}: {str(e)}")
        return False, -1  # Giả định cổng đang được sử dụng nếu có lỗi

def find_available_port(start_port: int = 9222, max_attempts: int = 100) -> int:
    """
    Tìm cổng khả dụng, bắt đầu từ start_port.
    
    Args:
        start_port (int): Cổng bắt đầu kiểm tra
        max_attempts (int): Số lần thử tối đa
        
    Returns:
        int: Cổng khả dụng đầu tiên
        
    Raises:
        DriverInitError: Nếu không tìm thấy cổng khả dụng sau số lần thử tối đa
    """
    current_port = start_port
    
    for _ in range(max_attempts):
        is_available, _ = check_port(port=current_port)
        if is_available:
            return current_port
        current_port += 1
    
    raise DriverInitError(f"Không tìm thấy cổng khả dụng sau {max_attempts} lần thử (từ {start_port} đến {current_port-1})")

def get_chrome_version(driver: webdriver.Chrome) -> str:
    """
    Lấy phiên bản Chrome đang chạy qua CDP.
    
    Args:
        driver (webdriver.Chrome): Instance WebDriver đang chạy
        
    Returns:
        str: Chuỗi chứa phiên bản Chrome
    """
    try:
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
        logger.debug(f"Lỗi khi lấy phiên bản Chrome: {str(e)}")
        return "unknown (error)"

def handle_driver_exception(exception: Exception) -> str:
    """
    Xử lý các ngoại lệ khi khởi tạo driver và trả về thông báo lỗi phù hợp.
    
    Args:
        exception: Ngoại lệ cần xử lý
        
    Returns:
        str: Thông báo lỗi
    """
    if isinstance(exception, SessionNotCreatedException):
        error_msg = f"Không thể tạo phiên ChromeDriver: {str(exception)}"
        # Kiểm tra lỗi phiên bản
        if "This version of ChromeDriver only supports Chrome version" in str(exception):
            logger.error("Phiên bản ChromeDriver không tương thích với phiên bản Chrome. Vui lòng cập nhật.")
        return error_msg
    
    elif isinstance(exception, InvalidArgumentException):
        return f"Tham số không hợp lệ khi khởi tạo ChromeDriver: {str(exception)}"
    
    elif isinstance(exception, WebDriverException):
        return f"Lỗi WebDriver khi khởi tạo: {str(exception)}"
    
    else:
        return f"Lỗi không xác định khi khởi tạo ChromeDriver: {type(exception).__name__}: {str(exception)}"

# -------------------------------
# Core Functions
# -------------------------------
def create_chrome_options(config: ChromeOptionsConfig) -> Options:
    """
    Tạo Options cho ChromeDriver với cấu hình linh hoạt.
    
    Args:
        config (ChromeOptionsConfig): Cấu hình cho Chrome options
        
    Returns:
        Options: Đối tượng chrome options đã được cấu hình
    """
    options = Options()
    
    # Thêm các tham số cơ bản
    if config.headless:
        options.add_argument("--headless=new")  # new headless mode for Chrome >= 109
    
    if config.disable_gpu:
        options.add_argument("--disable-gpu")
    
    if config.no_sandbox:
        options.add_argument("--no-sandbox")
    
    if config.disable_dev_shm:
        options.add_argument("--disable-dev-shm-usage")
    
    if config.disable_extensions:
        options.add_argument("--disable-extensions")
    
    if config.incognito:
        options.add_argument("--incognito")
    
    if config.start_maximized:
        options.add_argument("--start-maximized")
    
    if config.disable_infobars:
        options.add_argument("--disable-infobars")
    
    if config.disable_notifications:
        options.add_argument("--disable-notifications")
    
    # Các tùy chọn nâng cao
    if config.disable_images:
        options.add_argument("--blink-settings=imagesEnabled=false")
    
    if config.disable_javascript:
        options.add_argument("--disable-javascript")
    
    # Thiết lập proxy nếu được cung cấp
    if config.proxy:
        options.add_argument(f"--proxy-server={config.proxy}")
    
    # Thiết lập User-Agent tùy chỉnh
    if config.user_agent:
        options.add_argument(f"--user-agent={config.user_agent}")
    
    # Thêm các tham số bổ sung
    if config.additional_arguments:
        for arg in config.additional_arguments:
            options.add_argument(arg)
    
    # Thêm các tùy chọn thử nghiệm
    if config.experimental_options:
        for key, value in config.experimental_options.items():
            options.add_experimental_option(key, value)
    
    # Kết nối với trình duyệt đang chạy thông qua debugger address
    if config.debugger_address:
        options.add_experimental_option("debuggerAddress", config.debugger_address)
    
    # Thêm các tùy chọn hiệu suất cao
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-breakpad")  # Tắt crash reporter
    
    # Tắt log động cơ Chrome
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    return options

def create_chrome_service(driver_path: Union[str, Path], config: ServiceConfig) -> Service:
    """
    Tạo Service cho ChromeDriver với nhiều tùy chọn.
    
    Args:
        driver_path (Union[str, Path]): Đường dẫn đến ChromeDriver
        config (ServiceConfig): Cấu hình cho Chrome service
        
    Returns:
        Service: Service đã được cấu hình cho ChromeDriver
        
    Raises:
        DriverInitError: Nếu đường dẫn ChromeDriver không hợp lệ
    """
    try:
        # Kiểm tra và chuẩn hóa đường dẫn ChromeDriver
        driver_path = check_driver_path(driver_path)
        logger.debug(f"Sử dụng ChromeDriver tại: {driver_path}")
        
        # Tìm cổng khả dụng nếu không được chỉ định
        port = config.port
        if port is None:
            port = find_available_port()
        
        # Các tùy chọn service
        service_kwargs = {
            'executable_path': driver_path,
            'port': port
        }
        
        if config.verbose:
            service_kwargs['service_args'] = ['--verbose']
        
        if config.log_path:
            log_path_str = str(config.log_path)
            service_kwargs['log_path'] = log_path_str
            
            # Đảm bảo thư mục chứa file log tồn tại
            log_dir = os.path.dirname(log_path_str)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
        
        if config.env:
            service_kwargs['env'] = config.env
        
        # Tạo và trả về Service
        service = Service(**service_kwargs)
        logger.debug(f"Đã tạo ChromeDriver Service trên cổng {port}")
        return service
        
    except Exception as e:
        error_msg = f"Lỗi khi tạo ChromeDriver Service: {type(e).__name__}: {str(e)}"
        logger.error(error_msg)
        raise DriverInitError(error_msg)

def create_driver(config: DriverConfig) -> webdriver.Chrome:
    """
    Tạo một WebDriver với cấu hình toàn diện.
    
    Args:
        config (DriverConfig): Cấu hình đầy đủ cho driver
        
    Returns:
        webdriver.Chrome: Instance ChromeDriver đã được cấu hình
        
    Raises:
        DriverInitError: Nếu không thể khởi tạo driver
    """
    try:
        # Kiểm tra và chuẩn hóa đường dẫn ChromeDriver
        driver_path = check_driver_path(config.chrome_driver_path)
        logger.debug(f"Sử dụng ChromeDriver tại: {driver_path}")
        
        # Tạo service
        service = create_chrome_service(driver_path, config.service)
        
        # Tạo options
        options = create_chrome_options(config.chrome_options)
        
        # Thêm kích thước cửa sổ nếu không start_maximized
        if config.window_size and not config.chrome_options.start_maximized:
            options.add_argument(f"--window-size={config.window_size}")
        
        # Khởi tạo driver
        logger.debug("Đang khởi tạo ChromeDriver...")
        driver = webdriver.Chrome(service=service, options=options)
        
        # Thiết lập timeouts
        driver.set_page_load_timeout(config.timeouts.page_load)
        driver.set_script_timeout(config.timeouts.script)
        driver.implicitly_wait(config.timeouts.implicit_wait)
        
        logger.info("Đã khởi tạo ChromeDriver thành công")
        return driver
        
    except Exception as e:
        error_msg = handle_driver_exception(e)
        logger.error(error_msg)
        raise DriverInitError(error_msg)

# -------------------------------
# ChromeDriverManager Class
# -------------------------------
class ChromeDriverManager:
    """
    Lớp quản lý việc khởi tạo và cấu hình ChromeDriver.
    """
    
    def __init__(self, config: Optional[DriverConfig] = None, chrome_driver_path: Optional[Union[str, Path]] = None):
        """
        Khởi tạo ChromeDriverManager.
        
        Args:
            config (Optional[DriverConfig]): Cấu hình đầy đủ cho driver
            chrome_driver_path (Optional[Union[str, Path]]): Đường dẫn đến ChromeDriver 
                (được sử dụng nếu config = None)
        """
        if config is None:
            if chrome_driver_path is None:
                raise ValueError("Phải cung cấp config hoặc chrome_driver_path")
            self.config = DriverConfig(chrome_driver_path=chrome_driver_path)
        else:
            self.config = config
    
    def create_driver(self) -> webdriver.Chrome:
        """
        Tạo một instance ChromeDriver sử dụng cấu hình của manager.
        
        Returns:
            webdriver.Chrome: Instance ChromeDriver đã được cấu hình
            
        Raises:
            DriverInitError: Nếu không thể khởi tạo driver
        """
        return create_driver(self.config)
    
    def create_options(self) -> Options:
        """
        Tạo Chrome options sử dụng cấu hình của manager.
        
        Returns:
            Options: Chrome options đã được cấu hình
        """
        return create_chrome_options(self.config.chrome_options)
    
    def create_service(self) -> Service:
        """
        Tạo Chrome service sử dụng cấu hình của manager.
        
        Returns:
            Service: Chrome service đã được cấu hình
            
        Raises:
            DriverInitError: Nếu không thể tạo service
        """
        return create_chrome_service(self.config.chrome_driver_path, self.config.service)
    
    def update_config(self, new_config: DriverConfig) -> None:
        """
        Cập nhật cấu hình của manager.
        
        Args:
            new_config (DriverConfig): Cấu hình mới cho driver
        """
        self.config = new_config
    
    def update_timeouts(self, page_load: Optional[int] = None, script: Optional[int] = None, 
                        implicit_wait: Optional[int] = None, connection: Optional[int] = None) -> None:
        """
        Cập nhật cấu hình timeout của manager.
        
        Args:
            page_load (Optional[int]): Thời gian chờ tải trang (giây)
            script (Optional[int]): Thời gian chờ thực thi script (giây)
            implicit_wait (Optional[int]): Thời gian chờ implicit (giây)
            connection (Optional[int]): Thời gian chờ kết nối (giây)
        """
        if page_load is not None:
            self.config.timeouts.page_load = page_load
        if script is not None:
            self.config.timeouts.script = script
        if implicit_wait is not None:
            self.config.timeouts.implicit_wait = implicit_wait
        if connection is not None:
            self.config.timeouts.connection = connection

# -------------------------------
# Backwards Compatibility Functions
# -------------------------------
def create_chrome_options_compat(
    headless: bool = False,
    proxy: Optional[str] = None,
    user_agent: Optional[str] = None,
    disable_extensions: bool = True,
    incognito: bool = False,
    start_maximized: bool = False,
    disable_images: bool = False,
    disable_javascript: bool = False,
    disable_gpu: bool = True,
    no_sandbox: bool = True,
    disable_dev_shm: bool = True,
    disable_infobars: bool = True,
    disable_notifications: bool = True,
    additional_arguments: Optional[List[str]] = None,
    experimental_options: Optional[Dict[str, Any]] = None,
    debugger_address: Optional[str] = None
    ) -> Options:
    """
    Tạo Options cho ChromeDriver với các tùy chọn linh hoạt (phiên bản tương thích ngược).
    
    Args:
        Tham số giống như hàm create_chrome_options gốc
        
    Returns:
        Options: Đối tượng chrome options đã được cấu hình
    """
    config = ChromeOptionsConfig(
        headless=headless,
        proxy=proxy,
        user_agent=user_agent,
        disable_extensions=disable_extensions,
        incognito=incognito,
        start_maximized=start_maximized,
        disable_images=disable_images,
        disable_javascript=disable_javascript,
        disable_gpu=disable_gpu,
        no_sandbox=no_sandbox,
        disable_dev_shm=disable_dev_shm,
        disable_infobars=disable_infobars,
        disable_notifications=disable_notifications,
        additional_arguments=additional_arguments,
        experimental_options=experimental_options,
        debugger_address=debugger_address
    )
    
    return create_chrome_options(config)

def create_chrome_service_compat(
    driver_path: Union[str, Path],
    port: Optional[int] = None,
    verbose: bool = False,
    log_path: Optional[Union[str, Path]] = None,
    env: Optional[Dict[str, str]] = None
) -> Service:
    """
    Tạo Service cho ChromeDriver với nhiều tùy chọn (phiên bản tương thích ngược).
    
    Args:
        Tham số giống như hàm create_chrome_service gốc
        
    Returns:
        Service: Service đã được cấu hình cho ChromeDriver
        
    Raises:
        DriverInitError: Nếu đường dẫn ChromeDriver không hợp lệ
    """
    config = ServiceConfig(
        port=port,
        verbose=verbose,
        log_path=log_path,
        env=env
    )
    
    return create_chrome_service(driver_path, config)

def create_temp_driver(
    chrome_driver_path: Union[str, Path],
    headless: bool = True,
    page_load_timeout: int = DEFAULT_PAGE_LOAD_TIMEOUT,
    script_timeout: int = DEFAULT_SCRIPT_TIMEOUT,
    implicit_wait: int = DEFAULT_IMPLICIT_WAIT,
    options_config: Optional[Dict[str, Any]] = None,
    custom_options: Optional[Options] = None,
    proxy: Optional[str] = None,
    user_agent: Optional[str] = None,
    window_size: str = "1920,1080"
    ) -> webdriver.Chrome:
    """
    Tạo một WebDriver tạm thời (không sử dụng GenLogin) với nhiều tùy chọn.
    
    Args:
        chrome_driver_path (Union[str, Path]): Đường dẫn đến ChromeDriver
        headless (bool): Chạy Chrome ở chế độ headless (không giao diện)
        page_load_timeout (int): Timeout cho việc tải trang (giây)
        script_timeout (int): Timeout cho việc thực thi script (giây)
        implicit_wait (int): Thời gian chờ implicit (giây)
        options_config (Optional[Dict[str, Any]]): Cấu hình cho create_chrome_options
        custom_options (Optional[Options]): Đối tượng Options tùy chỉnh
        proxy (Optional[str]): Chuỗi proxy server
        user_agent (Optional[str]): User-Agent tùy chỉnh
        window_size (str): Kích thước cửa sổ trình duyệt (WxH format)
        
    Returns:
        webdriver.Chrome: Instance ChromeDriver đã được cấu hình
        
    Raises:
        DriverInitError: Nếu không thể khởi tạo driver
    """
    try:
        # Tạo cấu hình timeout
        timeout_config = TimeoutConfig(
            page_load=page_load_timeout,
            script=script_timeout,
            implicit_wait=implicit_wait
        )
        
        # Xử lý trường hợp có custom_options
        if custom_options:
            # Kiểm tra và chuẩn hóa đường dẫn ChromeDriver
            driver_path = check_driver_path(chrome_driver_path)
            logger.debug(f"Sử dụng ChromeDriver tại: {driver_path}")
            
            # Tạo service
            service = Service(executable_path=driver_path)
            
            # Đảm bảo headless được thiết lập nếu cần
            if headless and "--headless" not in str(custom_options.arguments):
                custom_options.add_argument("--headless=new")
            
            # Khởi tạo driver
            logger.debug("Khởi tạo ChromeDriver với custom options...")
            driver = webdriver.Chrome(service=service, options=custom_options)
            
            # Thiết lập timeouts
            driver.set_page_load_timeout(page_load_timeout)
            driver.set_script_timeout(script_timeout)
            driver.implicitly_wait(implicit_wait)
            
            logger.info("Đã khởi tạo ChromeDriver tạm thời thành công")
            return driver
        else:
            # Tạo cấu hình chrome options từ tham số
            chrome_options_config = ChromeOptionsConfig(
                headless=headless,
                proxy=proxy,
                user_agent=user_agent
            )
            
            # Gộp với options_config nếu được cung cấp
            if options_config:
                for key, value in options_config.items():
                    if hasattr(chrome_options_config, key):
                        setattr(chrome_options_config, key, value)
            
            # Tạo cấu hình driver đầy đủ
            driver_config = DriverConfig(
                chrome_driver_path=chrome_driver_path,
                timeouts=timeout_config,
                chrome_options=chrome_options_config,
                window_size=window_size
            )
            
            # Tạo và trả về driver
            return create_driver(driver_config)
    except Exception as e:
        error_msg = handle_driver_exception(e)
        logger.error(error_msg)
        raise DriverInitError(error_msg)

# -------------------------------
# Export Original Function Names (for backward compatibility)
# -------------------------------
# Sử dụng các phiên bản tương thích với tên hàm gốc
create_chrome_options = create_chrome_options_compat
create_chrome_service = create_chrome_service_compat

# -------------------------------
# End of Module
# -------------------------------