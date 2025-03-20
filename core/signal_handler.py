"""
signal_handler.py - Xử lý tín hiệu hệ thống

Module chuyên trách xử lý các tín hiệu hệ thống giúp chương trình thoát an toàn
và giải phóng tài nguyên khi cần thiết. Đặc biệt tập trung vào việc xử lý
các tín hiệu ngắt như SIGINT (Ctrl+C), SIGTERM và đảm bảo đóng đúng cách
các tiến trình Chrome/ChromeDriver.

Các chức năng chính:
- Quản lý tín hiệu hệ thống (SIGINT, SIGTERM, SIGBREAK)
- Thoát an toàn và dọn dẹp tài nguyên
- Thoát khẩn cấp khi phát hiện nhiều lần ngắt liên tiếp
- Hệ thống đăng ký nhiều hàm dọn dẹp (cleanup handlers)
- Đóng các tiến trình Chrome và ChromeDriver đang chạy
"""

import os
import sys
import time
import signal
import logging
import psutil
import platform
import traceback
import threading
from typing import Callable, Optional, List, Dict, Any, Set

# Import từ global_state
from global_state import request_shutdown, is_shutdown_requested

# --- Trạng thái xử lý tín hiệu ---
last_interrupt_time = 0
emergency_exit_in_progress = False
_registered_cleanup_handlers = []
_logger = logging.getLogger(__name__)

def register_cleanup_handler(handler: Callable, priority: int = 0) -> None:
    """
    Đăng ký hàm dọn dẹp tài nguyên sẽ được gọi khi thoát.
    
    Args:
        handler: Hàm dọn dẹp tài nguyên
        priority: Mức độ ưu tiên (cao hơn được gọi trước)
    """
    global _registered_cleanup_handlers
    
    # Cấu trúc (priority, handler, id)
    handler_info = (priority, handler, id(handler))
    
    # Kiểm tra xem handler đã tồn tại chưa
    if not any(h[2] == id(handler) for h in _registered_cleanup_handlers):
        _registered_cleanup_handlers.append(handler_info)
        # Sắp xếp theo thứ tự ưu tiên giảm dần
        _registered_cleanup_handlers.sort(reverse=True)
        _logger.debug(f"Đã đăng ký cleanup handler: {handler.__name__} (priority={priority})")

def setup_signal_handlers(cleanup_fn: Optional[Callable] = None) -> None:
    """
    Thiết lập xử lý tín hiệu để thoát an toàn.

    Args:
        cleanup_fn: Hàm dọn dẹp tài nguyên khi thoát (optional)
    """
    global last_interrupt_time, emergency_exit_in_progress
    
    if cleanup_fn:
        register_cleanup_handler(cleanup_fn, priority=100)  # Ưu tiên cao nhất
    
    def signal_handler(sig, frame):
        global last_interrupt_time, emergency_exit_in_progress
        current_time = time.time()
        
        # Nếu đã đang trong quá trình exit, bỏ qua
        if emergency_exit_in_progress:
            return
            
        # Kiểm tra xem đây có phải là Ctrl+C lần thứ hai trong khoảng thời gian ngắn
        if current_time - last_interrupt_time < 3:
            print("\n\nPhát hiện Ctrl+C lần thứ hai - THOÁT KHẨN CẤP!")
            emergency_exit(sig, frame)
            return
        
        last_interrupt_time = current_time
        print("\n\nĐã nhận tín hiệu dừng. Đang dọn dẹp tài nguyên...")
        _logger.warning("Đã nhận tín hiệu dừng. Bắt đầu quá trình thoát an toàn.")
        
        # Đặt cờ toàn cục
        request_shutdown()
        
        # Đóng các process liên quan đến Chrome mà không cần đợi quá lâu
        kill_chrome_processes(emergency=False)

        # Gọi tất cả các hàm dọn dẹp đã đăng ký
        run_all_cleanup_handlers()
        
        print("Thoát chương trình.")
        sys.exit(0)
    
    # Đăng ký xử lý tín hiệu
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGBREAK'):  # Windows only
        signal.signal(signal.SIGBREAK, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    _logger.info("Đã thiết lập xử lý tín hiệu hệ thống")

def emergency_exit(sig: int, frame) -> None:
    """
    Xử lý thoát khẩn cấp khi người dùng nhấn Ctrl+C nhiều lần.

    Args:
        sig: Tín hiệu nhận được
        frame: Frame hiện tại
    """
    global emergency_exit_in_progress
    
    # Nếu đã đang trong quá trình thoát khẩn cấp, thoát ngay lập tức
    if emergency_exit_in_progress:
        os._exit(1)
    
    emergency_exit_in_progress = True
    print("\n\nĐang thoát khẩn cấp...")
    _logger.warning("Thoát khẩn cấp được kích hoạt!")
    
    # Đặt cờ toàn cục
    request_shutdown()
    
    # Giảm tất cả timeout xuống giá trị cực thấp
    try:
        import socket
        socket.setdefaulttimeout(0.1)  # 100ms timeout
        _logger.debug("Đã giảm socket timeout xuống 100ms")
    except Exception as e:
        _logger.error(f"Không thể giảm socket timeout: {str(e)}")
    
    # Kill processes ngay lập tức không đợi
    kill_chrome_processes(emergency=True)
    
    # Chạy emergency cleanup trong một thread riêng với timeout
    try:
        emergency_cleanup_thread = threading.Thread(
            target=run_emergency_cleanup_handlers,
            name="EmergencyCleanup"
        )
        emergency_cleanup_thread.daemon = True
        emergency_cleanup_thread.start()
        
        # Chờ tối đa 2 giây
        emergency_cleanup_thread.join(2.0)
    except Exception as e:
        _logger.error(f"Lỗi khi chạy emergency cleanup: {str(e)}")
    
    # Thoát tức thì không thực hiện thêm cleanup
    print("Thoát khẩn cấp hoàn tất.")
    os._exit(1)

def kill_chrome_processes(emergency: bool = False) -> None:
    """
    Tìm và đóng tất cả tiến trình Chrome và ChromeDriver.
    
    Args:
        emergency: Nếu True, sử dụng biện pháp mạnh hơn để đóng ngay lập tức
    """
    try:
        # Nếu là thoát khẩn cấp và trên Windows, dùng taskkill trước
        if emergency and platform.system() == "Windows":
            _logger.info("Đóng Chrome/ChromeDriver qua taskkill")
            try:
                os.system("taskkill /f /im chromedriver.exe >nul 2>&1")
                os.system("taskkill /f /im chrome.exe >nul 2>&1")
            except Exception as e:
                _logger.error(f"Lỗi khi dùng taskkill: {str(e)}")
        
        # Tìm và đóng trực tiếp từng tiến trình
        killed_pids = set()
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    proc_name = proc.info.get('name', '').lower()
                    pid = proc.info.get('pid')
                    
                    if not pid or pid in killed_pids:
                        continue
                        
                    # Tìm các tiến trình Chrome và ChromeDriver
                    is_target = False
                    
                    if proc_name and ('chromedriver' in proc_name):
                        is_target = True
                    elif proc_name and ('chrome' in proc_name):
                        # Xác định kỹ hơn nếu là Chrome điều khiển bởi Selenium
                        cmdline = proc.info.get('cmdline', [])
                        if cmdline and any(arg for arg in cmdline if 'remote-debugging-port' in str(arg).lower()):
                            is_target = True
                    
                    if is_target:
                        _logger.debug(f"Đóng tiến trình {proc_name} (PID: {pid})")
                        try:
                            if emergency:
                                proc.kill()  # Đóng ngay lập tức
                            else:
                                proc.terminate()  # Đóng nhẹ nhàng hơn
                            killed_pids.add(pid)
                        except Exception as e:
                            _logger.debug(f"Không thể đóng tiến trình {pid}: {str(e)}")
                
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                    _logger.debug(f"Lỗi truy cập tiến trình: {str(e)}")
                except Exception as e:
                    _logger.error(f"Lỗi không xác định khi kiểm tra tiến trình: {str(e)}")
        
        except Exception as e:
            _logger.error(f"Lỗi khi liệt kê tiến trình: {str(e)}")
    
        _logger.info(f"Đã đóng {len(killed_pids)} tiến trình Chrome/ChromeDriver")
        
        # Nếu khẩn cấp, thử lại sau 500ms để đảm bảo tiến trình đã đóng
        if emergency and killed_pids:
            time.sleep(0.5)
            # Kiểm tra và kill tiếp các tiến trình còn sót
            for pid in killed_pids:
                try:
                    if psutil.pid_exists(pid):
                        proc = psutil.Process(pid)
                        proc.kill()
                except Exception:
                    pass
    
    except Exception as e:
        _logger.error(f"Lỗi khi đóng tiến trình Chrome: {str(e)}")

def run_all_cleanup_handlers() -> None:
    """
    Gọi tất cả các hàm dọn dẹp đã đăng ký theo thứ tự ưu tiên.
    """
    if not _registered_cleanup_handlers:
        _logger.info("Không có cleanup handler nào được đăng ký")
        return
        
    _logger.info(f"Đang gọi {len(_registered_cleanup_handlers)} cleanup handlers")
    
    # _registered_cleanup_handlers đã được sắp xếp theo thứ tự ưu tiên
    for priority, handler, _ in _registered_cleanup_handlers:
        try:
            handler_name = getattr(handler, '__name__', str(handler))
            _logger.info(f"Gọi cleanup handler: {handler_name} (priority={priority})")
            handler()
        except Exception as e:
            _logger.error(f"Lỗi khi gọi cleanup handler {handler_name}: {str(e)}")
            # Ghi chi tiết lỗi ra log
            _logger.error(traceback.format_exc())
            print(f"Lỗi khi dọn dẹp {handler_name}: {str(e)}")

def run_emergency_cleanup_handlers() -> None:
    """
    Gọi các handler dọn dẹp trong trường hợp khẩn cấp, với timeout cho mỗi handler.
    """
    _logger.info("Bắt đầu emergency cleanup")
    
    for priority, handler, _ in _registered_cleanup_handlers:
        try:
            handler_name = getattr(handler, '__name__', str(handler))
            _logger.debug(f"Emergency cleanup: {handler_name}")
            
            # Đặt timeout cho mỗi handler
            handler_thread = threading.Thread(
                target=_run_handler_with_timeout, 
                args=(handler, handler_name),
                name=f"Cleanup-{handler_name}"
            )
            handler_thread.daemon = True
            handler_thread.start()
            handler_thread.join(0.5)  # Timeout sau 0.5s
            
            # Nếu luồng vẫn đang chạy sau timeout
            if handler_thread.is_alive():
                _logger.warning(f"Handler {handler_name} đã timeout")
        
        except Exception as e:
            _logger.error(f"Lỗi khi chạy emergency cleanup cho {str(handler)}: {str(e)}")

def _run_handler_with_timeout(handler: Callable, handler_name: str) -> None:
    """Chạy handler với xử lý lỗi và timeout."""
    try:
        handler()
    except Exception as e:
        _logger.error(f"Lỗi trong handler {handler_name}: {str(e)}")

def cleanup_resources(resources: List[Any]) -> None:
    """
    Dọn dẹp danh sách tài nguyên theo thứ tự.
    
    Args:
        resources: Danh sách các đối tượng cần dọn dẹp (cần có phương thức shutdown)
    """
    if not resources:
        return
        
    _logger.info(f"Đang dọn dẹp {len(resources)} tài nguyên...")
    
    # Đặt cờ shutdown để thông báo cho tất cả thread
    request_shutdown()
    
    # Đợi ngắn để đảm bảo tín hiệu lan truyền
    time.sleep(0.2)
    
    # Ưu tiên WebDriverPool trước
    driver_pools = [r for r in resources if hasattr(r, '__class__') and 
                   r.__class__.__name__ == 'WebDriverPool']
    other_resources = [r for r in resources if r not in driver_pools]
    
    # Đóng các WebDriverPool trước
    for resource in driver_pools:
        try:
            resource_name = resource.__class__.__name__
            _logger.info(f"Đang dọn dẹp {resource_name}...")
            resource.shutdown()
            _logger.debug(f"Đã dọn dẹp {resource_name}")
        except Exception as e:
            _logger.error(f"Lỗi khi dọn dẹp {resource.__class__.__name__}: {str(e)}")
    
    # Đóng các tài nguyên khác
    for resource in other_resources:
        try:
            if hasattr(resource, 'shutdown'):
                resource_name = resource.__class__.__name__
                _logger.info(f"Đang dọn dẹp {resource_name}...")
                resource.shutdown()
                _logger.debug(f"Đã dọn dẹp {resource_name}")
            else:
                _logger.warning(f"Tài nguyên {resource.__class__.__name__} không có phương thức shutdown()")
        except Exception as e:
            _logger.error(f"Lỗi khi dọn dẹp {resource.__class__.__name__}: {str(e)}")
    
    _logger.info("Dọn dẹp tài nguyên hoàn tất")
    
    # Đảm bảo đóng tất cả các tiến trình Chrome còn sót lại
    kill_chrome_processes(emergency=False)

def is_emergency_exit_in_progress() -> bool:
    """
    Kiểm tra xem có đang trong quá trình thoát khẩn cấp không.
    
    Returns:
        bool: True nếu đang trong quá trình thoát khẩn cấp
    """
    return emergency_exit_in_progress

# Đăng ký kill_chrome_processes như một handler mặc định với ưu tiên thấp
register_cleanup_handler(lambda: kill_chrome_processes(emergency=False), priority=-100)
