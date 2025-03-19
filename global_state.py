"""
global_state.py - Module quản lý trạng thái toàn cục giữa các module
"""

# Biến trạng thái toàn cục
shutdown_requested = False

# Các hàm helper
def request_shutdown():
    """Đặt cờ shutdown thành True"""
    global shutdown_requested
    shutdown_requested = True
    return shutdown_requested

def is_shutdown_requested():
    """Kiểm tra trạng thái cờ shutdown"""
    return shutdown_requested
