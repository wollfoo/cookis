# logging_utils.py
import logging
import sys
from logging import Logger
from typing import Optional

def setup_logging(
    log_file: str = "checker.log",
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG
) -> Logger:
    """
    Thiết lập logging ra console và file log, cho phép cấu hình level riêng.
    
    Args:
        log_file (str): Đường dẫn/nơi lưu file log. Mặc định là 'checker.log'.
        console_level (int): Mức log cho console handler (VD: logging.INFO).
        file_level (int): Mức log cho file handler (VD: logging.DEBUG).

    Returns:
        Logger: Đối tượng logger đã được thiết lập handler cho console & file.
    """
    try:
        # Tạo một logger gốc (root logger) hoặc đặt tên cụ thể tùy nhu cầu
        logger = logging.getLogger()  # lấy root logger
        logger.setLevel(min(console_level, file_level))  
        # Hoặc dùng logger = logging.getLogger("cookie_checker") nếu muốn logger tên riêng

        # Xóa tất cả handler cũ (tránh add handler trùng lặp khi gọi setup_logging nhiều lần)
        if logger.hasHandlers():
            logger.handlers.clear()

        # Định dạng log chung
        file_formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        console_formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s]: %(message)s",
            datefmt="%H:%M:%S"
        )

        # -----------------------
        # 1) File Handler
        # -----------------------
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setLevel(file_level)
        fh.setFormatter(file_formatter)
        logger.addHandler(fh)

        # -----------------------
        # 2) Console Handler
        # -----------------------
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(console_level)
        ch.setFormatter(console_formatter)
        logger.addHandler(ch)

        # Trả về logger đã cấu hình đầy đủ
        return logger

    except Exception as e:
        # Nếu lỗi trong quá trình setup, in ra và dừng chương trình
        print(f"[!] Lỗi thiết lập logging: {e}")
        sys.exit(1)
