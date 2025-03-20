"""
result_manager.py - Quản lý kết quả kiểm tra cookie

Module được tái cấu trúc từ main.py để tách biệt logic quản lý kết quả,
giúp cải thiện khả năng bảo trì và mở rộng của hệ thống.

Chức năng chính:
- Lưu và tổng hợp kết quả kiểm tra cookie Azure/Microsoft
- Quản lý file cookie hợp lệ (di chuyển vào thư mục riêng)
- Tạo tóm tắt về quá trình kiểm tra
- Tối ưu bộ nhớ thông qua xử lý batch, giảm thiểu sử dụng RAM
- Dọn dẹp file cũ tự động, tối ưu không gian lưu trữ

Workflow điển hình:
1. Khởi tạo ResultManager với thư mục đầu ra
2. Thêm kết quả kiểm tra cookie thông qua add_result()
3. Di chuyển cookie hợp lệ bằng move_valid_cookie()
4. Lấy và hiển thị báo cáo tóm tắt với get_summary() và print_summary()
5. Lưu tất cả kết quả với save_all_results()
6. Dọn dẹp tài nguyên khi kết thúc với shutdown()

Phiên bản: 1.0.0
Tác giả: Cookie Checker Team
"""

import os
import re
import time
import json
import shutil
import logging
import threading
import traceback
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Union, Set, Iterator, Callable
from collections import defaultdict, Counter


class ResultManager:
    """
    Quản lý kết quả với tối ưu bộ nhớ và hiệu suất cao.
    
    Đặc điểm chính:
    - Lưu kết quả trung gian ra đĩa để giảm sử dụng RAM
    - Xử lý theo batch để kiểm soát bộ nhớ
    - Tạo báo cáo kết quả chi tiết và tổng hợp
    - Phân tích thống kê để cải thiện hiệu suất
    - Thread-safe với cơ chế lock đảm bảo an toàn khi đa luồng
    - Tự động dọn dẹp tài nguyên khi kết thúc
    
    Lưu ý:
    - Kết quả được lưu theo từng batch để tối ưu bộ nhớ
    - Cookie chỉ được coi là hợp lệ khi final_validation=True
      (kết hợp của login_success=True và api_success=True)
    - Các phương thức phân tích (analysis) không làm thay đổi dữ liệu
    """
    def __init__(
        self,
        output_dir: Path,
        max_results_in_memory: int = 500,
        logger: Optional[logging.Logger] = None
    ):
        """
        Khởi tạo ResultManager để quản lý kết quả kiểm tra cookie.
        
        Args:
            output_dir: Thư mục đầu ra để lưu kết quả và báo cáo
            max_results_in_memory: Số lượng kết quả tối đa lưu trong bộ nhớ trước khi ghi ra đĩa
            logger: Logger để ghi log, nếu None sẽ tạo logger mặc định
        """
        # Thiết lập thư mục đầu ra
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Thiết lập logger
        self.logger = logger or logging.getLogger(__name__)
        
        # Khởi tạo các biến quản lý kết quả
        self.results = {}  # {filename: result_dict}
        self.results_count = 0
        self.max_results_in_memory = max_results_in_memory
        self.valid_count = 0
        self.invalid_count = 0
        self.error_count = 0
        self.batch_files = []  # Danh sách các file batch đã lưu
        
        # Cơ chế lock để đảm bảo thread-safe
        self.result_lock = threading.RLock()
        
        # Tạo timestamp để đánh dấu phiên làm việc
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Thư mục lưu cookie hợp lệ
        self.valid_cookies_dir = self.output_dir / f"valid_cookies_{self.timestamp}"
        
        # Thông tin phân tích và metrics
        self.start_time = datetime.now()
        self.processing_stats = {
            "total_processing_time": 0,
            "avg_processing_time": 0,
            "error_types": defaultdict(int),
            "validation_methods": defaultdict(int),
            "batch_save_times": [],          # Thời gian lưu mỗi batch
            "memory_usage_estimates": [],    # Ước tính sử dụng bộ nhớ
            "disk_usage": {                  # Thông tin sử dụng đĩa
                "initial": self._get_folder_size(self.output_dir),
                "current": 0,
                "total_written": 0
            }
        }
        
        # Ghi log khởi tạo
        self.logger.info(f"ResultManager khởi tạo thành công với output_dir={output_dir}, max_in_memory={max_results_in_memory}")

    def add_result(self, filename: str, result: Dict) -> None:
        """
        Thêm kết quả kiểm tra vào quản lý kết quả.
        Tự động lưu batch khi đạt ngưỡng bộ nhớ.
        
        Cookie chỉ được coi là hợp lệ khi final_validation = True
        (tức là đã đạt cả login_success và api_success)
        
        Args:
            filename: Tên file cookie
            result: Kết quả kiểm tra
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
        """
        Lưu batch kết quả xuống đĩa để giảm sử dụng bộ nhớ.
        
        Phương thức này ghi kết quả ra file JSON và cập nhật thống kê batch.
        Các kết quả trong self.results sẽ bị xóa sau khi lưu thành công.
        """
        if not self.results:
            return
            
        batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        batch_file = self.output_dir / f"results_batch_{batch_timestamp}.json"
        batch_start_time = time.time()
        
        try:
            # Kiểm tra không gian đĩa trước khi lưu
            self._check_disk_space()
            
            # Tính toán thống kê batch
            batch_stats = {
                'valid_count': sum(1 for r in self.results.values() 
                               if r.get('valid', False) and r.get('details', {}).get('final_validation', False)),
                'invalid_count': sum(1 for r in self.results.values() 
                                 if not (r.get('valid', False) and r.get('details', {}).get('final_validation', False)) 
                                 and 'error' not in r),
                'error_count': sum(1 for r in self.results.values() if 'error' in r),
                'processed_count': len(self.results),
                'timestamp': batch_timestamp,
                'elapsed_time': (datetime.now() - self.start_time).total_seconds()
            }
            
            # Chuẩn bị dữ liệu batch
            batch_data = {
                'timestamp': batch_timestamp,
                'batch_stats': batch_stats,
                'results': self.results
            }
            
            # Tạo thư mục cha nếu chưa tồn tại
            batch_file.parent.mkdir(exist_ok=True, parents=True)
            
            # Mở file với context manager để đảm bảo đóng file sau khi viết
            with open(batch_file, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, ensure_ascii=False, indent=2)
            
            # Kiểm tra file đã được tạo thành công
            if not batch_file.exists() or batch_file.stat().st_size == 0:
                raise IOError(f"File batch {batch_file} không được tạo hoặc rỗng")
                
            # Cập nhật thông tin batch
            batch_size = batch_file.stat().st_size
            batch_time = time.time() - batch_start_time
            
            # Thêm vào danh sách các batch đã lưu
            self.batch_files.append(batch_file)
            
            # Cập nhật thống kê
            self.processing_stats["batch_save_times"].append(batch_time)
            self.processing_stats["disk_usage"]["total_written"] += batch_size
            
            # Cập nhật sử dụng đĩa hiện tại
            self.processing_stats["disk_usage"]["current"] = self._get_folder_size(self.output_dir)
            
            # Ghi log
            self.logger.debug(
                f"Đã lưu batch: {batch_file.name} | "
                f"{len(self.results)} kết quả | "
                f"{batch_size/1024:.1f}KB | "
                f"{batch_time:.3f}s"
            )
            
            # Xóa dữ liệu đã lưu khỏi bộ nhớ
            prev_results_count = len(self.results)
            self.results = {}
            
            # Ghi log tổng kết
            self.logger.info(
                f"Đã giải phóng bộ nhớ: {prev_results_count} kết quả | "
                f"Tổng số batch: {len(self.batch_files)}"
            )
            
        except Exception as e:
            error_msg = f"Lỗi lưu batch {batch_file}: {type(e).__name__}: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            
            # Cố gắng xóa file lỗi nếu tồn tại
            try:
                if batch_file.exists():
                    os.remove(batch_file)
                    self.logger.debug(f"Đã xóa file batch lỗi: {batch_file}")
            except Exception as cleanup_error:
                self.logger.warning(f"Không thể xóa file batch lỗi: {cleanup_error}")
                
            # Không xóa results nếu lưu không thành công
            # để không mất dữ liệu

    def move_valid_cookie(self, file_path: Path, is_valid: bool, result_details: Optional[Dict] = None) -> Optional[Path]:
        """
        Di chuyển file cookie hợp lệ vào thư mục riêng.
        Cookie chỉ được coi là hợp lệ khi final_validation = True (login_success AND api_success).
        
        Quá trình này bao gồm các bước:
        1. Kiểm tra xem file có hợp lệ dựa trên cả is_valid và final_validation
        2. Tạo thư mục đích nếu chưa tồn tại
        3. Sao chép file đến thư mục đích, giữ nguyên metadata
        4. Xóa file gốc nếu sao chép thành công
        5. Nếu sao chép thất bại, thử phương pháp di chuyển trực tiếp
        
        Args:
            file_path: Đường dẫn file cookie cần di chuyển
            is_valid: Cờ xác định file cookie có hợp lệ không (theo phía người gọi)
            result_details: Chi tiết kết quả kiểm tra (chứa thông tin validation)
        
        Returns:
            Optional[Path]: Đường dẫn mới nếu di chuyển thành công, None nếu thất bại
        
        Raises:
            FileNotFoundError: Nếu file nguồn không tồn tại
            PermissionError: Nếu không có quyền ghi vào thư mục đích
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
        
        Returns:
            Dict: Thông tin tóm tắt kết quả
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
            print(f"Không hợp lệ: {summary['invalid_count']} ({summary['invalid_percentage']:.1f}%)")
            print(f"Lỗi: {summary['error_count']} ({summary['error_percentage']:.1f}%)")
            
            # Thêm phần hiển thị thống kê API
            api_stats = summary.get('api_stats', {})
            api_total = api_stats.get('total_checked', 0)
            if api_total > 0:
                print(f"\nThống kê API:")
                print(f"  - Xác thực API thành công: {api_stats.get('success_count', 0)} ({api_stats.get('success_rate', 0):.1f}%)")
                print(f"  - Xác thực API thất bại: {api_stats.get('failed_count', 0)} ({100-api_stats.get('success_rate', 0):.1f}%)")
            
            print(f"\nThời gian xử lý: {summary['performance']['elapsed_time']:.2f} giây")
            print(f"Tốc độ xử lý: {summary['performance']['processed_per_second']:.2f} file/giây")
            
            # In top errors
            top_errors = summary.get('error_analysis', {}).get('top_errors', {})
            if top_errors:
                print("\nTop lỗi phổ biến:")
                for error, count in top_errors.items():
                    print(f"  - {error}: {count} lần")
                    
        if summary['valid_count'] > 0:
            print(f"\nCookie hợp lệ đã được lưu vào: {summary['output']['valid_cookies_dir']}")

    def save_all_results(self) -> Path:
        """
        Lưu toàn bộ kết quả vào một file JSON tổng hợp.
        
        Phương thức này tổng hợp kết quả từ tất cả các batch files đã lưu 
        và kết quả còn trong bộ nhớ, sau đó tạo một file JSON đơn giản.
        
        Returns:
            Path: Đường dẫn đến file kết quả tổng hợp
            
        Raises:
            IOError: Nếu có lỗi khi đọc/ghi file
        """
        try:
            # Trước tiên, lưu các kết quả còn trong bộ nhớ
            if self.results:
                self._save_results_batch()
                
            # Tạo file tổng hợp
            summary_file = self.output_dir / f"summary_results_{self.timestamp}.json"
            
            # Tổng hợp kết quả từ các batch
            combined_results = {}
            batch_count = 0
            
            # Thu thập kết quả từ các file batch
            for batch_file in self.batch_files:
                try:
                    if batch_file.exists():
                        with open(batch_file, 'r', encoding='utf-8') as f:
                            batch_data = json.load(f)
                            if 'results' in batch_data:
                                combined_results.update(batch_data['results'])
                                batch_count += 1
                except Exception as e:
                    self.logger.error(f"Lỗi đọc batch file {batch_file.name}: {str(e)}")
            
            # Tạo summary
            summary = self.get_summary()
            
            # Lưu kết quả tổng hợp
            final_data = {
                'summary': summary,
                'timestamp': datetime.now().isoformat(),
                'batch_count': batch_count,
                'results': combined_results
            }
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, ensure_ascii=False, indent=2)
                
            self.logger.info(f"Đã lưu kết quả tổng hợp vào {summary_file} ({len(combined_results)} kết quả)")
            return summary_file
            
        except Exception as e:
            self.logger.error(f"Lỗi khi lưu kết quả tổng hợp: {str(e)}")
            # Fallback - tạo file tổng hợp đơn giản chỉ chứa summary
            try:
                fallback_file = self.output_dir / f"summary_fallback_{self.timestamp}.json"
                with open(fallback_file, 'w', encoding='utf-8') as f:
                    json.dump(self.get_summary(), f, ensure_ascii=False, indent=2)
                self.logger.info(f"Đã lưu fallback summary vào {fallback_file}")
                return fallback_file
            except Exception as inner_e:
                self.logger.error(f"Lỗi nghiêm trọng khi lưu fallback: {str(inner_e)}")
                return Path()

    def cleanup_old_files(self, days_to_keep: int = 7) -> int:
        """
        Dọn dẹp các file kết quả cũ để tiết kiệm dung lượng.
        
        Phương thức này sẽ xóa các batch file và summary file cũ hơn số ngày
        được chỉ định. File được coi là cũ nếu thời gian sửa đổi (mtime)
        của nó cũ hơn `days_to_keep` ngày.
        
        Args:
            days_to_keep: Số ngày giữ lại file (xóa các file cũ hơn)
            
        Returns:
            int: Số file đã xóa
            
        Note:
            Thư mục chứa cookie hợp lệ sẽ không bị xóa, chỉ các file JSON kết quả
            và báo cáo được xóa.
        """
        try:
            cutoff_time = time.time() - (days_to_keep * 24 * 3600)
            files_removed = 0
            
            # Tìm tất cả các file batch và summary cũ
            patterns = ["results_batch_*.json", "summary_results_*.json"]
            
            for pattern in patterns:
                for file_path in self.output_dir.glob(pattern):
                    try:
                        # Kiểm tra thời gian tạo file
                        file_creation_time = file_path.stat().st_mtime
                        if file_creation_time < cutoff_time:
                            # File quá cũ, xóa
                            os.remove(str(file_path))
                            files_removed += 1
                            self.logger.debug(f"Đã xóa file cũ: {file_path.name}")
                    except Exception as e:
                        self.logger.warning(f"Không thể xóa file {file_path.name}: {str(e)}")
            
            if files_removed > 0:
                self.logger.info(f"Đã dọn dẹp {files_removed} file kết quả cũ (>{days_to_keep} ngày)")
                
            return files_removed
            
        except Exception as e:
            self.logger.error(f"Lỗi khi dọn dẹp file cũ: {str(e)}")
            return 0

    def get_detailed_report(self) -> Dict:
        """
        Tạo báo cáo chi tiết cho phân tích nội bộ.
        
        Phương thức này tập hợp thông tin chi tiết từ các phân tích 
        thành một báo cáo dạng dictionary để sử dụng nội bộ.
        
        Returns:
            Dict: Báo cáo chi tiết dạng Dict
        """
        summary = self.get_summary()
        
        # Thêm phân tích cơ bản vào summary
        detailed_report = {
            'summary': summary,
            'performance_analysis': self._analyze_performance(),
            'error_correlation': self._analyze_error_correlation(),
            'success_patterns': self._analyze_success_patterns(),
            'timestamp': datetime.now().isoformat(),
        }
        
        return detailed_report

    def _analyze_performance(self) -> Dict:
        """
        Phân tích cơ bản về hiệu suất xử lý.
        
        Returns:
            Dict: Thông tin hiệu suất cơ bản
        """
        # Phân tích đơn giản về hiệu suất hệ thống
        summary = self.get_summary()
        elapsed_time = summary['performance']['elapsed_time']
        
        # Tính toán các metrics hiệu suất
        files_per_second = summary['performance']['processed_per_second']
        avg_time_per_file = 1 / files_per_second if files_per_second > 0 else 0
        
        return {
            'files_per_second': round(files_per_second, 2),
            'avg_time_per_file': round(avg_time_per_file, 3),
            'total_time_seconds': round(elapsed_time, 2)
        }
        
    def _analyze_error_correlation(self) -> Dict:
        """
        Phân tích đơn giản về lỗi.
        
        Returns:
            Dict: Thông tin về lỗi
        """
        return {
            'total_errors': self.error_count,
            'error_rate': round((self.error_count / self.results_count * 100), 2) if self.results_count else 0
        }
        
    def _analyze_success_patterns(self) -> Dict:
        """
        Thống kê cơ bản về kết quả thành công.
        
        Returns:
            Dict: Thông tin về kết quả thành công
        """
        return {
            'success_rate': round((self.valid_count / self.results_count * 100), 2) if self.results_count else 0
        }

    def estimate_memory_usage(self) -> int:
        """
        Ước tính mức sử dụng bộ nhớ của ResultManager.
        
        Thực hiện ước tính dung lượng bộ nhớ mà ResultManager đang sử dụng
        dựa trên kích thước của các đối tượng dữ liệu. Kết quả này chỉ là
        ước tính và có thể khác với dung lượng thực tế.
        
        Returns:
            int: Dung lượng ước tính (bytes)
        """
        # Ước tính kích thước results
        results_size = 0
        for filename, result in self.results.items():
            # Ước tính kích thước filename
            filename_size = len(filename) * 2  # UTF-16 character ~ 2 bytes
            
            # Ước tính kích thước result
            result_str = json.dumps(result)
            result_size = len(result_str)
            
            # Tổng kích thước cho entry này
            results_size += filename_size + result_size
            
        # Ước tính cho các biến đếm và danh sách
        batch_files_size = sum(len(str(f)) for f in self.batch_files) * 2
        
        # Ước tính cho các metrics
        metrics_str = json.dumps(self.processing_stats)
        metrics_size = len(metrics_str)
        
        # Thêm overhead cho các đối tượng Python (ước tính)
        overhead = 1024 * 10  # ~10KB for Python object overhead
        
        # Tổng dung lượng ước tính
        total_size = results_size + batch_files_size + metrics_size + overhead
        
        # Lưu vào processing_stats để theo dõi
        self.processing_stats["memory_usage_estimates"].append({
            "timestamp": datetime.now().isoformat(),
            "results_count": len(self.results),
            "results_size": results_size,
            "batch_files_count": len(self.batch_files),
            "batch_files_size": batch_files_size,
            "metrics_size": metrics_size,
            "total_size": total_size
        })
        
        return total_size

    def merge_batch_results(self) -> Dict[str, Dict]:
        """
        Gộp kết quả từ tất cả các batch để phân tích.
        
        Returns:
            Dict[str, Dict]: Dictionary chứa tất cả kết quả đã gộp
        """
        merged_results = {}
        
        # Thêm kết quả hiện tại trong memory
        merged_results.update(self.results)
        
        # Đọc và gộp từ các batch files
        for batch_file in self.batch_files:
            try:
                if batch_file.exists():
                    with open(batch_file, 'r', encoding='utf-8') as f:
                        batch_data = json.load(f)
                        if 'results' in batch_data:
                            merged_results.update(batch_data['results'])
            except Exception as e:
                self.logger.warning(f"Không thể đọc batch file {batch_file}: {str(e)}")
                
        return merged_results
        
    def get_valid_cookies_info(self) -> List[Dict]:
        """
        Lấy thông tin chi tiết về tất cả cookie hợp lệ.
        
        Returns:
            List[Dict]: Danh sách thông tin cookie hợp lệ
        """
        return self._get_cookies_by_status(True)
        
    def get_invalid_cookies_info(self) -> List[Dict]:
        """
        Lấy thông tin chi tiết về tất cả cookie không hợp lệ.
        
        Returns:
            List[Dict]: Danh sách thông tin cookie không hợp lệ
        """
        return self._get_cookies_by_status(False)
        
    def _get_cookies_by_status(self, is_valid: bool) -> List[Dict]:
        """
        Lấy cookie theo trạng thái (hợp lệ/không hợp lệ).
        
        Args:
            is_valid: True để lấy cookie hợp lệ, False để lấy cookie không hợp lệ
            
        Returns:
            List[Dict]: Danh sách thông tin cookie theo trạng thái
        """
        cookies_info = []
        all_results = self.merge_batch_results()
        
        for filename, result in all_results.items():
            details = result.get('details', {})
            
            # Lấy trạng thái theo final_validation
            final_validation = False
            if 'final_validation' in details:
                final_validation = details['final_validation']
            elif 'final_validation' in result:
                final_validation = result['final_validation']
                
            # Kiểm tra trùng khớp trạng thái
            if final_validation == is_valid:
                # Trích xuất thông tin quan trọng
                cookies_info.append({
                    'filename': filename,
                    'login_success': details.get('login_status', False),
                    'api_success': details.get('api_check_status', False),
                    'user_id': details.get('user_id', 'N/A'),
                    'display_name': details.get('display_name', 'N/A'),
                    'domain_name': details.get('domain_name', 'N/A'),
                    'login_method': details.get('login_method', 'N/A'),
                    'timestamp': result.get('timestamp', details.get('timestamp', 'N/A'))
                })
                
        return cookies_info

    def shutdown(self) -> None:
        """
        Dọn dẹp tài nguyên khi đóng ResultManager.
        
        Thực hiện các thao tác cuối cùng trước khi đóng:
        1. Lưu các kết quả còn lại trong bộ nhớ
        2. Lưu báo cáo tổng hợp
        3. Cập nhật các metrics cuối cùng
        4. Ghi log thông tin tổng kết
        
        Phương thức này nên được gọi khi kết thúc quá trình kiểm tra,
        thường là trong khối finally của main.py.
        """
        self.logger.info("ResultManager đang thực hiện shutdown...")
        shutdown_start = time.time()
        
        try:
            # Cập nhật thời gian xử lý tổng cộng
            self.processing_stats["total_processing_time"] = (datetime.now() - self.start_time).total_seconds()
            
            # Cập nhật thời gian xử lý trung bình
            if self.results_count > 0:
                self.processing_stats["avg_processing_time"] = self.processing_stats["total_processing_time"] / self.results_count
            
            # Lưu kết quả còn lại trong bộ nhớ
            if self.results:
                self.logger.info(f"Lưu {len(self.results)} kết quả còn lại trong bộ nhớ")
                self._save_results_batch()
                
            # Lưu báo cáo tổng hợp
            summary_file = self.save_all_results()
            self.logger.info(f"Đã lưu báo cáo tổng hợp: {summary_file}")
            
            # Cập nhật thông tin sử dụng đĩa cuối cùng
            final_disk_usage = self._get_folder_size(self.output_dir)
            self.processing_stats["disk_usage"]["final"] = final_disk_usage
            self.processing_stats["disk_usage"]["growth"] = final_disk_usage - self.processing_stats["disk_usage"]["initial"]
            
            # Ghi log thông tin tổng kết
            summary = self.get_summary()
            self.logger.info(
                f"ResultManager shutdown hoàn tất: "
                f"{self.results_count} kết quả | "
                f"{summary['valid_count']} hợp lệ | "
                f"{summary['invalid_count']} không hợp lệ | "
                f"{summary['error_count']} lỗi | "
                f"Thời gian: {summary['performance']['elapsed_time']:.2f}s"
            )
            
            # Ghi thông tin hiệu suất
            self.logger.info(
                f"Hiệu suất: "
                f"{summary['performance']['processed_per_second']:.2f} file/giây | "
                f"Disk: {final_disk_usage/1024/1024:.2f}MB | "
                f"Batches: {len(self.batch_files)}"
            )
            
            # Ghi thời gian shutdown
            shutdown_time = time.time() - shutdown_start
            self.logger.info(f"Thời gian shutdown: {shutdown_time:.3f}s")
            
        except Exception as e:
            error_msg = f"Lỗi khi shutdown ResultManager: {type(e).__name__}: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())


def get_cookies_status(result_manager: ResultManager, list_valid: bool = True) -> List[Dict]:
    """
    Lấy danh sách chi tiết về trạng thái các cookie.
    
    Tiện ích này giúp lấy danh sách cookie hợp lệ hoặc không hợp lệ
    từ ResultManager, phục vụ cho xử lý tiếp theo.
    
    Args:
        result_manager: Đối tượng ResultManager chứa kết quả
        list_valid: True để liệt kê cookie hợp lệ, False để liệt kê cookie không hợp lệ
        
    Returns:
        List[Dict]: Danh sách thông tin trạng thái cookie, mỗi phần tử là một dict
                    chứa thông tin về filename, login_success, api_success, user_id,
                    display_name, domain_name, login_method, và timestamp
    """
    if list_valid:
        return result_manager.get_valid_cookies_info()
    else:
        return result_manager.get_invalid_cookies_info()


def create_result_manager(
    output_dir: Union[str, Path],
    max_results_in_memory: int = 500,
    logger: Optional[logging.Logger] = None
) -> ResultManager:
    """
    Tạo instance của ResultManager.
    
    Args:
        output_dir: Thư mục lưu kết quả
        max_results_in_memory: Số lượng kết quả tối đa lưu trong bộ nhớ
        logger: Logger instance để ghi log
        
    Returns:
        ResultManager: Đối tượng ResultManager đã khởi tạo
    """
    # Đảm bảo output_dir là kiểu Path
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    
    # Tạo logger mặc định nếu không được cung cấp
    if logger is None:
        logger = logging.getLogger("result_manager")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
    
    # Kiểm tra quyền ghi vào thư mục
    try:
        test_file = output_path / ".write_test"
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
    except (PermissionError, OSError) as e:
        logger.error(f"Không có quyền ghi vào thư mục {output_path}: {str(e)}")
        raise PermissionError(f"Không có quyền ghi vào thư mục {output_path}: {str(e)}")
    
    logger.info(f"Khởi tạo ResultManager với thư mục đầu ra: {output_path}")
    
    # Tạo và trả về instance ResultManager
    return ResultManager(
        output_dir=output_path,
        max_results_in_memory=max_results_in_memory,
        logger=logger
    )