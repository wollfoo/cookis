"""
Module for processing cookie files with optimized handling for large files,
caching, and prioritization of authentication cookies.
"""

import time
import re
import json
import threading
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from collections import defaultdict

# Import from other project modules
from cookie_utils import parse_cookie_file, group_cookies_by_domain

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
        """
        Khởi tạo CookieProcessor.

        Args:
            domain_url_map: Dict mapping domain names to URLs
            logger: Logger instance (optional)
            cache_size: Maximum number of cached results
        """
        self.domain_url_map = domain_url_map
        self.logger = logger or logging.getLogger(__name__)
        self.cache = {}  # {file_path: (timestamp, result)}
        self.cache_size = cache_size
        self.cache_lock = threading.RLock()
        self.processing_stats = {
            "total_processed": 0,
            "cache_hits": 0,
            "streaming_parsed": 0,
            "regular_parsed": 0,
            "failed_parse": 0,
            "total_cookies_processed": 0
        }

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
        
        Args:
            file_path: Path to the cookie file
            
        Returns:
            bool: True if the file appears to be a valid cookie file, False otherwise
        """
        try:
            file_path = Path(file_path)

            # Kiểm tra kích thước file
            file_size = file_path.stat().st_size
            if file_size == 0:
                self.logger.warning(f"File trống: {file_path}")
                return False

            if file_size > 10 * 1024 * 1024:  # 10MB limit
                self.logger.warning(f"File quá lớn (>10MB): {file_path}")
                return False

            content_check_size = min(file_size, 8192)  # Check first 8KB for patterns
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(content_check_size)

                # Check for JSON format or cookie structure
                if not ('{"' in sample or '[\n' in sample or '[{' in sample):
                    if not ('domain' in sample.lower() and 'value' in sample.lower()):
                        self.logger.warning(f"File không có định dạng cookie JSON: {file_path}")
                        return False

                # Check for Microsoft/Azure domains
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
        
        Args:
            file_path: Path to the cookie file
            
        Returns:
            Dict[str, List[Dict]]: Cookies grouped by domain, with prioritized ordering
        """
        start_time = time.time()
        self.processing_stats["total_processed"] += 1
        file_path_str = str(file_path)
        
        # Kiểm tra cache
        with self.cache_lock:
            if file_path_str in self.cache:
                timestamp, result = self.cache[file_path_str]
                file_mtime = Path(file_path).stat().st_mtime
                # Nếu file không thay đổi kể từ lần xử lý cuối, trả về kết quả cached
                if file_mtime <= timestamp:
                    self.logger.debug(f"Sử dụng kết quả cache cho {file_path}")
                    self.processing_stats["cache_hits"] += 1
                    return result
        
        file_path = Path(file_path)
        
        # Kiểm tra sự tồn tại của file
        if not file_path.exists():
            self.logger.error(f"File không tồn tại: {file_path}")
            return {}
            
        file_size = file_path.stat().st_size

        try:
            # Phương pháp xử lý dựa trên kích thước file
            if file_size > 1024 * 1024:  # 1MB
                self.logger.debug(f"Dùng streaming parser cho file lớn: {file_path} ({file_size/1024/1024:.2f}MB)")
                self.processing_stats["streaming_parsed"] += 1
                result = self._process_large_file(file_path)
            else:
                self.logger.debug(f"Xử lý file thông thường: {file_path} ({file_size/1024:.2f}KB)")
                self.processing_stats["regular_parsed"] += 1
                cookies = parse_cookie_file(str(file_path))
                result = self._group_and_prioritize_cookies(cookies)
                
            # Cập nhật thống kê
            total_cookies = sum(len(cookies) for cookies in result.values())
            self.processing_stats["total_cookies_processed"] += total_cookies
            
            # Lưu vào cache
            with self.cache_lock:
                # Giới hạn kích thước cache
                if len(self.cache) >= self.cache_size:
                    # Xóa entry cũ nhất
                    oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][0])
                    del self.cache[oldest_key]
                self.cache[file_path_str] = (time.time(), result)
            
            processing_time = time.time() - start_time
            if processing_time > 1.0:  # Log only if processing took more than 1 second
                self.logger.info(f"Xử lý file {file_path.name} hoàn tất trong {processing_time:.2f}s, {total_cookies} cookies")
            else:
                self.logger.debug(f"Xử lý file {file_path.name} hoàn tất trong {processing_time:.2f}s, {total_cookies} cookies")
                
            return result
        except Exception as e:
            self.processing_stats["failed_parse"] += 1
            self.logger.error(f"Lỗi xử lý file cookie {file_path}: {type(e).__name__}: {str(e)}")
            return {}

    def _process_large_file(self, file_path: Path) -> Dict[str, List[Dict]]:
        """
        Xử lý file cookie lớn với streaming.
        
        Args:
            file_path: Path to the large cookie file
            
        Returns:
            Dict[str, List[Dict]]: Cookies grouped by domain
        """
        try:
            self.logger.debug(f"Bắt đầu xử lý file lớn: {file_path}")
            if file_path.suffix.lower() == '.json':
                return self._process_large_json_file(file_path)
            else:
                return self._process_large_text_file(file_path)
        except Exception as e:
            self.logger.error(f"Lỗi xử lý file lớn {file_path}: {type(e).__name__}: {str(e)}")
            return {}

    def _process_large_json_file(self, file_path: Path) -> Dict[str, List[Dict]]:
        """
        Xử lý file JSON lớn bằng streaming parser.
        
        Args:
            file_path: Path to the large JSON cookie file
            
        Returns:
            Dict[str, List[Dict]]: Cookies grouped by domain
        """
        import json
        all_cookies = []
        cookies_found = 0
        start_time = time.time()
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_char = f.read(1).strip()
                f.seek(0)
                
                if first_char == '[':
                    # File is likely a JSON array
                    self.logger.debug(f"Phát hiện định dạng JSON array trong {file_path.name}")
                    try:
                        # Thử dùng ijson nếu có
                        try:
                            import ijson
                            self.logger.debug(f"Sử dụng ijson cho streaming parsing")
                            parser = ijson.items(f, 'item')
                            for cookie in parser:
                                if isinstance(cookie, dict) and 'domain' in cookie:
                                    cookies_found += 1
                                    all_cookies.append(cookie)
                                    # Log progress periodically
                                    if cookies_found % 1000 == 0:
                                        self.logger.debug(f"Đã tìm thấy {cookies_found} cookies...")
                        except ImportError:
                            self.logger.debug(f"ijson không khả dụng, sử dụng json.load thông thường")
                            data = json.load(f)
                            if isinstance(data, list):
                                for cookie in data:
                                    if isinstance(cookie, dict) and 'domain' in cookie:
                                        cookies_found += 1
                                        all_cookies.append(cookie)
                    except Exception as e:
                        # Fallback to standard JSON parser
                        self.logger.warning(f"Lỗi với streaming parser, fallback to standard parser: {str(e)}")
                        f.seek(0)
                        data = json.load(f)
                        if isinstance(data, list):
                            for cookie in data:
                                if isinstance(cookie, dict) and 'domain' in cookie:
                                    cookies_found += 1
                                    all_cookies.append(cookie)
                elif first_char == '{':
                    # File is likely a single JSON object or a nested structure
                    self.logger.debug(f"Phát hiện định dạng JSON object trong {file_path.name}")
                    data = json.load(f)
                    if isinstance(data, dict):
                        if 'domain' in data:
                            # Single cookie
                            cookies_found += 1
                            all_cookies.append(data)
                        elif 'cookies' in data and isinstance(data['cookies'], list):
                            # Array of cookies in a 'cookies' property
                            for cookie in data['cookies']:
                                if isinstance(cookie, dict) and 'domain' in cookie:
                                    cookies_found += 1
                                    all_cookies.append(cookie)
                else:
                    self.logger.warning(f"Không phát hiện cấu trúc JSON hợp lệ trong {file_path.name}")
        except Exception as e:
            self.logger.error(f"Lỗi đọc JSON từ {file_path}: {type(e).__name__}: {str(e)}")
            return {}
            
        # Log completion and processing stats
        processing_time = time.time() - start_time
        self.logger.debug(f"Đã xử lý {cookies_found} cookies từ {file_path.name} trong {processing_time:.2f}s")
        
        return self._group_and_prioritize_cookies(all_cookies)

    def _process_large_text_file(self, file_path: Path) -> Dict[str, List[Dict]]:
        """
        Xử lý file văn bản lớn theo từng dòng.
        
        Args:
            file_path: Path to the large text cookie file
            
        Returns:
            Dict[str, List[Dict]]: Cookies grouped by domain
        """
        all_cookies = []
        cookies_found = 0
        errors = 0
        start_time = time.time()
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
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
                                'value': parts[6].strip()  # Ensure no trailing newlines
                            }
                            all_cookies.append(cookie)
                            cookies_found += 1
                            
                            # Log progress periodically
                            if cookies_found % 1000 == 0:
                                self.logger.debug(f"Đã tìm thấy {cookies_found} cookies...")
                        except (IndexError, ValueError) as e:
                            errors += 1
                            if errors <= 10:  # Limit error logging
                                self.logger.debug(f"Lỗi xử lý dòng {line_num}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Lỗi đọc file văn bản {file_path}: {type(e).__name__}: {str(e)}")
            return {}
            
        # Log completion and processing stats
        processing_time = time.time() - start_time
        self.logger.debug(
            f"Đã xử lý {cookies_found} cookies từ {file_path.name} trong {processing_time:.2f}s, "
            f"{errors} lỗi parsing"
        )
        
        return self._group_and_prioritize_cookies(all_cookies)

    def _group_and_prioritize_cookies(self, cookies: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Gom nhóm cookie theo domain và sắp xếp theo mức độ ưu tiên.
        
        Sắp xếp cookie trong mỗi domain theo mức độ quan trọng:
        - Cookie có expiry cao hơn đặt sau
        - Cookie auth/session có mức ưu tiên cao hơn
        
        Args:
            cookies: List of cookie dictionaries
            
        Returns:
            Dict[str, List[Dict]]: Cookies grouped by domain with priority ordering
        """
        if not cookies:
            return {}
            
        cookies_by_domain = group_cookies_by_domain(cookies, self.domain_url_map)
        prioritized = {}
        
        # Validate cookies and filter out invalid ones
        valid_cookies = []
        for cookie in cookies:
            if not isinstance(cookie, dict) or 'domain' not in cookie or 'name' not in cookie or 'value' not in cookie:
                continue
            valid_cookies.append(cookie)
        
        if len(valid_cookies) < len(cookies):
            self.logger.debug(f"Đã lọc {len(cookies) - len(valid_cookies)} cookie không hợp lệ")
        
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
                    auth_terms = [
                        'auth', 'token', 'session', 'signin', 'login', 'credential', 
                        'access', '.auth', 'jwt', 'estsauth', 'bearer', 'refresh'
                    ]
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
                            elif time_left < 86400:  # Less than a day
                                score -= 2.0
                    
                    # Cookie httpOnly và secure có điểm cao hơn
                    if cookie.get('httpOnly', False):
                        score += 2.0
                    if cookie.get('secure', False):
                        score += 1.0
                        
                    # Ưu tiên cookie có domain cụ thể hơn
                    domain_specificity = len(cookie.get('domain', '').split('.'))
                    score += 0.5 * domain_specificity
                        
                    return score
                
                # Sắp xếp theo điểm, cao đến thấp
                prioritized[domain] = sorted(domain_cookies, key=score_cookie, reverse=True)
                del cookies_by_domain[domain]
                
        # Thêm các domain còn lại
        for domain, domain_cookies in cookies_by_domain.items():
            prioritized[domain] = domain_cookies
            
        return prioritized

    def clear_cache(self):
        """
        Xóa toàn bộ cache để giải phóng bộ nhớ.
        """
        with self.cache_lock:
            cache_size = len(self.cache)
            self.cache.clear()
            self.logger.debug(f"Đã xóa cache CookieProcessor ({cache_size} entries)")
            
    def get_stats(self) -> Dict[str, Any]:
        """
        Lấy thống kê về quá trình xử lý cookie.
        
        Returns:
            Dict[str, Any]: Statistics about cookie processing
        """
        stats = self.processing_stats.copy()
        stats["cache_size"] = len(self.cache)
        stats["cache_hit_rate"] = (stats["cache_hits"] / stats["total_processed"]) * 100 if stats["total_processed"] > 0 else 0
        stats["failure_rate"] = (stats["failed_parse"] / stats["total_processed"]) * 100 if stats["total_processed"] > 0 else 0
        return stats
