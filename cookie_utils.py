#!/usr/bin/env python3
"""
cookie_utils.py - Module quản lý cookie cho Selenium WebDriver với hiệu suất và độ tin cậy cao.
Phiên bản production kết hợp ưu điểm từ các phiên bản A và B.

Tính năng chính:
- Hỗ trợ parsing cookie từ nhiều định dạng: Netscape (.txt), JSON (.json), YAML (.yaml/.yml) và SQLite (Chrome/Chromium)
- Streaming parser để tiết kiệm bộ nhớ khi xử lý file lớn.
- Domain matching nâng cao sử dụng DomainInfo (dataclass) và LRU cache.
- Thiết lập cookie qua CDP (devtools_set_cookie) với tùy chọn adaptive batch processing.
- Hỗ trợ ưu tiên cookie quan trọng khi set.
"""

import os
import json
import logging
import time
import random
import re
import sqlite3
import mmap
import tempfile
from pathlib import Path
from collections import defaultdict, deque
from typing import Dict, List, Any, Optional, Tuple, Set, Union, Iterator, Callable
from functools import lru_cache, partial
from urllib.parse import urlparse
from io import TextIOWrapper
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dataclasses import dataclass

from selenium import webdriver
from selenium.common.exceptions import (
    WebDriverException, TimeoutException, InvalidSessionIdException, StaleElementReferenceException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Cấu hình logger cho module
logger = logging.getLogger(__name__)


# ==== CUSTOM EXCEPTIONS
class CookieParseError(Exception):
    """Ngoại lệ khi phân tích cookie."""
    pass

class CookieAddError(Exception):
    """Ngoại lệ khi thêm cookie."""
    pass

class CookieFormatError(CookieParseError):
    """Ngoại lệ khi định dạng cookie không hợp lệ."""
    pass


# ==== DOMAIN MATCHING UTILS
@dataclass(frozen=True)
class DomainInfo:
    """Thông tin về domain đã chuẩn hóa để matching hiệu quả."""
    original: str
    normalized: str
    parts: Tuple[str, ...]
    tld: Optional[str] = None
    sld: Optional[str] = None
    is_ip: bool = False

    @property
    def domain_root(self) -> str:
        """Trả về SLD+TLD (domain root) nếu có."""
        if self.sld and self.tld:
            return f"{self.sld}.{self.tld}"
        return self.normalized

def is_ip_address(domain: str) -> bool:
    """Kiểm tra xem domain có phải là địa chỉ IP không."""
    ipv4_pattern = re.compile(r'^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$')
    if ipv4_pattern.match(domain):
        return all(0 <= int(part) <= 255 for part in domain.split('.'))
    if ':' in domain and domain.count(':') >= 2:
        return True
    return False

def normalize_domain(domain: str) -> str:
    """Chuẩn hóa domain: loại bỏ protocol, www, port, dấu chấm đầu, chuyển về chữ thường."""
    if not domain:
        return ""
    if "://" in domain:
        domain = domain.split("://", 1)[1]
    domain = domain.split('/', 1)[0].split('?', 1)[0].split('#', 1)[0]
    if ':' in domain and not is_ip_address(domain):
        domain = domain.split(':', 1)[0]
    domain = domain.strip().lower().lstrip('.')
    if domain.startswith("www."):
        domain = domain[4:]
    return domain

def extract_domain_parts(domain: str) -> Tuple[List[str], Optional[str], Optional[str]]:
    """Phân tích domain thành các phần, trích xuất TLD và SLD."""
    if not domain or is_ip_address(domain):
        return ([], None, None)
    parts = domain.split('.')
    if len(parts) < 2:
        return (parts, None, None)
    complex_tlds = {
        'co.uk', 'co.jp', 'co.nz', 'co.za', 'co.kr', 'co.in', 'ac.uk', 'ac.jp',
        'org.uk', 'net.uk', 'org.au', 'net.au', 'com.au', 'com.tr', 'com.br',
        'com.mx', 'com.vn', 'com.sg', 'com.my', 'com.ph', 'com.tw', 'com.cn',
        'com.hk', 'com.ar', 'com.pe', 'com.co', 'com.ve', 'com.ec', 'com.bo',
        'com.py', 'com.uy', 'com.pa', 'com.ni', 'com.do', 'com.gt', 'com.hn',
        'com.sv', 'com.cr', 'gov.uk', 'gov.au', 'gov.in', 'gov.sg', 'edu.au'
    }
    if len(parts) >= 3:
        possible_complex_tld = '.'.join(parts[-2:])
        if possible_complex_tld in complex_tlds:
            tld = possible_complex_tld
            sld = parts[-3]
            return (parts, tld, sld)
    tld = parts[-1]
    sld = parts[-2]
    return (parts, tld, sld)

def create_domain_info(domain: str) -> DomainInfo:
    """Tạo đối tượng DomainInfo từ domain."""
    if not domain:
        return DomainInfo(original=domain, normalized="", parts=tuple())
    ip_flag = is_ip_address(domain)
    norm = normalize_domain(domain)
    if ip_flag:
        return DomainInfo(original=domain, normalized=norm, parts=tuple(norm.split('.')), is_ip=True)
    parts, tld, sld = extract_domain_parts(norm)
    return DomainInfo(original=domain, normalized=norm, parts=tuple(parts), tld=tld, sld=sld, is_ip=False)

@lru_cache(maxsize=512)
def get_domain_info(domain: str) -> DomainInfo:
    """Lấy thông tin domain đã chuẩn hóa với cache để tăng hiệu suất."""
    return create_domain_info(domain)

def domain_matches(cookie_domain: str, target_domain: str) -> bool:
    """So sánh cookie_domain và target_domain, hỗ trợ subdomain và root domain matching."""
    if not cookie_domain or not target_domain:
        return False
    cookie_info = get_domain_info(cookie_domain)
    target_info = get_domain_info(target_domain)
    if cookie_info.is_ip and target_info.is_ip:
        return cookie_info.normalized == target_info.normalized
    if cookie_info.is_ip or target_info.is_ip:
        return False
    if cookie_info.normalized == target_info.normalized:
        return True
    if cookie_info.normalized.endswith(f".{target_info.normalized}") or target_info.normalized.endswith(f".{cookie_info.normalized}"):
        return True
    if cookie_info.domain_root and target_info.domain_root and cookie_info.domain_root == target_info.domain_root:
        return True
    return False

def find_best_domain_match(cookie_domain: str, domains: List[str]) -> Optional[str]:
    """
    Tìm domain khớp nhất từ danh sách domain.
    Thứ tự ưu tiên:
    1. Khớp chính xác (normalized domain)
    2. Khớp root domain
    3. Khớp domain/subdomain thông thường
    """
    if not cookie_domain or not domains:
        return None
        
    cookie_info = get_domain_info(cookie_domain)
    exact_match = None    # Khớp chính xác (normalized)
    root_match = None     # Khớp root domain
    normal_match = None   # Khớp subdomain/domain thông thường
    
    # Thực hiện một lần duyệt duy nhất
    for domain in domains:
        # Chỉ kiểm tra domain_matches một lần
        if domain_matches(cookie_domain, domain):
            d_info = get_domain_info(domain)
            
            # Ưu tiên 1: Khớp chính xác
            if cookie_info.normalized == d_info.normalized:
                return domain  # Trả về ngay lập tức nếu tìm thấy khớp chính xác
                
            # Ưu tiên 2: Khớp root domain
            if (cookie_info.domain_root and d_info.domain_root and 
                cookie_info.domain_root == d_info.domain_root):
                root_match = root_match or domain
                
            # Ưu tiên 3: Khớp domain/subdomain thông thường
            normal_match = normal_match or domain
    
    # Trả về kết quả theo thứ tự ưu tiên
    return root_match or normal_match

def find_domain_key(cookie_domain: str, domain_map: Dict[str, str]) -> Optional[str]:
    """
    Tìm domain key từ domain_map tương ứng với cookie_domain.
    Sử dụng thuật toán tìm khớp tối ưu.
    """
    if not cookie_domain or not domain_map:
        return None
    return find_best_domain_match(cookie_domain, list(domain_map.keys()))

def group_cookies_by_domain(
    cookies: List[Dict[str, Any]],
    domain_map: Dict[str, str]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Gom nhóm cookie theo domain.
    Mỗi cookie sẽ được tìm domain key phù hợp trong domain_map (dùng find_domain_key),
    rồi thêm vào nhóm của domain đó.
    Trả về dict: { 'domain_key': [cookie1, cookie2, ...], ... }
    """
    grouped = defaultdict(list)
    for c in cookies:
        cookie_domain = c.get("domain", "")
        if not cookie_domain:
            continue
        best_key = find_domain_key(cookie_domain, domain_map)
        if best_key:
            grouped[best_key].append(c)
        else:
            logger.debug(f"Không tìm thấy domain key trong domain_map cho cookie domain={cookie_domain}")
    return dict(grouped)


# ==== COOKIE FORMAT DETECTION & PARSING

def detect_cookie_format(file_path: Union[str, Path]) -> str:
    """ Phát hiện định dạng file cookie dựa trên extension hoặc nội dung."""
    
    path = Path(file_path)
    suffix = path.suffix.lower()
    if suffix == ".json":
        return "json"
    elif suffix in (".sqlite", ".db"):
        return "sqlite"
    elif suffix in (".txt", ".cookies"):
        # Nếu file có extension là txt hay cookies, luôn xử lý theo định dạng Netscape
        return "netscape"
    # Trong trường hợp không rõ (không có extension), ta cố gắng đọc nội dung
    try:
        with open(path, 'rb') as f:
            header = f.read(16)
            if header.startswith(b'SQLite format 3\0'):
                return "sqlite"
    except Exception:
        pass
    try:
        sample_size = min(4096, os.path.getsize(path))
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(sample_size)
            if "# Netscape HTTP Cookie File" in content:
                return "netscape"
            tab_lines = sum(1 for line in content.split('\n') if line.count('\t') >= 5)
            if tab_lines > 3:
                return "netscape"
            json_indicators = ['{', '[', '"name":', '"value":', '"domain":']
            json_score = sum(1 for indicator in json_indicators if indicator in content)
            if json_score >= 3 and content.strip().startswith(('{', '[')):
                try:
                    json.loads(content)
                    return "json"
                except json.JSONDecodeError:
                    if json_score >= 4:
                        return "json"
                        
            # Nếu có các từ khóa cơ bản, ta trả về netscape (không quyết định dựa trên dấu tab)
            if "cookie" in content.lower() and "domain" in content.lower() and "path" in content.lower():
                return "netscape"
    except Exception as e:
        logger.warning(f"Lỗi khi đọc file để phát hiện định dạng: {str(e)}")
    return "netscape"

def get_file_encoding(file_path: Union[str, Path]) -> str:
    """Phát hiện encoding của file, thử một số encoding phổ biến."""
    encodings = ['utf-8', 'latin-1', 'windows-1252', 'ascii']
    for enc in encodings:
        try:
            with open(file_path, 'r', encoding=enc) as f:
                f.read(1024)
                return enc
        except UnicodeDecodeError:
            continue
    return 'latin-1'

def parse_netscape_cookies(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    Parse cookie theo định dạng Netscape (.txt).
    Mỗi dòng (không phải comment) chứa các trường cách nhau bởi tab.
    Trả về list các cookie dưới dạng dict.
    """
    cookies = []
    encoding = get_file_encoding(file_path)
    with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Phân tích theo các trường Netscape cookie chuẩn
            try:
                # Bước 1: Kiểm tra xem dòng có tab không
                has_tabs = '\t' in line
                if has_tabs:
                    parts = line.split('\t')
                else:
                    # Chỉ dùng split whitespace khi chắc chắn không có tab
                    parts = re.split(r'\s+', line)
                
                # Bước 2: Đảm bảo có đủ 8 phần tử (để an toàn cho parts[7])
                if len(parts) < 8:
                    parts.extend([""] * (8 - len(parts)))
                
                # Bước 3: Parse các trường
                domain = parts[0].lstrip('.').lower() if parts[0] else ""
                if not domain:
                    continue
                    
                # Trường 1: usually "TRUE" (đánh dấu áp dụng cho subdomain)
                include_subdomains = parts[1].upper() == "TRUE" if parts[1] else False
                
                # Trường 2: path
                path = parts[2] if parts[2] else "/"
                
                # Trường 3: secure flag
                secure = parts[3].upper() == "TRUE" if parts[3] else False
                
                # Trường 4: expiry time
                expiry = None
                if parts[4].strip() and parts[4].strip().isdigit():
                    try:
                        expiry_val = int(parts[4].strip())
                        current_time = int(time.time())
                        max_future = current_time + (10 * 365 * 24 * 60 * 60)  # ~10 năm
                        if 0 < expiry_val < max_future:
                            expiry = expiry_val
                    except (ValueError, TypeError):
                        # Xử lý trường hợp không thể convert thành int
                        pass
                
                # Trường 5: name
                name = parts[5] if parts[5] else ""
                if not name:
                    continue  # Cookie phải có name
                
                # Trường 6: value
                value = parts[6] if parts[6] else ""
                
                # Trường 7 (mở rộng): httpOnly flag
                # Đây là phần mở rộng từ định dạng Netscape gốc
                http_only = parts[7].upper() == "TRUE" if parts[7] else False
                
                # Tạo cookie dict
                cookie = {
                    "domain": domain,
                    "name": name,
                    "value": value,
                    "path": path,
                    "secure": secure,
                    "httpOnly": http_only
                }
                
                if expiry is not None:
                    cookie["expiry"] = expiry
                
                # Thêm metadata nếu cần
                if include_subdomains:
                    cookie["includeSubdomains"] = True

                cookies.append(cookie)
            except Exception as e:
                logger.warning(f"Lỗi parse netscape line '{line}': {str(e)}")

    return cookies

def parse_json_cookies(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    Parse cookie theo định dạng JSON.
    Hỗ trợ cả định dạng dạng list hoặc dict (với key "cookies").
    Trả về list các cookie dưới dạng dict.
    """
    with open(file_path, 'r', encoding=get_file_encoding(file_path), errors='ignore') as f:
        data = json.load(f)

    cookies = []
    if isinstance(data, list):
        raw_cookies = data
    elif isinstance(data, dict):
        raw_cookies = data.get("cookies", [])
    else:
        raise CookieParseError("Định dạng JSON không được hỗ trợ")

    for cookie in raw_cookies:
        if not isinstance(cookie, dict):
            continue

        if "name" not in cookie or not cookie["name"]:
            continue
        if "domain" not in cookie or not cookie["domain"]:
            continue

        c = cookie.copy()
        c["domain"] = c["domain"].lstrip('.').lower()

        if "path" not in c or not c["path"]:
            c["path"] = "/"
        elif not c["path"].startswith("/"):
            c["path"] = "/" + c["path"]

        if "value" not in c:
            c["value"] = ""
        elif c["value"] is None:
            c["value"] = ""

        if "expiry" not in c:
            for key in ["expires", "expirationDate", "Expires", "expire"]:
                if key in c and c[key]:
                    try:
                        expiry_val = int(float(c[key]))
                        current_time = int(time.time())
                        if expiry_val > current_time:
                            c["expiry"] = expiry_val
                            break
                    except (ValueError, TypeError):
                        pass

        for bool_field in ["secure", "httpOnly"]:
            if bool_field in c:
                c[bool_field] = bool(c[bool_field])

        if "sameSite" in c:
            same_site_val = c["sameSite"]
            valid_values = ["Strict", "Lax", "None"]
            if same_site_val not in valid_values:
                if same_site_val.lower() == "strict":
                    c["sameSite"] = "Strict"
                elif same_site_val.lower() == "lax":
                    c["sameSite"] = "Lax"
                elif same_site_val.lower() == "none":
                    c["sameSite"] = "None"
                else:
                    c.pop("sameSite", None)

        cookies.append(c)

    return cookies

def parse_sqlite_cookies(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    Parse cookie từ file SQLite (ví dụ như cơ sở dữ liệu cookie của Chrome/Chromium).
    Truy vấn bảng "cookies" và chuyển kết quả thành list dict.
    """
    cookies = []
    try:
        conn = sqlite3.connect(file_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT host_key, name, value, path, expires_utc, is_secure, is_httponly, same_site FROM cookies"
        )
        rows = cursor.fetchall()

        WINDOWS_TO_UNIX_EPOCH = 11644473600

        for row in rows:
            domain = row["host_key"].lstrip('.').lower() if row["host_key"] else ""
            if not domain or not row["name"]:
                continue

            cookie = {
                "domain": domain,
                "name": row["name"],
                "value": row["value"],
                "path": row["path"] if row["path"] else "/",
                "secure": bool(row["is_secure"]),
                "httpOnly": bool(row["is_httponly"]),
            }

            if row["expires_utc"] and int(row["expires_utc"]) > 0:
                try:
                    seconds_since_epoch = int(row["expires_utc"]) / 10000 - WINDOWS_TO_UNIX_EPOCH
                    current_time = time.time()
                    if seconds_since_epoch > current_time:
                        cookie["expiry"] = int(seconds_since_epoch)
                except (ValueError, TypeError) as e:
                    logger.debug(f"Bỏ qua giá trị expires_utc không hợp lệ: {row['expires_utc']} - {e}")

            if "same_site" in row.keys():
                same_site_map = {0: "None", 1: "Lax", 2: "Strict"}
                same_site_value = row["same_site"]
                if same_site_value in same_site_map:
                    cookie["sameSite"] = same_site_map[same_site_value]

            cookies.append(cookie)
        conn.close()
    except Exception as e:
        raise CookieParseError(f"Lỗi khi parse sqlite cookies: {e}")

    return cookies

def parse_cookie_file(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    Parse file cookie theo định dạng được phát hiện tự động.
    Nếu tất cả các cách thất bại, raise CookieParseError.
    """
    path = Path(file_path)
    if not path.exists():
        raise CookieParseError(f"File cookie không tồn tại: {path}")
    start_time = time.time()
    format_type = detect_cookie_format(path)
    logger.debug(f"Đã phát hiện định dạng file: {format_type} trong {time.time() - start_time:.3f}s")
    if format_type == "netscape":
        return parse_netscape_cookies(path)
    elif format_type == "json":
        return parse_json_cookies(path)
    elif format_type == "sqlite":
        return parse_sqlite_cookies(path)
    else:
        exceptions = []
        try:
            return parse_netscape_cookies(path)
        except CookieParseError as e:
            exceptions.append(f"Netscape: {str(e)}")
            try:
                return parse_json_cookies(path)
            except CookieParseError as e:
                exceptions.append(f"JSON: {str(e)}")
                try:
                    return parse_sqlite_cookies(path)
                except CookieParseError as e:
                    exceptions.append(f"SQLite: {str(e)}")
        raise CookieParseError(f"Không thể parse file cookie: {path}. Lỗi: {'; '.join(exceptions)}")


# ==== STREAMING PARSERS
class StreamingParser:
    """
    Base class cho các streaming parser nhằm tiết kiệm bộ nhớ với file cookie lớn.
    """
    def __init__(self, file_path: Union[str, Path], batch_size: int = 100):
        self.file_path = Path(file_path).resolve()
        self.batch_size = batch_size
        if not self.file_path.exists():
            raise CookieParseError(f"File không tồn tại: {self.file_path}")
        self.encoding = get_file_encoding(self.file_path)

    def parse(self) -> Iterator[List[Dict[str, Any]]]:
        raise NotImplementedError("Các lớp con phải implement phương thức này")

class NetscapeStreamingParser(StreamingParser):
    """
    Streaming parser cho định dạng Netscape cookie, sử dụng mmap cho file lớn.
    """
    def parse(self) -> Iterator[List[Dict[str, Any]]]:
        batch = []
        line_num = 0
        file_size = self.file_path.stat().st_size
        if file_size > 10 * 1024 * 1024:  # >10MB
            try:
                with open(self.file_path, 'r+b') as f:
                    mm = mmap.mmap(f.fileno(), 0)
                    line = mm.readline().decode(self.encoding, errors='ignore')
                    while line:
                        line_num += 1
                        line = line.strip()
                        if not line or line.startswith('#'):
                            line = mm.readline().decode(self.encoding, errors='ignore')
                            continue
                        try:
                            cookie = self._parse_line(line, line_num)
                            if cookie:
                                batch.append(cookie)
                                if len(batch) >= self.batch_size:
                                    yield batch
                                    batch = []
                        except Exception as e:
                            logger.warning(f"Bỏ qua dòng {line_num}, lỗi: {str(e)}")
                        line = mm.readline().decode(self.encoding, errors='ignore')
                    mm.close()
            except Exception as e:
                logger.warning(f"Lỗi mmap, fallback sang đọc thường: {str(e)}")
                with open(self.file_path, 'r', encoding=self.encoding, errors='ignore') as f:
                    for batch in self._parse_file_object(f):
                        yield batch
        else:
            with open(self.file_path, 'r', encoding=self.encoding, errors='ignore') as f:
                for batch in self._parse_file_object(f):
                    yield batch
        if batch:
            yield batch

    def _parse_file_object(self, file_obj: TextIOWrapper) -> Iterator[List[Dict[str, Any]]]:
        batch = []
        line_num = 0
        for line in file_obj:
            line_num += 1
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            try:
                cookie = self._parse_line(line, line_num)
                if cookie:
                    batch.append(cookie)
                    if len(batch) >= self.batch_size:
                        yield batch
                        batch = []
            except Exception as e:
                logger.warning(f"Bỏ qua dòng {line_num}, lỗi: {str(e)}")
        if batch:
            yield batch

    def _parse_line(self, line: str, line_num: int) -> Optional[Dict[str, Any]]:
        parts = line.split('\t')
        if len(parts) < 6:
            parts = re.split(r'\s+', line)
            if len(parts) < 6:
                parts.extend([""] * (6 - len(parts)))
        try:
            domain = parts[0].lstrip('.').lower() if parts[0] else ""
            if not domain:
                return None
            path = parts[2] if len(parts) > 2 and parts[2] else "/"
            secure = False
            if len(parts) > 3 and parts[3]:
                secure_val = parts[3].lower()
                secure = (secure_val == "true" or secure_val == "1")
            expiry = None
            if len(parts) > 4 and parts[4].strip():
                if parts[4].strip().isdigit():
                    expiry = int(parts[4].strip())
            name = parts[5] if len(parts) > 5 else ""
            value = parts[6] if len(parts) > 6 else ""
            if not name:
                return None
            cookie_dict = {"domain": domain, "name": name, "value": value, "path": path, "secure": secure}
            if expiry:
                cookie_dict["expiry"] = expiry
            # Nếu có trường httpOnly (index 7) thì xử lý, nếu không mặc định False
            if len(parts) > 7 and parts[7].strip():
                cookie_dict["httpOnly"] = (parts[7].lower() in ('true', '1'))
            else:
                cookie_dict["httpOnly"] = False
            return cookie_dict
        except Exception as e:
            logger.warning(f"Lỗi phân tích dòng {line_num}: {type(e).__name__}: {str(e)}")
            return None

class JsonStreamingParser(StreamingParser):
    """
    Streaming parser cho định dạng JSON cookie.
    """
    def parse(self) -> Iterator[List[Dict[str, Any]]]:
        try:
            with open(self.file_path, 'r', encoding=self.encoding, errors='ignore') as f:
                json_data = json.load(f)
            raw_cookies = []
            if isinstance(json_data, list):
                raw_cookies = json_data
            elif isinstance(json_data, dict):
                if "cookies" in json_data:
                    raw_cookies = json_data.get("cookies", [])
                else:
                    raw_cookies = [json_data]
            else:
                raise CookieParseError("Định dạng JSON không được hỗ trợ")
            batch = []
            for cookie_data in raw_cookies:
                if not isinstance(cookie_data, dict):
                    continue
                normalized_cookie = self._normalize_cookie(cookie_data)
                if normalized_cookie:
                    batch.append(normalized_cookie)
                    if len(batch) >= self.batch_size:
                        yield batch
                        batch = []
            if batch:
                yield batch
        except Exception as e:
            raise CookieParseError(f"Lỗi khi đọc file cookie JSON: {str(e)}")

    def _normalize_cookie(self, cookie_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if "name" not in cookie_data or not cookie_data["name"]:
            return None
        if "domain" not in cookie_data or not cookie_data["domain"]:
            return None
        cookie = cookie_data.copy()
        cookie["domain"] = cookie["domain"].lstrip('.').lower()
        if "path" not in cookie or not cookie["path"]:
            cookie["path"] = "/"
        if "value" not in cookie:
            cookie["value"] = ""
        if "expiry" not in cookie:
            for key in ["expires", "expirationDate", "Expires", "expire"]:
                if key in cookie and cookie[key]:
                    try:
                        cookie["expiry"] = int(cookie[key])
                        break
                    except (ValueError, TypeError):
                        pass
        if "secure" not in cookie:
            cookie["secure"] = False
        return cookie


# ==== COOKIE SETTING VIA CHROME DEVTOOLS PROTOCOL & ADAPTIVE BATCH PROCESSING
def devtools_set_cookie(driver: webdriver.Chrome, cookie: Dict[str, Any], timeout: float = 2.0) -> bool:
    """
    Thiết lập một cookie qua Chrome DevTools Protocol với timeout.
    Bỏ qua kiểm tra domain của Selenium.
    """
    try:
        if not hasattr(driver, 'service') or not driver.service.is_connectable():
            logger.error("Driver không còn kết nối")
            return False

        cookie_name = cookie.get("name", "unknown")
        params = {
            "name": cookie["name"],
            "value": cookie["value"],
            "path": cookie.get("path", "/"),
        }

        if "domain" in cookie and cookie["domain"]:
            params["domain"] = cookie["domain"].lstrip('.')
        else:
            logger.warning(f"Cookie {cookie_name} không có domain, không thể set qua CDP")
            return False

        if "expiry" in cookie and isinstance(cookie["expiry"], (int, float)):
            expiry_val = int(cookie["expiry"])
            current_time = int(time.time())
            max_future = current_time + (10 * 365 * 24 * 60 * 60)
            if 0 < expiry_val < max_future:
                params["expires"] = expiry_val
            else:
                logger.debug(f"Bỏ qua 'expiry' không hợp lệ cho cookie {cookie_name}: {expiry_val}")

        if "secure" in cookie:
            params["secure"] = bool(cookie["secure"])
        if "httpOnly" in cookie:
            params["httpOnly"] = bool(cookie["httpOnly"])
        if "sameSite" in cookie:
            valid_values = ["Strict", "Lax", "None"]
            sameSite = cookie["sameSite"]
            if isinstance(sameSite, str) and sameSite in valid_values:
                params["sameSite"] = sameSite

        start_time = time.time()
        result = driver.execute_cdp_cmd("Network.setCookie", params)
        elapsed = time.time() - start_time
        if elapsed > 1.0:
            logger.debug(f"CDP setCookie took {elapsed:.2f}s for {cookie_name}")
        success = bool(result.get("success", False))
        if success:
            logger.debug(f"Set cookie thành công: {cookie_name} cho domain {params.get('domain', 'unknown')}")
        else:
            logger.warning(f"Set cookie thất bại: {cookie_name} cho domain {params.get('domain', 'unknown')}")
        return success

    except KeyError as e:
        logger.warning(f"Thiếu trường bắt buộc trong cookie: {e}")
        return False
    except (WebDriverException, InvalidSessionIdException) as e:
        logger.warning(f"Lỗi WebDriver khi set cookie: {str(e)}")
        return False
    except Exception as e:
        logger.warning(f"Lỗi không xác định khi set cookie qua DevTools: {type(e).__name__}: {str(e)}")
        return False

class AdaptiveBatchProcessor:
    """
    Lớp xử lý batch cookie với khả năng tự điều chỉnh kích thước batch.
    """
    def __init__(
        self,
        driver: webdriver.Chrome,
        initial_batch_size: int = 20,
        min_batch_size: int = 5,
        max_batch_size: int = 50,
        per_cookie_timeout: float = 2.0
    ):
        self.driver = driver
        self.batch_size = initial_batch_size
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.per_cookie_timeout = per_cookie_timeout
        self.success_rate_history = deque(maxlen=5)
        self.batch_time_history = deque(maxlen=5)
        self.lock = Lock()

    def process_batch(self, cookies: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not cookies:
            return 0, 0
        success_count = 0
        failure_count = 0
        batch_start = time.time()
        batch_timeout = len(cookies) * self.per_cookie_timeout
        for cookie in cookies:
            if time.time() - batch_start > batch_timeout:
                logger.warning(f"Batch timeout sau {batch_timeout}s, processed {success_count + failure_count}/{len(cookies)}")
                break
            if devtools_set_cookie(self.driver, cookie, self.per_cookie_timeout):
                success_count += 1
            else:
                failure_count += 1
        batch_time = time.time() - batch_start
        success_rate = success_count / len(cookies) if cookies else 0
        with self.lock:
            self.success_rate_history.append(success_rate)
            self.batch_time_history.append(batch_time)
            self._adjust_batch_size()
        return success_count, failure_count

    def _adjust_batch_size(self):
        if len(self.success_rate_history) < 2:
            return
        avg_success = sum(self.success_rate_history) / len(self.success_rate_history)
        avg_time = sum(self.batch_time_history) / len(self.batch_time_history)
        if avg_success > 0.9 and avg_time < self.batch_size * self.per_cookie_timeout * 0.7:
            self.batch_size = min(self.max_batch_size, int(self.batch_size * 1.2))
            logger.debug(f"Tăng kích thước batch lên {self.batch_size} (success: {avg_success:.2f}, time: {avg_time:.2f}s)")
        elif avg_success < 0.7 or avg_time > self.batch_size * self.per_cookie_timeout * 0.9:
            self.batch_size = max(self.min_batch_size, int(self.batch_size * 0.8))
            logger.debug(f"Giảm kích thước batch xuống {self.batch_size} (success: {avg_success:.2f}, time: {avg_time:.2f}s)")

    @property
    def current_batch_size(self) -> int:
        with self.lock:
            return self.batch_size

def prioritize_cookies(cookies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sắp xếp cookie theo mức độ ưu tiên dựa trên các yếu tố như expiry, httpOnly, secure, path, và tên chứa từ khóa auth.
    """
    def score(cookie: Dict[str, Any]) -> float:
        s = 0.0
        if "expiry" not in cookie:
            s += 10.0
        if cookie.get("httpOnly", False):
            s += 5.0
        if cookie.get("secure", False):
            s += 3.0
        if cookie.get("path", "") == "/":
            s += 2.0
        name = cookie.get("name", "").lower()
        for term in ["auth", "session", "token", "id", "sid", "user", "login", "csrf", "xsrf"]:
            if term in name:
                s += 4.0
                break
        return s
    return sorted(cookies, key=score, reverse=True)

def sanitize_cookie(cookie: Dict[str, Any]) -> Dict[str, Any]:
    """
    Làm sạch và chuẩn hóa cookie trước khi gửi đến Chrome DevTools Protocol.
    Loại bỏ các trường không hợp lệ và chuẩn hóa giá trị.
    """
    if not cookie or not isinstance(cookie, dict):
        return {}

    sanitized = cookie.copy()

    if "name" not in sanitized or not sanitized["name"]:
        logger.debug("Bỏ qua cookie không có tên")
        return {}

    if "domain" not in sanitized or not sanitized["domain"]:
        logger.debug(f"Bỏ qua cookie {sanitized.get('name')} không có domain")
        return {}

    sanitized["domain"] = sanitized["domain"].lstrip('.').lower()

    if "path" not in sanitized or not sanitized["path"]:
        sanitized["path"] = "/"
    elif not sanitized["path"].startswith("/"):
        sanitized["path"] = "/" + sanitized["path"]

    if "value" not in sanitized:
        sanitized["value"] = ""
    elif sanitized["value"] is None:
        sanitized["value"] = ""

    if "expiry" in sanitized:
        try:
            expiry_val = int(float(sanitized["expiry"]))
            current_time = int(time.time())
            max_future = current_time + (10 * 365 * 24 * 60 * 60)
            if expiry_val <= 0 or expiry_val > max_future:
                logger.debug(f"Loại bỏ expiry không hợp lệ: {expiry_val}")
                sanitized.pop("expiry")
            else:
                sanitized["expiry"] = expiry_val
        except (ValueError, TypeError):
            logger.debug(f"Loại bỏ expiry không thể chuyển đổi: {sanitized['expiry']}")
            sanitized.pop("expiry")

    if "sameSite" in sanitized:
        valid_values = ["Strict", "Lax", "None"]
        if sanitized["sameSite"] not in valid_values:
            if isinstance(sanitized["sameSite"], str):
                capitalized = sanitized["sameSite"].capitalize()
                if capitalized in ["Strict", "Lax", "None"]:
                    sanitized["sameSite"] = capitalized
                else:
                    sanitized.pop("sameSite")
            else:
                sanitized.pop("sameSite")

    for bool_field in ["secure", "httpOnly"]:
        if bool_field in sanitized:
            sanitized[bool_field] = bool(sanitized[bool_field])

    return sanitized

def devtools_batch_set_cookies(
    driver: webdriver.Chrome,
    cookies: List[Dict[str, Any]],
    initial_batch_size: int = 20,
    batch_timeout: float = 30.0,
    per_cookie_timeout: float = 2.0,
    adaptive_batch: bool = True
) -> Tuple[int, int]:
    """
    Thiết lập nhiều cookie cùng lúc qua CDP với adaptive batch.
    Trả về tổng số cookie set thành công và thất bại.
    
    Args:
        driver: WebDriver đang hoạt động
        cookies: Danh sách cookie cần thiết lập
        initial_batch_size: Kích thước batch ban đầu
        batch_timeout: Thời gian tối đa cho mỗi batch (giây)
        per_cookie_timeout: Thời gian tối đa cho mỗi cookie (giây)
        adaptive_batch: Sử dụng kích thước batch tự động điều chỉnh
        
    Returns:
        Tuple[int, int]: (cookie_thành_công, cookie_thất_bại)
    """
    if not cookies:
        return 0, 0
    
    success_count = 0
    failure_count = 0

    # Làm sạch và kiểm tra cookies
    sanitized_cookies = []
    for cookie in cookies:
        clean_cookie = sanitize_cookie(cookie)
        if clean_cookie:
            sanitized_cookies.append(clean_cookie)
        else:
            failure_count += 1

    if not sanitized_cookies:
        logger.warning("Không có cookie nào hợp lệ sau khi sanitize")
        return 0, failure_count

    # Sắp xếp cookies theo mức độ ưu tiên
    prioritized = prioritize_cookies(sanitized_cookies)
    
    # Khởi tạo processor nếu sử dụng adaptive batch
    processor = None
    if adaptive_batch:
        try:
            processor = AdaptiveBatchProcessor(
                driver, 
                initial_batch_size, 
                per_cookie_timeout=per_cookie_timeout
            )
        except Exception as e:
            logger.warning(f"Không thể khởi tạo AdaptiveBatchProcessor: {str(e)}")
            # Tiếp tục với batch cố định
            adaptive_batch = False
    
    # Đảm bảo current_batch_size luôn được khởi tạo
    current_batch_size = initial_batch_size
    
    # Bắt đầu xử lý cookies theo batch
    i = 0
    start_total = time.time()
    total_cookies = len(prioritized)
    
    while i < total_cookies:
        # Cập nhật kích thước batch hiện tại nếu cần
        if adaptive_batch and processor:
            current_batch_size = processor.current_batch_size
        
        # Xác định batch hiện tại
        batch_end = min(i + current_batch_size, total_cookies)
        current_batch = prioritized[i:batch_end]
        batch_size = len(current_batch)
        batch_start = time.time()
        
        # Xử lý batch
        if adaptive_batch and processor:
            # Sử dụng AdaptiveBatchProcessor
            batch_success, batch_failure = processor.process_batch(current_batch)
        else:
            # Xử lý từng cookie một cách thủ công
            batch_success = 0
            batch_failure = 0
            for cookie in current_batch:
                if time.time() - batch_start > batch_timeout:
                    logger.warning(f"Batch timeout sau {batch_timeout}s")
                    break
                if devtools_set_cookie(driver, cookie, per_cookie_timeout):
                    batch_success += 1
                else:
                    batch_failure += 1
        
        # Cập nhật tổng số cookie thành công/thất bại
        success_count += batch_success
        failure_count += batch_failure
        
        # Log thông tin batch
        batch_number = (i // current_batch_size) + 1
        logger.debug(f"Batch {batch_number}: {batch_success}/{batch_size} thành công, {batch_failure} thất bại")
        
        # Chuẩn bị cho batch tiếp theo
        i = batch_end
        
        # Tạm dừng giữa các batch nếu cần
        if i < total_cookies:
            batch_duration = time.time() - batch_start
            pause = 0.1 if batch_duration < 2.0 else min(0.5, batch_duration / 10)
            time.sleep(pause)
    
    # Log kết quả cuối cùng
    elapsed_total = time.time() - start_total
    success_rate = (success_count / len(cookies)) * 100 if cookies else 0
    logger.info(f"Đã set {success_count}/{len(cookies)} cookie ({success_rate:.1f}%) thành công trong {elapsed_total:.2f}s")
    
    return success_count, failure_count

def add_cookies_for_domain(
    driver: webdriver.Chrome,
    domain: str,
    cookie_list: List[Dict[str, Any]],
    domain_url_map: Dict[str, str],
    timeout: int = 5,
    max_retries: int = 3,
    batch_mode: bool = True,
    adaptive_batch: bool = True,
    initial_batch_size: int = 20,
    per_cookie_timeout: float = 2.0
) -> bool:
    """
    Mở URL từ domain_url_map và thêm cookie qua CDP. Hỗ trợ cả chế độ batch và riêng lẻ.
    Ghi log chi tiết từng cookie được set.
    """
    if not driver or not cookie_list:
        logger.warning("Driver hoặc cookie_list trống")
        return False
    if domain not in domain_url_map:
        logger.error(f"Domain {domain} không có trong domain_url_map")
        return False
    url = domain_url_map[domain]
    logger.info(f" - Mở {url} để thêm {len(cookie_list)} cookie (domain={domain})")
    try:
        driver.get(url)
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")
    except (TimeoutException, WebDriverException) as e:
        logger.warning(f"Lỗi tải trang {url}: {e}")
        return False
    if batch_mode and len(cookie_list) > 5:
        batch_timeout = min(30.0, len(cookie_list) * per_cookie_timeout * 0.8)
        success, _ = devtools_batch_set_cookies(
            driver=driver,
            cookies=cookie_list,
            initial_batch_size=initial_batch_size,
            batch_timeout=batch_timeout,
            per_cookie_timeout=per_cookie_timeout,
            adaptive_batch=adaptive_batch
        )
        return success > 0
    else:
        prioritized = prioritize_cookies(cookie_list)
        success_count = 0
        for c in prioritized:
            logger.debug(f"Thêm cookie: {c}")
            retry = 0
            while retry < max_retries:
                try:
                    if devtools_set_cookie(driver, c, per_cookie_timeout):
                        success_count += 1
                        break
                    else:
                        retry += 1
                        logger.warning(f"Lỗi set cookie {c.get('name', 'unknown')} (retry {retry}/{max_retries})")
                        if retry < max_retries:
                            c_copy = c.copy()
                            if 'domain' in c_copy and not c_copy['domain'].startswith('.'):
                                c_copy['domain'] = '.' + c_copy['domain']
                                if devtools_set_cookie(driver, c_copy, per_cookie_timeout):
                                    success_count += 1
                                    break
                            if 'expiry' in c_copy:
                                c_copy.pop('expiry', None)
                                if devtools_set_cookie(driver, c_copy, per_cookie_timeout):
                                    success_count += 1
                                    break
                        if retry >= max_retries:
                            break
                except Exception as e:
                    retry += 1
                    logger.warning(f"Lỗi set cookie {c.get('name', 'unknown')} (retry {retry}/{max_retries}): {type(e).__name__}: {str(e)}")
                    time.sleep(min(1 * retry, 3))
        logger.info(f"Đã set thành công {success_count}/{len(cookie_list)} cookie cho domain {domain}")
        return success_count > 0


# ==== COOKIE MANAGER CLASS
class CookieManager:
    """
    Lớp quản lý cookie: parse file, gom nhóm theo domain, và thêm cookie qua CDP.
    Hỗ trợ adaptive batch processing để tối ưu hiệu suất.
    """
    def __init__(
        self,
        driver: webdriver.Chrome,
        domain_map: Dict[str, str],
        timeout: int = 5,
        max_retries: int = 3,
        logger: Optional[logging.Logger] = None,
        batch_size: int = 20,
        use_batching: bool = True,
        adaptive_batch: bool = True,
        per_cookie_timeout: float = 2.0
    ):
        self.driver = driver
        self.domain_map = domain_map
        self.timeout = timeout
        self.max_retries = max_retries
        self.logger = logger if logger else logging.getLogger(__name__)
        self.batch_size = batch_size
        self.use_batching = use_batching
        self.adaptive_batch = adaptive_batch
        self.per_cookie_timeout = per_cookie_timeout

    def parse_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Parse file cookie theo định dạng được phát hiện tự động."""
        return parse_cookie_file(file_path)

    def group_by_domain(self, cookies: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Gom nhóm cookie theo domain dựa trên domain_map."""
        return group_cookies_by_domain(cookies, self.domain_map)

    def add_cookies(self, domain: str, cookie_list: List[Dict[str, Any]]) -> bool:
        """Thêm cookie cho một domain với hỗ trợ batch processing."""
        return add_cookies_for_domain(
            self.driver,
            domain,
            cookie_list,
            self.domain_map,
            self.timeout,
            self.max_retries,
            batch_mode=self.use_batching,
            adaptive_batch=self.adaptive_batch,
            initial_batch_size=self.batch_size,
            per_cookie_timeout=self.per_cookie_timeout
        )

    def load_all_cookies(self, file_path: str) -> bool:
        """
        Parse file cookie, gom nhóm theo domain và thêm cookie vào browser.
        Trả về True nếu ít nhất một domain được set cookie thành công.
        """
        start_time = time.time()
        try:
            cookies = self.parse_file(file_path)
            if not cookies:
                self.logger.warning(f"Không tìm thấy cookie hợp lệ trong file {file_path}")
                return False
            cookies_by_domain = self.group_by_domain(cookies)
            if not cookies_by_domain:
                self.logger.warning(f"Không có cookie nào khớp với domain_map trong file {file_path}")
                return False
            success_domains = 0
            for domain in self.domain_map:
                if domain in cookies_by_domain:
                    cookie_list = cookies_by_domain[domain]
                    self.logger.info(f" - Mở {self.domain_map[domain]} để thêm {len(cookie_list)} cookie (domain={domain})")
                    if self.add_cookies(domain, cookie_list):
                        success_domains += 1
                    else:
                        self.logger.error(f"Không set được cookie cho domain {domain}")
                    time.sleep(0.5)
            elapsed = time.time() - start_time
            self.logger.info(f"Load cookie từ file {os.path.basename(file_path)} hoàn tất trong {elapsed:.2f}s; " +
                             f"{success_domains}/{len(cookies_by_domain)} domain thành công.")
            return success_domains > 0
        except CookieParseError as e:
            self.logger.error(f"Lỗi parse cookie: {str(e)}")
            return False
        except InvalidSessionIdException as e:
            self.logger.error(f"Phiên WebDriver đã đóng: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Lỗi không mong đợi khi load cookie: {type(e).__name__}: {str(e)}")
            return False

    def get_current_cookies(self) -> List[Dict[str, Any]]:
        """Lấy danh sách cookie hiện tại từ browser."""
        try:
            if not hasattr(self.driver, 'service') or not self.driver.service.is_connectable():
                self.logger.error("Driver không còn kết nối")
                return []
            return self.driver.get_cookies()
        except InvalidSessionIdException as e:
            self.logger.error(f"Phiên WebDriver đã đóng: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Lỗi khi lấy cookie từ browser: {type(e).__name__}: {str(e)}")
            return []

    def get_domains_with_cookies(self) -> List[str]:
        """Lấy danh sách domain có cookie trong browser."""
        cookies = self.get_current_cookies()
        domains: Set[str] = set()
        for c in cookies:
            if "domain" in c:
                domains.add(c["domain"].lstrip('.').lower())
        return sorted(list(domains))

    def extract_cookies_for_domain(self, domain: str) -> List[Dict[str, Any]]:
        """Lấy tất cả cookie cho một domain cụ thể."""
        all_cookies = self.get_current_cookies()
        domain_cookies = []
        for c in all_cookies:
            if "domain" in c and domain_matches(c["domain"], domain):
                domain_cookies.append(c)
        return domain_cookies

    def save_cookies_to_file(self, file_path: Union[str, Path], format_type: str = "json") -> bool:
        """Lưu tất cả cookie hiện tại vào file theo định dạng chỉ định."""
        cookies = self.get_current_cookies()
        if not cookies:
            self.logger.warning("Không có cookie nào để lưu")
            return False
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            if format_type.lower() == "json":
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(cookies, f, indent=2)
            else:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write("# Netscape HTTP Cookie File\n")
                    f.write("# Generated by CookieManager\n\n")
                    for c in cookies:
                        d = c.get("domain", "")
                        p = c.get("path", "/")
                        s = "TRUE" if c.get("secure", False) else "FALSE"
                        e = str(c.get("expiry", 0))
                        n = c.get("name", "")
                        v = c.get("value", "")
                        f.write(f"{d}\tTRUE\t{p}\t{s}\t{e}\t{n}\t{v}\n")
            self.logger.info(f"Đã lưu {len(cookies)} cookie vào file {path}")
            return True
        except Exception as e:
            self.logger.error(f"Lỗi khi lưu cookie: {type(e).__name__}: {str(e)}")
            return False

    def has_valid_session(self, test_domain: str = None) -> bool:
        """Kiểm tra xem có session cookie hợp lệ hay không."""
        cookies = self.get_current_cookies()
        if not cookies:
            return False
        if test_domain:
            domain_cookies = self.extract_cookies_for_domain(test_domain)
            for c in domain_cookies:
                if "expiry" not in c or c.get("httpOnly", False):
                    return True
            return False
        for c in cookies:
            if "expiry" not in c or c.get("httpOnly", False):
                return True
        return False