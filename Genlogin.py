"""
Genlogin.py - Module tương tác với GenLogin API để quản lý profiles và điều khiển session browser.
Version: 2.2.0 (Đã tối ưu hóa và đồng bộ với API gốc)

Các tính năng chính:
- Gọi các API của GenLogin: getProfile, getProfiles, getWsEndpoint, runProfile, stopProfile, getProfilesRunning.
- Cơ chế retry thông minh với exponential backoff và jitter.
- Quản lý trạng thái profile thông qua caching (ProfileStatus, ProfileStatusTracker).
- Custom exceptions giúp phân loại lỗi và xử lý các tình huống đặc biệt.
"""

import logging
import time
import random
import threading
from datetime import datetime
from enum import Enum, auto
from typing import Dict, List, Any, Optional

import requests
from requests.exceptions import RequestException, ConnectionError, Timeout, HTTPError, ReadTimeout
from global_state import request_shutdown, is_shutdown_requested

# -------------------------------
# Custom Exception & Error Types
# -------------------------------

class GenloginErrorType(Enum):
    CONNECTION = auto()
    AUTHENTICATION = auto()
    PROFILE_NOT_FOUND = auto()
    PROFILE_RUNNING = auto()
    PROFILE_LOCKED = auto()
    SERVER_ERROR = auto()
    TIMEOUT = auto()
    UNKNOWN = auto()

class GenloginError(Exception):
    """Ngoại lệ chung cho GenLogin với thông tin lỗi chi tiết."""
    def __init__(self, message: str, error_type: GenloginErrorType = GenloginErrorType.UNKNOWN, status_code: Optional[int] = None):
        self.message = message
        self.error_type = error_type
        self.status_code = status_code
        super().__init__(message)

class GenloginConnectionError(GenloginError):
    """Ngoại lệ khi không thể kết nối đến GenLogin."""
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message, GenloginErrorType.CONNECTION, status_code)

class GenloginProfileError(GenloginError):
    """Ngoại lệ khi có vấn đề liên quan đến profile (không tồn tại, đang chạy trên thiết bị khác, v.v.)."""
    def __init__(self, message: str, error_type: GenloginErrorType = GenloginErrorType.UNKNOWN, status_code: Optional[int] = None):
        super().__init__(message, error_type, status_code)

# -------------------------------
# Profile Status Tracking
# -------------------------------

class ProfileStatus:
    """
    Lưu trữ trạng thái của một profile, bao gồm wsEndpoint và các thông tin cập nhật.
    """
    def __init__(self, profile_id: str):
        self.profile_id = profile_id
        self.is_running = False
        self.ws_endpoint = None
        self.running_device = None
        self.last_checked = datetime.now()
        self.last_started = None
        self.last_stopped = None

    def update(self, is_running: bool, ws_endpoint: Optional[str] = None, device_info: Optional[str] = None):
        self.is_running = is_running
        self.ws_endpoint = ws_endpoint
        self.running_device = device_info
        if is_running and not self.last_started:
            self.last_started = datetime.now()
        elif not is_running:
            self.last_stopped = datetime.now()
        self.last_checked = datetime.now()

    def is_stale(self, ttl: int = 60) -> bool:
        return (datetime.now() - self.last_checked).total_seconds() > ttl

class ProfileStatusTracker:
    """
    Quản lý cache trạng thái của các profile, giúp giảm số lần gọi API.
    """
    def __init__(self, ttl_seconds: int = 60):
        self.statuses: Dict[str, ProfileStatus] = {}
        self.ttl_seconds = ttl_seconds
        self.lock = threading.RLock()

    def get_status(self, profile_id: str) -> ProfileStatus:
        with self.lock:
            if profile_id not in self.statuses:
                self.statuses[profile_id] = ProfileStatus(profile_id)
            return self.statuses[profile_id]

    def update_status(self, profile_id: str, is_running: bool, ws_endpoint: Optional[str] = None, device_info: Optional[str] = None):
        with self.lock:
            status = self.get_status(profile_id)
            status.update(is_running, ws_endpoint, device_info)

    def clear_status(self, profile_id: str):
        with self.lock:
            if profile_id in self.statuses:
                del self.statuses[profile_id]

# -------------------------------
# Genlogin Class
# -------------------------------

class Genlogin:
    """
    Lớp tương tác với GenLogin API để quản lý profiles và điều khiển session browser.
    Hỗ trợ retry, caching trạng thái profile.
    """
    def __init__(self,
                 api_key: str = "",
                 base_url: str = "http://localhost:55550/backend",
                 request_timeout: int = 30,
                 max_retries: int = 3,
                 retry_delay: float = 1.0,
                 status_cache_ttl: int = 60):
        """
        Khởi tạo client Genlogin với các tùy chọn cấu hình.
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.profiles_url = f"{self.base_url}/profiles"
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(__name__)
        self.status_tracker = ProfileStatusTracker(status_cache_ttl)

    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, data: Optional[Dict] = None,
                      retry_on_codes: Optional[List[int]] = None, use_jitter: bool = True) -> Dict[str, Any]:
        """
        Thực hiện HTTP request với cơ chế retry thông minh.
        """
        # Kiểm tra cờ shutdown đầu tiên
        if is_shutdown_requested():
            self.logger.warning("Phát hiện yêu cầu shutdown, hủy request")
            raise GenloginError("Shutdown requested", GenloginErrorType.CONNECTION)

        if retry_on_codes is None:
            retry_on_codes = [408, 429, 500, 502, 503, 504]
        url = f"{self.profiles_url}/{endpoint.lstrip('/')}"
        retry_count = 0
        last_error = None
        while retry_count <= self.max_retries:
            try:
                self.logger.debug(f"Gửi {method.upper()} request đến {url} (try {retry_count+1})")
                response = requests.request(
                    method=method.lower(),
                    url=url,
                    params=params,
                    json=data,
                    timeout=self.request_timeout
                )
                if response.status_code in retry_on_codes and retry_count < self.max_retries:
                    retry_count += 1
                    wait_time = self.retry_delay * (2 ** (retry_count - 1))
                    if use_jitter:
                        wait_time *= random.uniform(0.8, 1.2)
                    self.logger.warning(f"Nhận mã lỗi {response.status_code}, retry {retry_count}/{self.max_retries} sau {wait_time:.2f}s")
                    time.sleep(wait_time)
                    continue
                if response.status_code >= 400:
                    try:
                        error_data = response.json()
                        if isinstance(error_data, dict):
                            error_msg = error_data.get('message', f"HTTP {response.status_code}")
                        else:
                            error_msg = str(error_data)
                    except Exception:
                        error_msg = f"HTTP {response.status_code}"
                    if response.status_code == 404:
                        raise GenloginProfileError(error_msg, GenloginErrorType.PROFILE_NOT_FOUND, response.status_code)
                    elif response.status_code in [401, 403]:
                        raise GenloginError(error_msg, GenloginErrorType.AUTHENTICATION, response.status_code)
                    elif response.status_code >= 500:
                        raise GenloginError(error_msg, GenloginErrorType.SERVER_ERROR, response.status_code)
                    else:
                        raise GenloginError(error_msg, GenloginErrorType.UNKNOWN, response.status_code)
                response.raise_for_status()
                try:
                    return response.json()
                except Exception as e:
                    raise GenloginError(f"Lỗi parse JSON: {str(e)}", GenloginErrorType.UNKNOWN)
            except RequestException as e:
                last_error = e
                retry_count += 1
                if retry_count <= self.max_retries:
                    wait_time = self.retry_delay * (2 ** (retry_count - 1))
                    if use_jitter:
                        wait_time *= random.uniform(0.8, 1.2)
                    self.logger.warning(f"Request error {type(e).__name__}: {str(e)}, retry {retry_count}/{self.max_retries} sau {wait_time:.2f}s")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Thất bại sau {self.max_retries} lần retry: {str(e)}")
                    raise GenloginError(f"Lỗi request: {str(e)}", GenloginErrorType.CONNECTION)
            except Exception as e:
                self.logger.error(f"Lỗi không xác định: {type(e).__name__}: {str(e)}")
                raise GenloginError(f"Lỗi không xác định: {type(e).__name__}: {str(e)}", GenloginErrorType.UNKNOWN)
        if last_error:
            raise GenloginError(f"Thất bại request sau {self.max_retries} lần: {str(last_error)}", GenloginErrorType.UNKNOWN)
        raise GenloginError("Thất bại không xác định khi thực hiện request", GenloginErrorType.UNKNOWN)

    def getProfile(self, id: str) -> Dict[str, Any]:
        """Lấy thông tin profile theo ID."""
        try:
            response = self._make_request("get", f"{id}")
            return response.get('data', {})
        except GenloginError as e:
            if e.error_type == GenloginErrorType.PROFILE_NOT_FOUND:
                raise GenloginProfileError(f"Profile {id} không tồn tại", GenloginErrorType.PROFILE_NOT_FOUND)
            self.logger.error(f"getProfile error: {str(e)}")
            raise

    def getProfiles(self, offset: int = 0, limit: int = 1000, filter_str: Optional[str] = None) -> Dict[str, Any]:
        """Lấy danh sách profiles với phân trang và tùy chọn lọc."""
        params = {'limit': limit, 'offset': offset}
        if filter_str:
            params['filter'] = filter_str
        try:
            response = self._make_request("get", "", params=params)
            data = response.get('data', {})
            return {
                'profiles': data.get('items', []),
                'pagination': data.get('pagination', {})
            }
        except GenloginError as e:
            self.logger.error(f"getProfiles error: {str(e)}")
            raise

    def getWsEndpoint(self, id: str) -> Dict[str, Any]:
        """Lấy wsEndpoint cho profile theo ID."""
        try:
            return self._make_request("get", f"{id}/ws-endpoint")
        except GenloginError as e:
            if e.error_type == GenloginErrorType.PROFILE_NOT_FOUND:
                raise GenloginProfileError(f"Profile {id} không tồn tại", GenloginErrorType.PROFILE_NOT_FOUND)
            self.logger.error(f"getWsEndpoint error: {str(e)}")
            raise

    def runProfile(self, id: str, force_restart: bool = False) -> Dict[str, Any]:
        """
        Khởi động profile. Nếu profile đã chạy và không cần force_restart,
        trả về wsEndpoint hiện tại.
        """
        try:
            ws_data = self.getWsEndpoint(id)
            ws_endpoint = ws_data.get("data", {}).get("wsEndpoint", "")
            if ws_endpoint and not force_restart:
                self.logger.info(f"Profile {id} đã đang chạy với wsEndpoint: {ws_endpoint}")
                return {'success': True, 'wsEndpoint': ws_endpoint, 'message': 'Profile đã đang chạy'}
            response = self._make_request("put", f"{id}/start")
            if not response.get('success', False):
                error_msg = response.get('message', 'Không rõ lỗi')
                self.logger.error(f"Không thể chạy profile {id}: {error_msg}")
                raise GenloginProfileError(error_msg, GenloginErrorType.PROFILE_RUNNING)
            ws_endpoint = response.get('data', {}).get('wsEndpoint', '')
            if ws_endpoint:
                self.status_tracker.update_status(id, True, ws_endpoint, None)
                return {'success': True, 'wsEndpoint': ws_endpoint, 'message': 'Profile đã khởi động'}
            else:
                raise GenloginProfileError("Không nhận được wsEndpoint sau khi chạy profile", GenloginErrorType.UNKNOWN)
        except GenloginProfileError as e:
            self.logger.error(f"runProfile error: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"runProfile error: {type(e).__name__}: {str(e)}")
            raise GenloginProfileError(f"Lỗi khi khởi động profile {id}: {str(e)}", GenloginErrorType.UNKNOWN)

    def stopProfile(self, id: str) -> Dict[str, Any]:
        """Dừng profile theo ID."""
        try:
            result = self._make_request("put", f"{id}/stop")
            self.status_tracker.update_status(id, False, None, None)
            return result
        except Exception as e:
            self.logger.error(f"stopProfile error: {type(e).__name__}: {str(e)}")
            raise GenloginProfileError(f"Lỗi khi dừng profile {id}: {str(e)}", GenloginErrorType.UNKNOWN)

    def getProfilesRunning(self) -> Dict[str, Any]:
        """Lấy danh sách profile đang chạy."""
        try:
            result = self._make_request("get", "running")
            # Nếu result trả về là list, bọc lại dưới dạng dict có key "data" chứa "items"
            if isinstance(result, dict):
                data = result.get("data")
                if isinstance(data, dict):
                    items = data.get("items", [])
                else:
                    items = []
                    result = {"data": {"items": items}}
            elif isinstance(result, list):
                items = result
                result = {"data": {"items": items}}
            else:
                items = []
                result = {"data": {"items": items}}
            for profile in items:
                if isinstance(profile, dict):
                    pid = profile.get("id")
                    device = profile.get("device", "")
                    self.status_tracker.update_status(pid, True, None, device)
            return result
        except Exception as e:
            self.logger.error(f"getProfilesRunning error: {type(e).__name__}: {str(e)}")
            raise

    def stopBrowser(self, id: str) -> Dict[str, Any]:
        """
        Dừng trình duyệt cho profile cụ thể.
        Phương thức này tương tự với stopProfile nhưng sử dụng endpoint khác.
        """
        try:
            # Có thể API này khác biệt giữa các phiên bản GenLogin
            result = self._make_request("put", f"{id}/stop-browser")
            self.status_tracker.update_status(id, False, None, None)
            return result
        except Exception as e:
            # Nếu API không tồn tại, fallback về stopProfile
            self.logger.warning(f"stopBrowser không khả dụng, fallback về stopProfile: {str(e)}")
            return self.stopProfile(id)

# Định nghĩa __all__ để xuất các lớp cần thiết
__all__ = [
    "Genlogin", 
    "GenloginError", 
    "GenloginProfileError", 
    "GenloginConnectionError"
]