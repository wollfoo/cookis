"""
system_monitor.py - Giám sát tài nguyên hệ thống
- Theo dõi CPU, bộ nhớ, và các tài nguyên hệ thống khác
- Đưa ra khuyến nghị về số lượng worker và tài nguyên tối ưu
- Phát hiện và cảnh báo khi tài nguyên gần cạn kiệt
- Hỗ trợ giám sát liên tục và phân tích xu hướng
"""

import os
import time
import logging
import threading
import psutil
import platform
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Union, Callable
from collections import deque

class SystemMonitor:
    """
    Giám sát tài nguyên hệ thống và đưa ra khuyến nghị về
    việc sử dụng tài nguyên hợp lý
    
    Tính năng:
    - Giám sát CPU, RAM, disk và network
    - Đề xuất số lượng worker tối ưu dựa trên tài nguyên
    - Giám sát liên tục với background thread
    - Phát hiện và cảnh báo khi tài nguyên gần cạn kiệt
    - Phân tích xu hướng sử dụng tài nguyên
    - Hỗ trợ giám sát GPU (nếu có)
    """

    def __init__(
        self, 
        logger=None, 
        history_size: int = 60,
        warning_threshold: Dict[str, float] = None,
        enable_gpu_monitoring: bool = False,
        continuous_monitoring: bool = False,
        monitoring_interval: float = 5.0,
        worker_memory_gb: float = 1.5
    ):
        """
        Khởi tạo SystemMonitor
        
        Args:
            logger: Logger instance để ghi log
            history_size: Số lượng mẫu lịch sử lưu trữ cho phân tích xu hướng
            warning_threshold: Ngưỡng cảnh báo cho các tài nguyên (% sử dụng)
                               Mặc định: {'cpu': 85.0, 'memory': 90.0, 'disk': 95.0}
            enable_gpu_monitoring: Bật giám sát GPU nếu True
            continuous_monitoring: Bật giám sát liên tục nếu True
            monitoring_interval: Khoảng thời gian giữa các lần giám sát (giây)
            worker_memory_gb: Lượng RAM ước tính cần thiết cho mỗi worker (GB)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.history_size = history_size
        self.warning_threshold = warning_threshold or {
            'cpu': 85.0, 
            'memory': 90.0, 
            'disk': 95.0
        }
        self.enable_gpu_monitoring = enable_gpu_monitoring
        self.monitoring_interval = monitoring_interval
        self.worker_memory_gb = worker_memory_gb
        
        # Khởi tạo các biến lưu trữ lịch sử
        self.history = {
            'cpu': deque(maxlen=history_size),
            'memory': deque(maxlen=history_size),
            'disk': deque(maxlen=history_size),
            'network': deque(maxlen=history_size),
            'timestamp': deque(maxlen=history_size)
        }
        
        # Metadata và thông tin hệ thống
        self.system_info = self._get_system_info()
        
        # Trạng thái giám sát liên tục
        self.continuous_monitoring = continuous_monitoring
        self._monitor_thread = None
        self._stop_monitoring = threading.Event()
        
        # Callback cho cảnh báo
        self.warning_callbacks = []
        
        # Khởi động giám sát liên tục nếu được yêu cầu
        if continuous_monitoring:
            self.start_continuous_monitoring()
            
        # Lưu trữ thông tin tài nguyên hiện tại
        self.current_resources = self.get_system_resources()
        
        # Lưu trữ thời điểm cập nhật cuối cùng
        self.last_update_time = time.time()
        
        # Trạng thái GPU (nếu được kích hoạt)
        self.gpu_info = None
        if enable_gpu_monitoring:
            self._init_gpu_monitoring()
            
        self.logger.info(f"SystemMonitor khởi tạo: {self.system_info['os']} - "
                        f"{self.system_info['cpu_model']} ({self.system_info['cpu_count']} cores) - "
                        f"{self.system_info['memory_total_gb']:.1f}GB RAM")

    def _get_system_info(self) -> Dict[str, Any]:
        """
        Thu thập thông tin cơ bản về hệ thống
        
        Returns:
            Dict[str, Any]: Thông tin hệ thống
        """
        try:
            cpu_info = platform.processor() or "Unknown CPU"
            if hasattr(psutil, "cpu_freq") and psutil.cpu_freq():
                cpu_freq = psutil.cpu_freq().current
                cpu_info += f" @ {cpu_freq/1000:.2f}GHz"
                
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                "os": f"{platform.system()} {platform.release()}",
                "python_version": platform.python_version(),
                "cpu_model": cpu_info,
                "cpu_count": os.cpu_count() or 2,
                "cpu_physical_count": psutil.cpu_count(logical=False) or 1,
                "memory_total_gb": memory.total / (1024 ** 3),
                "memory_available_gb": memory.available / (1024 ** 3),
                "disk_total_gb": disk.total / (1024 ** 3),
                "disk_free_gb": disk.free / (1024 ** 3),
                "hostname": platform.node(),
                "boot_time": datetime.fromtimestamp(psutil.boot_time()).isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Không thể lấy thông tin hệ thống đầy đủ: {str(e)}")
            return {
                "os": platform.system(),
                "cpu_count": os.cpu_count() or 2,
                "memory_total_gb": 0,
                "error": str(e)
            }

    def _init_gpu_monitoring(self) -> None:
        """
        Khởi tạo giám sát GPU nếu có thể
        
        Hỗ trợ NVIDIA GPU qua thư viện pynvml hoặc py3nvml
        và AMD GPU qua thư viện pyamdgpuinfo
        """
        try:
            # Thử import thư viện NVIDIA GPU monitoring
            try:
                import pynvml
                pynvml.nvmlInit()
                self.logger.info("Đã kích hoạt giám sát NVIDIA GPU qua pynvml")
                self.gpu_library = "pynvml"
                self.pynvml = pynvml
                self.gpu_count = pynvml.nvmlDeviceGetCount()
                self.gpu_info = {"type": "nvidia", "count": self.gpu_count, "devices": []}
                
                # Thu thập thông tin các GPU
                for i in range(self.gpu_count):
                    handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                    name = pynvml.nvmlDeviceGetName(handle)
                    memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    self.gpu_info["devices"].append({
                        "index": i,
                        "name": name,
                        "memory_total_mb": memory.total / (1024 * 1024)
                    })
                
                return
            except (ImportError, Exception) as e:
                self.logger.debug(f"Không thể kích hoạt pynvml: {str(e)}")
            
            # Thử import thư viện py3nvml nếu pynvml không khả dụng
            try:
                import py3nvml.py3nvml as nvml
                nvml.nvmlInit()
                self.logger.info("Đã kích hoạt giám sát NVIDIA GPU qua py3nvml")
                self.gpu_library = "py3nvml"
                self.pynvml = nvml
                self.gpu_count = nvml.nvmlDeviceGetCount()
                self.gpu_info = {"type": "nvidia", "count": self.gpu_count, "devices": []}
                
                # Thu thập thông tin các GPU
                for i in range(self.gpu_count):
                    handle = nvml.nvmlDeviceGetHandleByIndex(i)
                    name = nvml.nvmlDeviceGetName(handle)
                    memory = nvml.nvmlDeviceGetMemoryInfo(handle)
                    self.gpu_info["devices"].append({
                        "index": i,
                        "name": name,
                        "memory_total_mb": memory.total / (1024 * 1024)
                    })
                
                return
            except (ImportError, Exception) as e:
                self.logger.debug(f"Không thể kích hoạt py3nvml: {str(e)}")
            
            # Thử import thư viện AMD GPU monitoring
            try:
                import pyamdgpuinfo
                self.logger.info("Đã kích hoạt giám sát AMD GPU qua pyamdgpuinfo")
                self.gpu_library = "pyamdgpuinfo"
                self.pyamdgpuinfo = pyamdgpuinfo
                self.gpu_count = pyamdgpuinfo.detect_gpus()
                self.gpu_info = {"type": "amd", "count": self.gpu_count, "devices": []}
                
                # Thu thập thông tin các GPU
                for i in range(self.gpu_count):
                    gpu = pyamdgpuinfo.get_gpu(i)
                    self.gpu_info["devices"].append({
                        "index": i,
                        "name": gpu.name,
                        "memory_total_mb": gpu.memory_info()['vram_size'] / (1024 * 1024)
                    })
                
                return
            except (ImportError, Exception) as e:
                self.logger.debug(f"Không thể kích hoạt pyamdgpuinfo: {str(e)}")
            
            # Nếu không tìm thấy GPU hoặc thư viện hỗ trợ
            self.logger.warning("Không phát hiện GPU hoặc thư viện hỗ trợ. Tắt giám sát GPU.")
            self.enable_gpu_monitoring = False
            self.gpu_info = {"error": "No GPU detected or libraries not available"}
        
        except Exception as e:
            self.logger.warning(f"Không thể khởi tạo giám sát GPU: {str(e)}")
            self.enable_gpu_monitoring = False
            self.gpu_info = {"error": str(e)}

    def get_system_resources(self) -> Dict[str, Any]:
        """
        Lấy thông tin tài nguyên hệ thống hiện tại
        
        Returns:
            Dict[str, Any]: Thông tin tài nguyên hệ thống
        """
        try:
            # CPU info
            cpu_count = os.cpu_count() or 2
            cpu_percent = psutil.cpu_percent(interval=0.5)
            cpu_per_core = psutil.cpu_percent(interval=0.1, percpu=True)
            
            # Memory info
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_gb = memory.total / (1024 ** 3)
            memory_available_gb = memory.available / (1024 ** 3)
            memory_used_gb = memory.used / (1024 ** 3)
            
            # Disk info
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            disk_total_gb = disk.total / (1024 ** 3)
            disk_free_gb = disk.free / (1024 ** 3)
            
            # Network info
            net_io = psutil.net_io_counters()
            
            # Timestamp
            current_time = datetime.now()
            
            # Basic result
            result = {
                "cpu_count": cpu_count,
                "cpu_percent": cpu_percent,
                "cpu_per_core": cpu_per_core,
                "memory_total_gb": memory_gb,
                "memory_available_gb": memory_available_gb,
                "memory_used_gb": memory_used_gb,
                "memory_percent": memory_percent,
                "disk_percent": disk_percent,
                "disk_total_gb": disk_total_gb,
                "disk_free_gb": disk_free_gb,
                "net_bytes_sent": net_io.bytes_sent,
                "net_bytes_recv": net_io.bytes_recv,
                "timestamp": current_time.isoformat(),
                "epoch": current_time.timestamp()
            }
            
            # Add process info
            try:
                process = psutil.Process()
                process_info = {
                    "process_cpu_percent": process.cpu_percent(interval=0.1),
                    "process_memory_percent": process.memory_percent(),
                    "process_memory_mb": process.memory_info().rss / (1024 * 1024),
                    "process_threads": process.num_threads(),
                    "process_create_time": datetime.fromtimestamp(process.create_time()).isoformat()
                }
                result.update(process_info)
            except Exception as e:
                self.logger.debug(f"Không thể lấy thông tin process: {str(e)}")
            
            # Add GPU info if enabled
            if self.enable_gpu_monitoring and self.gpu_info and "type" in self.gpu_info:
                gpu_data = self._get_gpu_metrics()
                if gpu_data:
                    result.update({"gpu": gpu_data})
            
            # Update history
            self._update_history(result)
            
            # Check for resource warnings
            self._check_resource_warnings(result)
            
            return result
        
        except Exception as e:
            self.logger.warning(f"Không thể lấy thông tin tài nguyên: {str(e)}")
            return {
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    def _get_gpu_metrics(self) -> Dict[str, Any]:
        """
        Lấy thông tin về tình trạng GPU
        
        Returns:
            Dict[str, Any]: Thông tin GPU hoặc None nếu không khả dụng
        """
        if not self.enable_gpu_monitoring or not self.gpu_info or "error" in self.gpu_info:
            return None
            
        try:
            result = {"devices": []}
            
            if self.gpu_info["type"] == "nvidia":
                if hasattr(self, "pynvml"):
                    for i in range(self.gpu_count):
                        handle = self.pynvml.nvmlDeviceGetHandleByIndex(i)
                        
                        # Get memory info
                        memory = self.pynvml.nvmlDeviceGetMemoryInfo(handle)
                        
                        # Get utilization
                        utilization = self.pynvml.nvmlDeviceGetUtilizationRates(handle)
                        
                        # Get temperature
                        try:
                            temp = self.pynvml.nvmlDeviceGetTemperature(
                                handle, self.pynvml.NVML_TEMPERATURE_GPU
                            )
                        except Exception:
                            temp = 0
                            
                        # Get power usage if available
                        try:
                            power = self.pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0  # Convert to watts
                        except Exception:
                            power = 0
                            
                        # Add to result
                        result["devices"].append({
                            "index": i,
                            "memory_used_mb": memory.used / (1024 * 1024),
                            "memory_total_mb": memory.total / (1024 * 1024),
                            "memory_percent": memory.used / memory.total * 100,
                            "gpu_utilization": utilization.gpu,
                            "memory_utilization": utilization.memory,
                            "temperature": temp,
                            "power_usage_watts": power
                        })
                        
            elif self.gpu_info["type"] == "amd" and hasattr(self, "pyamdgpuinfo"):
                for i in range(self.gpu_count):
                    gpu = self.pyamdgpuinfo.get_gpu(i)
                    memory_info = gpu.memory_info()
                    temperature = gpu.query_temperature()
                    load = gpu.query_load()
                    
                    result["devices"].append({
                        "index": i,
                        "memory_used_mb": memory_info['vram_used'] / (1024 * 1024),
                        "memory_total_mb": memory_info['vram_size'] / (1024 * 1024),
                        "memory_percent": memory_info['vram_used'] / memory_info['vram_size'] * 100,
                        "gpu_utilization": load,
                        "temperature": temperature
                    })
                    
            return result
                
        except Exception as e:
            self.logger.warning(f"Không thể lấy thông tin GPU: {str(e)}")
            return {"error": str(e)}

    def _update_history(self, resources: Dict[str, Any]) -> None:
        """
        Cập nhật lịch sử tài nguyên
        
        Args:
            resources: Thông tin tài nguyên hiện tại
        """
        try:
            self.history['cpu'].append(resources['cpu_percent'])
            self.history['memory'].append(resources['memory_percent'])
            self.history['disk'].append(resources['disk_percent'])
            
            # Tính toán network speed
            current_time = time.time()
            if hasattr(self, 'last_net_bytes_sent'):
                time_diff = current_time - self.last_update_time
                if time_diff > 0:
                    net_sent_speed = (resources['net_bytes_sent'] - self.last_net_bytes_sent) / time_diff / 1024  # KB/s
                    net_recv_speed = (resources['net_bytes_recv'] - self.last_net_bytes_recv) / time_diff / 1024  # KB/s
                    self.history['network'].append((net_sent_speed, net_recv_speed))
            
            # Lưu giá trị network hiện tại cho lần sau
            self.last_net_bytes_sent = resources['net_bytes_sent']
            self.last_net_bytes_recv = resources['net_bytes_recv']
            self.last_update_time = current_time
            
            # Lưu timestamp
            self.history['timestamp'].append(resources['epoch'])
            
        except Exception as e:
            self.logger.debug(f"Không thể cập nhật lịch sử tài nguyên: {str(e)}")

    def _check_resource_warnings(self, resources: Dict[str, Any]) -> None:
        """
        Kiểm tra và phát cảnh báo khi tài nguyên vượt ngưỡng
        
        Args:
            resources: Thông tin tài nguyên hiện tại
        """
        warnings = []
        
        # Kiểm tra CPU
        if resources.get('cpu_percent', 0) > self.warning_threshold.get('cpu', 85):
            msg = f"CPU usage is high: {resources['cpu_percent']:.1f}% (threshold: {self.warning_threshold['cpu']}%)"
            warnings.append(("cpu", msg, resources['cpu_percent']))
            
        # Kiểm tra memory
        if resources.get('memory_percent', 0) > self.warning_threshold.get('memory', 90):
            msg = f"Memory usage is high: {resources['memory_percent']:.1f}% (threshold: {self.warning_threshold['memory']}%)"
            warnings.append(("memory", msg, resources['memory_percent']))
            
        # Kiểm tra disk
        if resources.get('disk_percent', 0) > self.warning_threshold.get('disk', 95):
            msg = f"Disk usage is high: {resources['disk_percent']:.1f}% (threshold: {self.warning_threshold['disk']}%)"
            warnings.append(("disk", msg, resources['disk_percent']))
            
        # Kiểm tra GPU nếu có
        if 'gpu' in resources and 'devices' in resources['gpu']:
            for i, device in enumerate(resources['gpu']['devices']):
                if device.get('memory_percent', 0) > self.warning_threshold.get('gpu_memory', 90):
                    msg = f"GPU {i} memory usage is high: {device['memory_percent']:.1f}% (threshold: {self.warning_threshold.get('gpu_memory', 90)}%)"
                    warnings.append(("gpu_memory", msg, device['memory_percent']))
                    
                if device.get('gpu_utilization', 0) > self.warning_threshold.get('gpu_util', 95):
                    msg = f"GPU {i} utilization is high: {device['gpu_utilization']:.1f}% (threshold: {self.warning_threshold.get('gpu_util', 95)}%)"
                    warnings.append(("gpu_util", msg, device['gpu_utilization']))
                
                if device.get('temperature', 0) > self.warning_threshold.get('gpu_temp', 85):
                    msg = f"GPU {i} temperature is high: {device['temperature']}°C (threshold: {self.warning_threshold.get('gpu_temp', 85)}°C)"
                    warnings.append(("gpu_temp", msg, device['temperature']))
        
        # Log và gọi callback cho mỗi cảnh báo
        for warning_type, message, value in warnings:
            self.logger.warning(message)
            
            # Gọi các callback đã đăng ký
            for callback in self.warning_callbacks:
                try:
                    callback(warning_type, message, value)
                except Exception as e:
                    self.logger.error(f"Lỗi khi gọi warning callback: {str(e)}")

    def register_warning_callback(self, callback: Callable[[str, str, float], None]) -> None:
        """
        Đăng ký callback để nhận cảnh báo khi tài nguyên vượt ngưỡng
        
        Args:
            callback: Hàm callback với tham số (warning_type, message, value)
        """
        self.warning_callbacks.append(callback)

    def start_continuous_monitoring(self) -> None:
        """
        Bắt đầu giám sát liên tục trong một thread riêng biệt
        """
        if self._monitor_thread and self._monitor_thread.is_alive():
            self.logger.warning("Giám sát liên tục đã được khởi động trước đó")
            return
            
        self._stop_monitoring.clear()
        self._monitor_thread = threading.Thread(
            target=self._continuous_monitoring_thread,
            daemon=True
        )
        self._monitor_thread.start()
        self.continuous_monitoring = True
        self.logger.info(f"Đã bắt đầu giám sát liên tục (interval={self.monitoring_interval}s)")

    def stop_continuous_monitoring(self) -> None:
        """
        Dừng giám sát liên tục
        """
        if not self._monitor_thread or not self._monitor_thread.is_alive():
            return
            
        self._stop_monitoring.set()
        self._monitor_thread.join(timeout=2)
        self.continuous_monitoring = False
        self.logger.info("Đã dừng giám sát liên tục")

    def _continuous_monitoring_thread(self) -> None:
        """
        Thread giám sát liên tục
        """
        while not self._stop_monitoring.is_set():
            try:
                # Lấy thông tin tài nguyên và cập nhật
                self.current_resources = self.get_system_resources()
            except Exception as e:
                self.logger.error(f"Lỗi trong quá trình giám sát liên tục: {str(e)}")
                
            # Ngủ một khoảng thời gian với khả năng hủy sớm
            self._stop_monitoring.wait(self.monitoring_interval)

    def recommend_workers(self, max_workers_requested: int) -> int:
        """
        Khuyến nghị số lượng worker phù hợp dựa trên tài nguyên hệ thống
        
        Args:
            max_workers_requested: Số worker tối đa được yêu cầu
            
        Returns:
            int: Số worker được khuyến nghị
        """
        try:
            # Cập nhật thông tin tài nguyên nếu cần
            if time.time() - self.last_update_time > 3:
                self.current_resources = self.get_system_resources()
                
            resources = self.current_resources
            
            # Lấy thông tin cơ bản
            cpu_count = resources.get("cpu_count", os.cpu_count() or 2)
            memory_gb = resources.get("memory_total_gb", 0)
            memory_available_gb = resources.get("memory_available_gb", 0)
            cpu_percent = resources.get("cpu_percent", 0)
            
            # Công thức khuyến nghị dựa trên CPU và RAM
            # Giảm dần theo mức độ sử dụng CPU hiện tại
            
            # 1. Khuyến nghị theo RAM
            # Dành 20% RAM cho hệ thống, phần còn lại cho workers
            system_reserved_gb = max(1, memory_gb * 0.2)
            usable_memory_gb = memory_gb - system_reserved_gb
            recommended_by_memory = max(1, int(usable_memory_gb / self.worker_memory_gb))
            
            # 2. Khuyến nghị theo CPU
            # Tính toán nhật định theo mức độ sử dụng CPU hiện tại
            cpu_load_factor = max(0.1, 1.0 - (cpu_percent / 100.0))
            recommended_by_cpu = max(1, int(cpu_count * cpu_load_factor))
            
            # 3. Xem xét xu hướng sử dụng tài nguyên gần đây
            if len(self.history['cpu']) > 5:
                recent_cpu_trend = self._analyze_resource_trend('cpu', 5)
                if recent_cpu_trend > 0.1:  # Xu hướng tăng mạnh
                    self.logger.debug("Xu hướng CPU tăng mạnh, giảm khuyến nghị worker")
                    recommended_by_cpu = max(1, int(recommended_by_cpu * 0.8))
                    
            if len(self.history['memory']) > 5:
                recent_memory_trend = self._analyze_resource_trend('memory', 5)
                if recent_memory_trend > 0.1:  # Xu hướng tăng mạnh
                    self.logger.debug("Xu hướng Memory tăng mạnh, giảm khuyến nghị worker")
                    recommended_by_memory = max(1, int(recommended_by_memory * 0.8))
            
            # 4. Tính toán khuyến nghị cuối cùng (chọn giá trị nhỏ nhất)
            recommended = min(recommended_by_memory, recommended_by_cpu)
            
            # 5. Kiểm tra và điều chỉnh giới hạn
            if max_workers_requested > 0:
                if max_workers_requested > recommended:
                    self.logger.info(
                        f"Khuyến nghị giảm workers từ {max_workers_requested} xuống {recommended} "
                        f"(CPU: {cpu_percent:.1f}%, RAM: {usable_memory_gb:.1f}GB available)"
                    )
                    return recommended
                else:
                    # Người dùng đã chọn số worker ít hơn khuyến nghị
                    return max_workers_requested
                    
            return recommended
            
        except Exception as e:
            self.logger.warning(f"Lỗi khi tính toán worker khuyến nghị: {str(e)}")
            # Fallback an toàn
            return min(max_workers_requested, os.cpu_count() or 2)

    def _analyze_resource_trend(self, resource_type: str, window_size: int = 5) -> float:
        """
        Phân tích xu hướng sử dụng tài nguyên
        
        Args:
            resource_type: Loại tài nguyên ('cpu', 'memory', 'disk', 'network')
            window_size: Kích thước cửa sổ phân tích
            
        Returns:
            float: Hệ số xu hướng (>0: tăng, <0: giảm, ~0: ổn định)
        """
        if resource_type not in self.history or len(self.history[resource_type]) < window_size:
            return 0.0
            
        # Lấy dữ liệu gần đây
        recent_data = list(self.history[resource_type])[-window_size:]
        
        if not recent_data:
            return 0.0
            
        # Tính toán xu hướng đơn giản (chênh lệch giữa giá trị cuối và đầu)
        if resource_type == 'network':
            # Xử lý đặc biệt cho network (tuple of sent/recv)
            start_value = sum(recent_data[0]) / 2
            end_value = sum(recent_data[-1]) / 2
        else:
            start_value = recent_data[0]
            end_value = recent_data[-1]
            
        # Tính hệ số xu hướng (được chuẩn hóa)
        trend = (end_value - start_value) / max(1, start_value)
        
        return trend

    def get_resource_trends(self, window_size: int = 10) -> Dict[str, float]:
        """
        Lấy xu hướng sử dụng tài nguyên
        
        Args:
            window_size: Kích thước cửa sổ phân tích
            
        Returns:
            Dict[str, float]: Xu hướng sử dụng tài nguyên
        """
        trends = {}
        for resource_type in ['cpu', 'memory', 'disk', 'network']:
            trends[resource_type] = self._analyze_resource_trend(resource_type, window_size)
            
        return trends

    def get_resource_history(self) -> Dict[str, List]:
        """
        Lấy lịch sử sử dụng tài nguyên
        
        Returns:
            Dict[str, List]: Lịch sử sử dụng tài nguyên
        """
        return {
            'cpu': list(self.history['cpu']),
            'memory': list(self.history['memory']),
            'disk': list(self.history['disk']),
            'network': list(self.history['network']),
            'timestamp': list(self.history['timestamp'])
        }

    def print_resource_usage(self) -> None:
        """
        In thông tin sử dụng tài nguyên hệ thống ra console.
        """
        try:
            # Cập nhật thông tin tài nguyên nếu cần
            if time.time() - self.last_update_time > 3:
                self.current_resources = self.get_system_resources()
                
            resources = self.current_resources
            cpu_percent = resources.get("cpu_percent", 0)
            memory_percent = resources.get("memory_percent", 0)
            memory_available_gb = resources.get("memory_available_gb", 0)
            memory_total_gb = resources.get("memory_total_gb", 0)
            
            # Hiển thị thông tin cơ bản
            print(f"CPU: {cpu_percent:.1f}% | RAM: {memory_percent:.1f}% ({memory_total_gb-memory_available_gb:.1f}GB/{memory_total_gb:.1f}GB)")
            
            # Hiển thị thông tin CPU per core nếu có
            if "cpu_per_core" in resources:
                core_usage = " ".join([f"{p:.0f}%" for p in resources["cpu_per_core"]])
                print(f"CPU Cores: [{core_usage}]")
                
            # Hiển thị thông tin disk
            if "disk_percent" in resources:
                print(f"Disk: {resources['disk_percent']:.1f}% ({resources['disk_free_gb']:.1f}GB free)")
                
            # Hiển thị thông tin process nếu có
            if "process_memory_mb" in resources:
                print(f"Process: CPU {resources.get('process_cpu_percent', 0):.1f}% | RAM {resources.get('process_memory_mb', 0):.0f}MB")
                
            # Hiển thị thông tin GPU nếu có
            if "gpu" in resources and "devices" in resources["gpu"]:
                for i, device in enumerate(resources["gpu"]["devices"]):
                    print(f"GPU {i}: Util {device.get('gpu_utilization', 0):.1f}% | Mem {device.get('memory_percent', 0):.1f}% | Temp {device.get('temperature', 0)}°C")
            
        except Exception as e:
            print(f"Lỗi khi hiển thị thông tin tài nguyên: {str(e)}")

    def get_detailed_resource_metrics(self) -> Dict[str, Any]:
        """
        Lấy metrics chi tiết về tài nguyên hệ thống
        
        Returns:
            Dict[str, Any]: Metrics chi tiết
        """
        # Cập nhật thông tin tài nguyên nếu cần
        if time.time() - self.last_update_time > 3:
            self.current_resources = self.get_system_resources()
            
        resources = self.current_resources
        
        # Thêm xu hướng sử dụng
        trends = self.get_resource_trends()
        resources['trends'] = trends
        
        # Thêm khuyến nghị về số worker
        default_workers = min(os.cpu_count() or 2, 4)
        resources['recommended_workers'] = self.recommend_workers(default_workers)
        
        # Thêm thông tin swap
        try:
            swap = psutil.swap_memory()
            resources['swap'] = {
                'total_gb': swap.total / (1024 ** 3),
                'used_gb': swap.used / (1024 ** 3),
                'percent': swap.percent
            }
        except Exception:
            pass
            
        # Thêm thông tin nhiệt độ CPU nếu có thể
        try:
            if hasattr(psutil, "sensors_temperatures"):
                temps = psutil.sensors_temperatures()
                if temps:
                    resources['cpu_temperature'] = {}
                    for name, entries in temps.items():
                        if entries:
                            resources['cpu_temperature'][name] = [
                                {'label': entry.label, 'current': entry.current, 'high': entry.high, 'critical': entry.critical}
                                for entry in entries
                            ]
        except Exception:
            pass
            
        # Thêm thông tin I/O disk
        try:
            disk_io = psutil.disk_io_counters()
            resources['disk_io'] = {
                'read_count': disk_io.read_count,
                'write_count': disk_io.write_count,
                'read_bytes': disk_io.read_bytes,
                'write_bytes': disk_io.write_bytes
            }
        except Exception:
            pass
            
        return resources

    def recommend_worker_configuration(self, workload_type: str = "general") -> Dict[str, Any]:
        """
        Đưa ra khuyến nghị chi tiết về cấu hình worker tối ưu
        
        Args:
            workload_type: Loại workload ('general', 'cpu_intensive', 'memory_intensive', 'io_intensive')
            
        Returns:
            Dict[str, Any]: Khuyến nghị chi tiết
        """
        # Cập nhật thông tin tài nguyên
        resources = self.get_system_resources()
        
        # Lấy thông tin cơ bản
        cpu_count = resources.get("cpu_count", os.cpu_count() or 2)
        memory_gb = resources.get("memory_total_gb", 0)
        memory_available_gb = resources.get("memory_available_gb", 0)
        cpu_percent = resources.get("cpu_percent", 0)
        
        # Điều chỉnh ước tính tài nguyên dựa trên loại workload
        worker_cpu_units = 1.0  # Default: 1 CPU logical per worker
        worker_memory_gb = self.worker_memory_gb  # Default from instance
        
        if workload_type == "cpu_intensive":
            worker_cpu_units = 2.0
            worker_memory_gb = self.worker_memory_gb * 0.8
        elif workload_type == "memory_intensive":
            worker_cpu_units = 0.8
            worker_memory_gb = self.worker_memory_gb * 1.5
        elif workload_type == "io_intensive":
            worker_cpu_units = 0.5
            worker_memory_gb = self.worker_memory_gb * 0.7
            
        # Tính toán số worker tối đa dựa trên CPU và memory
        system_reserved_gb = max(1, memory_gb * 0.2)
        usable_memory_gb = memory_gb - system_reserved_gb
        
        max_workers_by_memory = max(1, int(usable_memory_gb / worker_memory_gb))
        max_workers_by_cpu = max(1, int(cpu_count / worker_cpu_units))
        
        # Tính toán worker tối ưu
        optimal_workers = min(max_workers_by_memory, max_workers_by_cpu)
        
        # Điều chỉnh theo mức sử dụng CPU hiện tại
        if cpu_percent > 70:
            optimal_workers = max(1, int(optimal_workers * 0.8))
            
        # Xem xét xu hướng
        if len(self.history['cpu']) > 5:
            cpu_trend = self._analyze_resource_trend('cpu', 5)
            if cpu_trend > 0.1:  # Xu hướng tăng
                optimal_workers = max(1, int(optimal_workers * 0.9))
                
        # Xem xét workload
        conservative_workers = max(1, int(optimal_workers * 0.8))
        aggressive_workers = min(int(optimal_workers * 1.2), max_workers_by_cpu, max_workers_by_memory)
        
        return {
            "workload_type": workload_type,
            "optimal_workers": optimal_workers,
            "conservative_workers": conservative_workers,
            "aggressive_workers": aggressive_workers,
            "max_workers_by_cpu": max_workers_by_cpu,
            "max_workers_by_memory": max_workers_by_memory,
            "worker_cpu_units": worker_cpu_units,
            "worker_memory_gb": worker_memory_gb,
            "system_resources": {
                "cpu_count": cpu_count,
                "memory_total_gb": memory_gb,
                "memory_available_gb": memory_available_gb,
                "cpu_percent": cpu_percent
            },
            "recommendations": [
                f"Dựa trên tài nguyên hệ thống, khuyến nghị sử dụng {optimal_workers} worker(s) cho workload {workload_type}",
                f"Cấu hình bảo thủ: {conservative_workers} worker(s) để tránh quá tải",
                f"Cấu hình tích cực: {aggressive_workers} worker(s) cho hiệu suất tối đa"
            ]
        }

    def check_system_compatibility(self) -> Dict[str, Any]:
        """
        Kiểm tra tính tương thích của hệ thống và đưa ra cảnh báo về tiềm ẩn vấn đề
        
        Returns:
            Dict[str, Any]: Kết quả kiểm tra và cảnh báo
        """
        compatibility = {
            "status": "ok",
            "warnings": [],
            "recommendations": []
        }
        
        # Lấy thông tin tài nguyên
        resources = self.get_system_resources()
        
        # Kiểm tra CPU
        cpu_count = resources.get("cpu_count", os.cpu_count() or 2)
        if cpu_count < 2:
            compatibility["warnings"].append("Số lượng CPU quá thấp (<2)")
            compatibility["recommendations"].append("Hệ thống cần ít nhất 2 CPU cores để xử lý tác vụ hiệu quả")
            compatibility["status"] = "warning"
            
        # Kiểm tra RAM
        memory_gb = resources.get("memory_total_gb", 0)
        if memory_gb < 4:
            compatibility["warnings"].append(f"Bộ nhớ hệ thống thấp ({memory_gb:.1f}GB)")
            compatibility["recommendations"].append("Hệ thống cần ít nhất 4GB RAM để xử lý tác vụ hiệu quả")
            compatibility["status"] = "warning"
            
        # Kiểm tra disk
        disk_free_gb = resources.get("disk_free_gb", 0)
        if disk_free_gb < 5:
            compatibility["warnings"].append(f"Dung lượng ổ đĩa trống thấp ({disk_free_gb:.1f}GB)")
            compatibility["recommendations"].append("Cần ít nhất 5GB dung lượng trống để lưu trữ tạm thời và kết quả")
            compatibility["status"] = "warning"
            
        # Kiểm tra CPU load
        cpu_percent = resources.get("cpu_percent", 0)
        if cpu_percent > 80:
            compatibility["warnings"].append(f"CPU đang chịu tải cao ({cpu_percent:.1f}%)")
            compatibility["recommendations"].append("Giảm số lượng worker để tránh quá tải CPU")
            compatibility["status"] = "warning"
            
        # Kiểm tra memory load
        memory_percent = resources.get("memory_percent", 0)
        if memory_percent > 85:
            compatibility["warnings"].append(f"Bộ nhớ đang chịu tải cao ({memory_percent:.1f}%)")
            compatibility["recommendations"].append("Giảm số lượng worker để tránh quá tải bộ nhớ")
            compatibility["status"] = "warning"
            
        # Kiểm tra swap
        try:
            swap = psutil.swap_memory()
            swap_gb = swap.total / (1024 ** 3)
            if swap_gb < 1 and memory_gb < 8:
                compatibility["warnings"].append(f"Dung lượng swap thấp ({swap_gb:.1f}GB) với RAM < 8GB")
                compatibility["recommendations"].append("Tăng dung lượng swap để cải thiện hiệu suất khi bộ nhớ thấp")
                compatibility["status"] = "warning"
        except Exception:
            pass
            
        # Kiểm tra nhiệt độ nếu có thể
        try:
            if hasattr(psutil, "sensors_temperatures"):
                temps = psutil.sensors_temperatures()
                if temps:
                    high_temp = False
                    for name, entries in temps.items():
                        for entry in entries:
                            if entry.current > 80:  # Ngưỡng nhiệt độ cao
                                high_temp = True
                                compatibility["warnings"].append(f"Nhiệt độ CPU cao ({entry.current}°C)")
                                
                    if high_temp:
                        compatibility["recommendations"].append("Kiểm tra hệ thống làm mát và giảm số worker để tránh quá nhiệt")
                        compatibility["status"] = "warning"
        except Exception:
            pass
            
        return compatibility

    def shutdown(self) -> None:
        """
        Dọn dẹp tài nguyên và đóng monitor
        """
        self.logger.info("Đang dọn dẹp SystemMonitor...")
        
        # Dừng giám sát liên tục nếu đang chạy
        if self.continuous_monitoring:
            self.stop_continuous_monitoring()
            
        # Đóng GPU monitoring nếu có
        if self.enable_gpu_monitoring and hasattr(self, "pynvml"):
            try:
                self.pynvml.nvmlShutdown()
                self.logger.debug("Đã đóng NVML")
            except Exception as e:
                self.logger.debug(f"Lỗi khi đóng NVML: {str(e)}")
                
        self.logger.info("SystemMonitor đã được đóng")

# Hàm tiện ích để sử dụng SystemMonitor 
def get_recommended_workers(requested_workers: int = 4, logger = None) -> int:
    """
    Hàm tiện ích để lấy nhanh số lượng worker khuyến nghị
    
    Args:
        requested_workers: Số lượng worker được yêu cầu
        logger: Logger instance để ghi log
        
    Returns:
        int: Số lượng worker được khuyến nghị
    """
    monitor = SystemMonitor(logger=logger)
    recommended = monitor.recommend_workers(requested_workers)
    
    if logger:
        logger.info(f"Requested workers: {requested_workers}, Recommended: {recommended}")
        
    return recommended

def print_system_info(logger = None) -> None:
    """
    In thông tin chi tiết về hệ thống
    
    Args:
        logger: Logger instance để ghi log
    """
    monitor = SystemMonitor(logger=logger)
    resources = monitor.get_system_resources()
    
    print("\n=== THÔNG TIN HỆ THỐNG ===")
    print(f"OS: {monitor.system_info['os']}")
    print(f"CPU: {monitor.system_info['cpu_model']} ({monitor.system_info['cpu_count']} cores)")
    print(f"RAM: {monitor.system_info['memory_total_gb']:.1f}GB")
    print(f"Disk: {monitor.system_info['disk_total_gb']:.1f}GB total, {monitor.system_info['disk_free_gb']:.1f}GB free")
    
    print("\n=== TÀI NGUYÊN HIỆN TẠI ===")
    monitor.print_resource_usage()
    
    print("\n=== KHUYẾN NGHỊ ===")
    recommendation = monitor.recommend_worker_configuration()
    for rec in recommendation['recommendations']:
        print(f"- {rec}")
        
    if logger:
        logger.info(f"System info: {monitor.system_info['os']}, "
                   f"CPU: {monitor.system_info['cpu_model']} ({monitor.system_info['cpu_count']} cores), "
                   f"RAM: {monitor.system_info['memory_total_gb']:.1f}GB")
