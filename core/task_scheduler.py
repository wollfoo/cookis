"""
task_scheduler.py - Lên lịch và quản lý công việc thích ứng

Module này cung cấp lớp AdaptiveTaskScheduler để quản lý công việc thông minh:
- Tự động điều chỉnh số worker dựa trên tài nguyên hệ thống
- Quản lý hàng đợi ưu tiên cho các tác vụ
- Theo dõi và phân tích hiệu suất xử lý tác vụ
- Tối ưu hóa phân phối tài nguyên theo thời gian thực
- Dự đoán số worker tối ưu bằng mô hình học máy đơn giản

Ví dụ sử dụng:
    scheduler = AdaptiveTaskScheduler(max_workers=4)
    scheduler.add_task('task1', lambda: process_file('file1.txt'), priority=10)
    optimal_workers = scheduler.get_optimal_workers()
    report = scheduler.get_performance_report()
"""

import os
import time
import logging
import threading
import statistics
import psutil
import random
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Callable, Union, Set
from collections import defaultdict

# Import từ global_state
from global_state import is_shutdown_requested

class AdaptiveTaskScheduler:
    """
    Quản lý công việc với tự động điều chỉnh số lượng worker:
    - Giám sát tài nguyên hệ thống (CPU/RAM)
    - Tự động điều chỉnh số worker phù hợp
    - Theo dõi hiệu suất để tối ưu
    - Lập lịch thông minh dựa trên mức độ ưu tiên và tài nguyên
    
    Thuộc tính:
        max_workers (int): Số lượng worker tối đa
        current_workers (int): Số lượng worker hiện tại đang được sử dụng
        max_cpu_percent (int): Ngưỡng CPU tối đa (%) trước khi giảm worker
        max_memory_percent (int): Ngưỡng RAM tối đa (%) trước khi giảm worker
        task_queue (list): Hàng đợi các tác vụ đang chờ xử lý
    """
    def __init__(
        self,
        max_workers: int = None,
        max_cpu_percent: int = 70,
        max_memory_percent: int = 80,
        check_interval: int = 15,
        logger: Optional[logging.Logger] = None
    ):
        """
        Khởi tạo AdaptiveTaskScheduler
        
        Args:
            max_workers: Số lượng worker tối đa, mặc định là min(cpu_count, 4)
            max_cpu_percent: Ngưỡng CPU tối đa (%) trước khi giảm worker
            max_memory_percent: Ngưỡng RAM tối đa (%) trước khi giảm worker
            check_interval: Khoảng thời gian (giây) giữa các lần kiểm tra tài nguyên
            logger: Logger tùy chọn
        """
        self.max_workers = max_workers or min(os.cpu_count() or 2, 4)
        self.max_cpu_percent = max_cpu_percent
        self.max_memory_percent = max_memory_percent
        self.check_interval = check_interval
        self.logger = logger or logging.getLogger(__name__)
        self.current_workers = self.max_workers
        self.last_check_time = time.time()
        self.resource_lock = threading.RLock()
        self.task_times = {}  # {task_id: {'start': time, 'end': time, 'success': bool}}
        self.task_success = {}  # {task_id: bool}
        self.system_metrics = []  # [{timestamp, cpu, memory, workers}, ...]
        self.shutdown_flag = False
        self.monitor_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        self.monitor_thread.start()
        self.task_queue = []  # [(priority, task_id, callback), ...]
        self.task_queue_lock = threading.RLock()
        self.performance_history = []  # [{timestamp, avg_time, success_rate, cpu, memory}, ...]
        
        # Machine learning state (simple)
        self.learning_data = []
        self.predictor_trained = False
        self.optimal_workers_map = {}
        
        # Thống kê hiệu suất chi tiết hơn
        self.performance_stats = {
            "started_tasks": 0,
            "completed_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "total_execution_time": 0,
            "worker_adjustments": 0,
            "peak_queue_length": 0
        }
        
        self.logger.info(
            f"Khởi tạo AdaptiveTaskScheduler với {self.max_workers} worker tối đa, "
            f"max_cpu={max_cpu_percent}%, max_memory={max_memory_percent}%"
        )

    def _monitor_resources(self):
        """Thread giám sát tài nguyên hệ thống và tự động điều chỉnh workers"""
        self.logger.debug("Bắt đầu monitor thread để giám sát tài nguyên")
        while not self.shutdown_flag:
            try:
                # Kiểm tra cờ shutdown
                if is_shutdown_requested():
                    self.logger.info("Phát hiện yêu cầu shutdown, dừng monitor_resources")
                    self.shutdown_flag = True
                    break
                
                current_time = time.time()
                if current_time - self.last_check_time >= self.check_interval:
                    self._adjust_worker_count()
                    self._update_performance_history()
                    self.last_check_time = current_time
                    
                    # Sau mỗi 10 lần kiểm tra, cập nhật model dự đoán
                    if len(self.performance_history) % 10 == 0 and len(self.performance_history) >= 20:
                        self._update_prediction_model()
            except Exception as e:
                self.logger.error(f"Lỗi giám sát tài nguyên: {type(e).__name__}: {str(e)}")
                # Ghi log traceback chi tiết nếu ở debug mode
                if self.logger.level <= logging.DEBUG:
                    import traceback
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
            
            # Sleep với kiểm tra shutdown định kỳ
            for _ in range(5):  # Sleep 5 giây, kiểm tra mỗi 1 giây
                if self.shutdown_flag or is_shutdown_requested():
                    break
                time.sleep(1)
                
        self.logger.debug("Monitor thread đã kết thúc")

    def _adjust_worker_count(self):
        """
        Tự động điều chỉnh số worker dựa trên tài nguyên và hiệu suất
        
        Chiến lược:
        1. Sử dụng model ML nếu đã được training (ưu tiên)
        2. Giảm worker nếu tài nguyên vượt ngưỡng
        3. Tăng worker nếu tài nguyên có dư thừa
        """
        try:
            # Lấy thông tin tài nguyên hệ thống hiện tại
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            
            # Lưu metrics
            self.system_metrics.append({
                'timestamp': time.time(),
                'cpu': cpu_percent,
                'memory': memory_percent,
                'workers': self.current_workers
            })
            if len(self.system_metrics) > 20:
                self.system_metrics = self.system_metrics[-20:]
                
            # Sử dụng model dự đoán nếu đã được training
            if self.predictor_trained and len(self.learning_data) >= 10:
                predicted_workers = self._predict_optimal_workers(cpu_percent, memory_percent)
                if predicted_workers is not None:
                    with self.resource_lock:
                        old_workers = self.current_workers
                        self.current_workers = max(1, min(self.max_workers, predicted_workers))
                        if old_workers != self.current_workers:
                            self.logger.info(
                                f"[ML] Điều chỉnh số worker: {old_workers} -> {self.current_workers} "
                                f"(CPU: {cpu_percent:.1f}%, RAM: {memory_percent:.1f}%)"
                            )
                            self.performance_stats["worker_adjustments"] += 1
                    return
                    
            # Nếu không có model dự đoán, sử dụng logic cơ bản
            with self.resource_lock:
                old_workers = self.current_workers
                
                # Chiến lược điều chỉnh worker thông minh hơn
                if cpu_percent > self.max_cpu_percent or memory_percent > self.max_memory_percent:
                    # Giảm workers nếu tài nguyên quá tải - giảm nhanh hơn nếu vượt xa ngưỡng
                    reduction_factor = 0.75  # Giảm 25% theo mặc định
                    
                    # Nếu vượt xa ngưỡng, giảm nhanh hơn
                    if cpu_percent > self.max_cpu_percent * 1.2 or memory_percent > self.max_memory_percent * 1.2:
                        reduction_factor = 0.6  # Giảm 40% 
                    
                    self.current_workers = max(1, int(self.current_workers * reduction_factor))
                
                elif cpu_percent < self.max_cpu_percent * 0.7 and memory_percent < self.max_memory_percent * 0.7:
                    # Tăng workers nếu tài nguyên dư thừa
                    # Tăng thêm tối đa 1 worker mỗi lần
                    self.current_workers = min(self.max_workers, self.current_workers + 1)
                
                if old_workers != self.current_workers:
                    self.logger.info(
                        f"Đã điều chỉnh số worker: {old_workers} -> {self.current_workers} "
                        f"(CPU: {cpu_percent:.1f}%, RAM: {memory_percent:.1f}%)"
                    )
                    self.performance_stats["worker_adjustments"] += 1
                    
        except Exception as e:
            self.logger.error(f"Lỗi điều chỉnh worker: {type(e).__name__}: {str(e)}")
            # Log chi tiết hơn ở debug mode
            if self.logger.level <= logging.DEBUG:
                import traceback
                self.logger.debug(f"Traceback: {traceback.format_exc()}")

    def _update_performance_history(self):
        """
        Cập nhật lịch sử hiệu suất để đào tạo mô hình ML
        và theo dõi tình hình hiệu suất hệ thống
        """
        try:
            # Chỉ tính trên các task đã hoàn thành
            completed_tasks = [
                task for task_id, task in self.task_times.items() 
                if 'end' in task and 'start' in task
            ]
            
            if completed_tasks:
                # Tính thời gian trung bình
                avg_time = sum(task['end'] - task['start'] for task in completed_tasks) / len(completed_tasks)
                
                # Tính tỷ lệ thành công
                success_tasks = sum(1 for task_id, success in self.task_success.items() if success)
                success_rate = success_tasks / len(self.task_success) if self.task_success else 0
                
                # Lấy metrics hệ thống hiện tại
                current_metrics = self.system_metrics[-1] if self.system_metrics else {'cpu': 0, 'memory': 0}
                
                # Thêm vào lịch sử
                self.performance_history.append({
                    'timestamp': time.time(),
                    'avg_time': avg_time,
                    'success_rate': success_rate,
                    'workers': self.current_workers,
                    'cpu': current_metrics.get('cpu', 0),
                    'memory': current_metrics.get('memory', 0),
                    'queue_length': len(self.task_queue)
                })
                
                # Giới hạn lịch sử
                if len(self.performance_history) > 100:
                    self.performance_history = self.performance_history[-100:]
                    
                # Lưu dữ liệu cho machine learning
                self.learning_data.append({
                    'workers': self.current_workers,
                    'cpu': current_metrics.get('cpu', 0),
                    'memory': current_metrics.get('memory', 0),
                    'avg_time': avg_time,
                    'success_rate': success_rate,
                    'queue_length': len(self.task_queue) 
                })
                
                # Cập nhật peak_queue_length
                self.performance_stats["peak_queue_length"] = max(
                    self.performance_stats["peak_queue_length"], 
                    len(self.task_queue)
                )
                
                # Reset task counters sau khi đã lưu dữ liệu nếu quá lớn
                if len(self.task_times) > 1000:
                    self.logger.debug(f"Xóa lịch sử task times (size={len(self.task_times)})")
                    self.task_times.clear()
                    self.task_success.clear()
                    
        except Exception as e:
            self.logger.error(f"Lỗi cập nhật lịch sử hiệu suất: {str(e)}")
            if self.logger.level <= logging.DEBUG:
                import traceback
                self.logger.debug(f"Traceback: {traceback.format_exc()}")

    def _update_prediction_model(self):
        """
        Cập nhật model dự đoán số worker tối ưu dựa trên dữ liệu hiệu suất.
        
        Model sử dụng phương pháp bucketing để gom nhóm các cấu hình tài nguyên
        tương tự nhau, sau đó tìm số lượng worker tối ưu cho mỗi nhóm.
        """
        try:
            # Chỉ thực hiện khi có đủ dữ liệu
            if len(self.learning_data) < 20:
                self.logger.debug("Chưa đủ dữ liệu để cập nhật model dự đoán")
                return
                    
            # Đơn giản hóa: Tìm số worker tối ưu dựa trên hiệu suất cao nhất cho từng level tài nguyên
            # Chia tài nguyên thành các bucket
            resource_buckets = {}
            
            for entry in self.learning_data:
                # Kiểm tra dữ liệu hợp lệ
                if not all(k in entry for k in ['cpu', 'memory', 'avg_time', 'workers', 'success_rate']):
                    continue
                    
                # Lấy bucket dựa trên CPU và RAM (chia thành lưới 10x10)
                try:
                    cpu_bucket = min(9, int(entry['cpu'] / 10))
                    mem_bucket = min(9, int(entry['memory'] / 10))
                    bucket_key = f"{cpu_bucket}_{mem_bucket}"
                except (TypeError, ValueError):
                    # Bỏ qua entry có dữ liệu không hợp lệ
                    continue
                    
                if bucket_key not in resource_buckets:
                    resource_buckets[bucket_key] = []
                        
                # Tính điểm hiệu suất (trọng số: thời gian 70%, success rate 30%)
                try:
                    # Thời gian thấp -> điểm cao
                    if entry['avg_time'] > 0:
                        time_score = 1.0 / entry['avg_time']
                    else:
                        time_score = 10  # Giá trị cao nếu thời gian gần 0
                        
                    success_score = entry['success_rate']
                    perf_score = 0.7 * time_score + 0.3 * success_score
                    
                    resource_buckets[bucket_key].append({
                        'workers': entry['workers'],
                        'score': perf_score
                    })
                except (TypeError, ValueError, ZeroDivisionError):
                    # Bỏ qua các lỗi tính toán
                    continue
            
            if not resource_buckets:
                self.logger.warning("Không có dữ liệu hợp lệ để cập nhật model dự đoán")
                return
                
            # Tìm số worker tối ưu cho mỗi bucket
            self.optimal_workers_map = {}
            for bucket_key, entries in resource_buckets.items():
                if not entries:
                    continue
                        
                # Group by worker count và tính điểm trung bình
                worker_scores = defaultdict(list)
                for entry in entries:
                    if not isinstance(entry['workers'], (int, float)) or entry['workers'] <= 0:
                        continue
                    worker_scores[entry['workers']].append(entry['score'])
                    
                # Kiểm tra worker_scores có dữ liệu không
                if not worker_scores:
                    self.logger.debug(f"Không có dữ liệu worker hợp lệ cho bucket {bucket_key}")
                    continue
                    
                # Tính điểm trung bình cho mỗi số lượng worker
                avg_scores = {w: sum(scores)/len(scores) for w, scores in worker_scores.items()}
                
                # Kiểm tra avg_scores có dữ liệu không trước khi gọi max()
                if not avg_scores:
                    self.logger.debug(f"Không có điểm trung bình cho bucket {bucket_key}")
                    continue
                    
                # Chọn số worker có điểm cao nhất
                try:
                    best_worker_count = max(avg_scores.items(), key=lambda x: x[1])[0]
                    self.optimal_workers_map[bucket_key] = best_worker_count
                except ValueError as e:
                    self.logger.warning(f"Không thể tìm worker tối ưu cho bucket {bucket_key}: {str(e)}")
            
            # Kiểm tra xem có update được không
            if not self.optimal_workers_map:
                self.logger.warning("Không thể cập nhật model dự đoán - không có bucket nào có dữ liệu hợp lệ")
                return
                
            self.predictor_trained = True
            self.logger.info(f"Đã cập nhật model dự đoán với {len(self.optimal_workers_map)} bucket tài nguyên")
            
        except Exception as e:
            self.logger.error(f"Lỗi cập nhật model dự đoán: {str(e)}")
            # Không đặt predictor_trained = False ở đây để tránh mất dữ liệu từ lần update trước
        
    def _predict_optimal_workers(self, cpu_percent, memory_percent) -> Optional[int]:
        """
        Dự đoán số worker tối ưu dựa trên tài nguyên hiện tại
        
        Args:
            cpu_percent: Phần trăm CPU đang sử dụng
            memory_percent: Phần trăm RAM đang sử dụng
            
        Returns:
            int hoặc None: Số worker tối ưu dự đoán, hoặc None nếu không thể dự đoán
        """
        try:
            if not self.predictor_trained or not self.optimal_workers_map:
                return None
                
            # Lấy bucket tương ứng với mức tài nguyên hiện tại
            cpu_bucket = min(9, int(cpu_percent / 10))
            mem_bucket = min(9, int(memory_percent / 10))
            bucket_key = f"{cpu_bucket}_{mem_bucket}"
            
            # Nếu không có dữ liệu cho bucket này, thử tìm bucket gần nhất
            if bucket_key not in self.optimal_workers_map:
                # Tìm bucket gần nhất (khoảng cách Manhattan nhỏ nhất)
                nearest_bucket = None
                min_distance = float('inf')
                
                for existing_bucket in self.optimal_workers_map.keys():
                    # Parse bucket
                    try:
                        ex_cpu, ex_mem = map(int, existing_bucket.split('_'))
                        distance = abs(ex_cpu - cpu_bucket) + abs(ex_mem - mem_bucket)
                        
                        if distance < min_distance:
                            min_distance = distance
                            nearest_bucket = existing_bucket
                    except Exception:
                        continue
                
                if nearest_bucket and min_distance <= 3:  # Chỉ sử dụng nếu gần
                    bucket_key = nearest_bucket
                else:
                    return None
            
            # Lấy giá trị dự đoán
            return self.optimal_workers_map.get(bucket_key)
        except Exception as e:
            self.logger.error(f"Lỗi dự đoán workers: {str(e)}")
            return None

    def get_optimal_workers(self) -> int:
        """
        Lấy số lượng worker tối ưu hiện tại
        
        Returns:
            int: Số lượng worker tối ưu hiện tại
        """
        with self.resource_lock:
            return self.current_workers

    def record_task_start(self, task_id: str) -> None:
        """
        Ghi nhận thời điểm bắt đầu task
        
        Args:
            task_id: ID của task
        """
        with self.resource_lock:
            self.task_times[task_id] = {'start': time.time()}
            self.performance_stats["started_tasks"] += 1

    def record_task_complete(self, task_id: str, success: bool = True) -> None:
        """
        Ghi nhận thời điểm hoàn thành và kết quả task
        
        Args:
            task_id: ID của task
            success: Kết quả thực thi task (True nếu thành công)
        """
        with self.resource_lock:
            if task_id in self.task_times:
                # Ghi nhận thời gian kết thúc
                end_time = time.time()
                self.task_times[task_id]['end'] = end_time
                
                # Tính thời gian thực thi
                start_time = self.task_times[task_id].get('start', end_time)
                execution_time = end_time - start_time
                
                # Cập nhật thống kê
                self.task_success[task_id] = success
                self.performance_stats["completed_tasks"] += 1
                self.performance_stats["total_execution_time"] += execution_time
                
                if success:
                    self.performance_stats["successful_tasks"] += 1
                else:
                    self.performance_stats["failed_tasks"] += 1
                
                # Log ở debug level
                if self.logger.level <= logging.DEBUG:
                    if success:
                        self.logger.debug(f"Task {task_id} hoàn thành thành công sau {execution_time:.2f}s")
                    else:
                        self.logger.debug(f"Task {task_id} thất bại sau {execution_time:.2f}s")

    def add_task(self, task_id: str, callback: Callable, priority: int = 0) -> None:
        """
        Thêm task vào hàng đợi ưu tiên
        
        Args:
            task_id: ID của task
            callback: Hàm callback để thực thi task
            priority: Độ ưu tiên của task (cao hơn được xử lý trước)
        """
        with self.task_queue_lock:
            # Thêm task vào queue với priority
            self.task_queue.append((priority, task_id, callback))
            # Sắp xếp theo priority (cao đến thấp)
            self.task_queue.sort(key=lambda x: x[0], reverse=True)
            
            # Cập nhật peak_queue_length nếu cần
            if len(self.task_queue) > self.performance_stats["peak_queue_length"]:
                self.performance_stats["peak_queue_length"] = len(self.task_queue)
            
            # Log thông tin task mới
            self.logger.debug(f"Đã thêm task {task_id} vào hàng đợi với priority={priority}")

    def get_next_task(self) -> Optional[Tuple[str, Callable]]:
        """
        Lấy task tiếp theo từ hàng đợi
        
        Returns:
            Tuple[str, Callable] hoặc None: (task_id, callback) hoặc None nếu hàng đợi trống
        """
        with self.task_queue_lock:
            if not self.task_queue:
                return None
            _, task_id, callback = self.task_queue.pop(0)
            return task_id, callback

    def get_performance_report(self) -> Dict[str, Any]:
        """
        Lấy báo cáo hiệu suất chi tiết
        
        Returns:
            Dict: Báo cáo hiệu suất chi tiết
        """
        # Lấy thông tin về các task đã hoàn thành
        completed_tasks = [t for t in self.task_times.values() if 'end' in t]
        
        # Trường hợp chưa có task nào hoàn thành
        if not completed_tasks:
            return {
                'tasks_completed': 0,
                'avg_task_time': 0,
                'success_rate': 0,
                'current_workers': self.current_workers,
                'memory_usage': psutil.virtual_memory().percent,
                'cpu_usage': psutil.cpu_percent(interval=0.1),
                'queue_length': len(self.task_queue)
            }
            
        # Tính các chỉ số hiệu suất từ task đã hoàn thành
        task_durations = [(t['end'] - t['start']) for t in completed_tasks]
        success_count = sum(1 for s in self.task_success.values() if s)
        total_success_count = len(self.task_success)
        
        # Tính toán sự ổn định (độ lệch chuẩn chuẩn hóa)
        if len(task_durations) > 1:
            try:
                mean_time = statistics.mean(task_durations)
                stdev_time = statistics.stdev(task_durations)
                # Hệ số biến thiên (CV): stdev/mean
                cv = stdev_time / mean_time if mean_time > 0 else 0
                stability = max(0, 1 - min(1, cv))  # 0 = không ổn định, 1 = rất ổn định
            except Exception:
                stability = 0.5  # Giá trị mặc định nếu có lỗi
        else:
            stability = 1.0
            
        # Tính tốc độ xử lý (tasks/min)
        oldest_task = min((t['start'] for t in completed_tasks), default=time.time())
        newest_task = max((t['end'] for t in completed_tasks), default=time.time())
        time_span = max(0.001, newest_task - oldest_task)
        processing_rate = len(completed_tasks) / (time_span / 60)  # tasks/min
        
        # Tạo báo cáo đầy đủ
        report = {
            # Thông tin cơ bản
            'tasks_completed': len(completed_tasks),
            'avg_task_time': sum(task_durations) / len(task_durations) if task_durations else 0,
            'min_task_time': min(task_durations) if task_durations else 0,
            'max_task_time': max(task_durations) if task_durations else 0,
            'success_rate': success_count / total_success_count if total_success_count else 0,
            'current_workers': self.current_workers,
            
            # Thông tin tài nguyên
            'system_metrics': self.system_metrics[-1] if self.system_metrics else None,
            'memory_usage': psutil.virtual_memory().percent,
            'cpu_usage': psutil.cpu_percent(interval=0.1),
            
            # Chỉ số hiệu suất
            'stability': stability,
            'processing_rate': processing_rate,
            'queue_length': len(self.task_queue),
            'worker_adjustments': self.performance_stats.get("worker_adjustments", 0),
            
            # Thống kê tổng hợp
            'total_stats': {
                'started': self.performance_stats.get("started_tasks", 0),
                'completed': self.performance_stats.get("completed_tasks", 0),
                'successful': self.performance_stats.get("successful_tasks", 0),
                'failed': self.performance_stats.get("failed_tasks", 0),
                'total_execution_time': self.performance_stats.get("total_execution_time", 0),
                'peak_queue_length': self.performance_stats.get("peak_queue_length", 0)
            }
        }
        
        # Thêm thông tin về ML model nếu đã được training
        if self.predictor_trained:
            report['ml_model'] = {
                'trained': True,
                'data_points': len(self.learning_data),
                'bucket_count': len(self.optimal_workers_map)
            }
        
        return report

    def clear_history(self, keep_last: int = 10) -> None:
        """
        Xóa lịch sử cũ để giải phóng bộ nhớ
        
        Args:
            keep_last: Số lượng bản ghi gần nhất cần giữ lại
        """
        with self.resource_lock:
            # Chỉ giữ lại một số bản ghi gần nhất
            if len(self.performance_history) > keep_last:
                self.performance_history = self.performance_history[-keep_last:]
                
            # Chỉ giữ lại một số metrics gần nhất
            if len(self.system_metrics) > keep_last:
                self.system_metrics = self.system_metrics[-keep_last:]
                
            # Lưu task times và task success mới (đang làm) và xóa cũ
            active_task_ids = {k for k, v in self.task_times.items() if 'end' not in v}
            
            # Nếu có quá nhiều task đã hoàn thành, xóa các task cũ
            if len(self.task_times) - len(active_task_ids) > 100:
                # Giữ lại chỉ các task đang thực thi
                new_task_times = {k: v for k, v in self.task_times.items() if k in active_task_ids}
                self.task_times = new_task_times
                
                # Giữ lại chỉ kết quả của các task đang thực thi
                new_task_success = {k: v for k, v in self.task_success.items() if k in active_task_ids}
                self.task_success = new_task_success
                
                self.logger.debug(f"Đã xóa lịch sử task cũ, giữ lại {len(active_task_ids)} task đang thực thi")

    def get_resource_usage(self) -> Dict[str, float]:
        """
        Lấy thông tin sử dụng tài nguyên hệ thống hiện tại
        
        Returns:
            Dict: Thông tin sử dụng tài nguyên
        """
        try:
            cpu_percent = psutil.cpu_percent(interval=0.5)
            memory = psutil.virtual_memory()
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available_gb': memory.available / (1024 ** 3),
                'memory_total_gb': memory.total / (1024 ** 3),
                'current_workers': self.current_workers,
                'queue_length': len(self.task_queue),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Lỗi khi lấy thông tin tài nguyên: {str(e)}")
            return {
                'cpu_percent': 0,
                'memory_percent': 0,
                'error': str(e),
                'current_workers': self.current_workers,
                'timestamp': datetime.now().isoformat()
            }

    def shutdown(self) -> None:
        """
        Dừng scheduler và dọn dẹp tài nguyên
        """
        self.logger.info("Bắt đầu shutdown AdaptiveTaskScheduler")
        self.shutdown_flag = True
        
        # Đợi monitor thread kết thúc
        if hasattr(self, 'monitor_thread') and self.monitor_thread.is_alive():
            try:
                self.monitor_thread.join(timeout=2)
                self.logger.debug("Monitor thread đã kết thúc")
            except Exception as e:
                self.logger.warning(f"Lỗi khi đợi monitor thread: {str(e)}")
        
        # Ghi log summary
        total_tasks = self.performance_stats.get("started_tasks", 0)
        completed_tasks = self.performance_stats.get("completed_tasks", 0)
        success_rate = (self.performance_stats.get("successful_tasks", 0) / completed_tasks * 100) if completed_tasks > 0 else 0
        
        self.logger.info(
            f"AdaptiveTaskScheduler đã shutdown: "
            f"Tổng {total_tasks} task, hoàn thành {completed_tasks}, "
            f"tỷ lệ thành công {success_rate:.1f}%"
        )


# Hàm tiện ích để tính toán số worker tối ưu
def calculate_optimal_workers(
    cpu_count: int = None, 
    memory_gb: float = None, 
    max_cpu_percent: int = 70,
    max_memory_percent: int = 80
) -> int:
    """
    Tính toán số worker tối ưu dựa trên tài nguyên hệ thống
    
    Args:
        cpu_count: Số lõi CPU (nếu None sẽ tự động phát hiện)
        memory_gb: Bộ nhớ RAM (GB) (nếu None sẽ tự động phát hiện)
        max_cpu_percent: Ngưỡng CPU tối đa (%)
        max_memory_percent: Ngưỡng RAM tối đa (%)
        
    Returns:
        int: Số worker tối ưu
    """
    try:
        # Tự động phát hiện tài nguyên nếu không được cung cấp
        if cpu_count is None:
            cpu_count = os.cpu_count() or 2
        
        if memory_gb is None:
            memory = psutil.virtual_memory()
            memory_gb = memory.total / (1024 ** 3)
            memory_percent = memory.percent
        else:
            # Giả định sử dụng 50% nếu không được cung cấp
            memory_percent = max_memory_percent * 0.5
        
        # Lấy mức sử dụng CPU
        cpu_percent = psutil.cpu_percent(interval=0.5)
        
        # Tính số worker dựa trên CPU
        cpu_capacity = cpu_count * (1 - cpu_percent / 100) * (max_cpu_percent / 100)
        recommended_by_cpu = max(1, round(cpu_capacity))
        
        # Tính số worker dựa trên Memory (giả định mỗi worker cần 1GB)
        memory_remaining = memory_gb * (1 - memory_percent / 100)
        recommended_by_memory = max(1, round(memory_remaining / 1.5))  # Giả định mỗi worker cần 1.5GB
        
        # Lấy giá trị nhỏ nhất
        recommended = min(recommended_by_cpu, recommended_by_memory)
        
        # Đảm bảo có ít nhất 1 worker nhưng không quá 2x CPU count
        recommended = max(1, min(recommended, cpu_count * 2))
        
        return recommended
    except Exception:
        # Fallback đến giá trị an toàn
        return min(os.cpu_count() or 2, 4)
