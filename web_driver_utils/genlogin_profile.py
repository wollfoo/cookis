"""
genlogin_profile.py - quản lý và tương tác với GenLogin.
Version: 2.0.0 (Production)

Key features:
- Centralized configuration management with ProfileManagerConfig
- Robust error handling with standardized exceptions
- Efficient resource management and process cleanup
- Consistent retry mechanism with exponential backoff and jitter
- Memory optimization and thorough logging
- Improved API structure with proper access controls
"""

import os
import time
import logging
import random
import platform
import socket
import subprocess
import psutil
import functools
from pathlib import Path
from typing import Optional, Dict, Union, List, Tuple, Any, Callable, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta

# Import GenLogin and related exceptions
from Genlogin import Genlogin, GenloginError, GenloginProfileError, GenloginConnectionError

# Import from global_state if available
try:
    from global_state import request_shutdown, is_shutdown_requested
except ImportError:
    # Fallback when global_state module is not available
    def is_shutdown_requested():
        return False
    
    def request_shutdown():
        pass

# Import chrome_utils module for process management (new utility module)
try:
    from chrome_utils import cleanup_chrome_processes
except ImportError:
    # Fallback implementation if chrome_utils is not available
    def cleanup_chrome_processes(profile_id=None):
        """
        Fallback implementation for cleaning up Chrome processes.
        This will be used if chrome_utils module is not available.
        """
        return _cleanup_orphaned_chrome_processes(profile_id)

# -------------------------------
# Configuration Classes
# -------------------------------
@dataclass
class ProfileManagerConfig:
    """Centralized configuration for GenloginProfileManager"""
    
    # API connection settings
    api: Dict[str, Any] = field(default_factory=lambda: {
        "key": "",
        "base_url": "http://localhost:55550/backend",
        "timeout": 10
    })
    
    # Retry settings
    retry: Dict[str, Any] = field(default_factory=lambda: {
        "max_attempts": 3,
        "backoff_factor": 2.0,
        "jitter": True,
        "initial_wait": 1.5
    })
    
    # Profile startup settings
    startup: Dict[str, Any] = field(default_factory=lambda: {
        "max_time": 30,
        "check_delay": 2.0,
        "verify_ws_endpoint": True,
        "connection_check_timeout": 3.0
    })
    
    # Profile shutdown settings
    shutdown: Dict[str, Any] = field(default_factory=lambda: {
        "max_wait": 10,
        "force_kill": True,
        "post_stop_delay": 2.0
    })
    
    # Process management settings
    processes: Dict[str, Any] = field(default_factory=lambda: {
        "cleanup_orphaned": True
    })
    
    # Cache settings
    cache: Dict[str, Any] = field(default_factory=lambda: {
        "running_profiles_ttl": 5  # Seconds
    })
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ProfileManagerConfig':
        """Create configuration from dictionary with nested updates."""
        config = cls()
        
        # Update each section if provided
        for section in ["api", "retry", "startup", "shutdown", "processes", "cache"]:
            if section in config_dict and isinstance(config_dict[section], dict):
                # Update existing section with provided values
                getattr(config, section).update(config_dict[section])
        
        return config

# -------------------------------
# Exception Classes
# -------------------------------
class GenloginManagerError(Exception):
    """Base exception for all GenloginProfileManager errors."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.details = details or {}
        super().__init__(message)


class ProfileRunningError(GenloginManagerError):
    """Exception when profile is running on another device."""
    def __init__(self, profile_id: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            f"Profile '{profile_id}' is currently running on another device",
            details or {"profile_id": profile_id}
        )


class GenloginApiError(GenloginManagerError):
    """Exception for GenLogin API errors."""
    def __init__(self, message: str, api_response: Optional[Dict[str, Any]] = None, details: Optional[Dict[str, Any]] = None):
        error_details = details or {}
        if api_response:
            error_details["api_response"] = api_response
        super().__init__(message, error_details)


class ProfileStartError(GenloginManagerError):
    """Exception when unable to start a profile."""
    def __init__(self, profile_id: str, reason: str, details: Optional[Dict[str, Any]] = None):
        error_details = details or {}
        error_details["profile_id"] = profile_id
        super().__init__(f"Failed to start profile '{profile_id}': {reason}", error_details)


class ProfileStopError(GenloginManagerError):
    """Exception when unable to stop a profile."""
    def __init__(self, profile_id: str, reason: str, details: Optional[Dict[str, Any]] = None):
        error_details = details or {}
        error_details["profile_id"] = profile_id
        super().__init__(f"Failed to stop profile '{profile_id}': {reason}", error_details)


class ConfigurationError(GenloginManagerError):
    """Exception for configuration errors."""
    pass

# -------------------------------
# Utility Functions and Decorators
# -------------------------------
# Type variable for generic function return type
T = TypeVar('T')

def retry_with_backoff(
    max_attempts: int = 3, 
    backoff_factor: float = 2.0,
    jitter: bool = True,
    initial_wait: float = 1.5,
    retry_exceptions: Tuple = (Exception,),
    logger: Optional[logging.Logger] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for retrying functions with exponential backoff and optional jitter.
    
    Args:
        max_attempts: Maximum number of attempts
        backoff_factor: Multiplier for wait time between retries
        jitter: Whether to add random jitter to wait times
        initial_wait: Initial wait time in seconds
        retry_exceptions: Exception types to catch and retry
        logger: Logger to use for logging retries
    
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            local_logger = logger or logging.getLogger(__name__)
            
            attempt = 0
            last_exception = None
            
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    attempt += 1
                    last_exception = e
                    
                    # If this was the last attempt, re-raise the exception
                    if attempt >= max_attempts:
                        local_logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts. "
                            f"Last error: {type(e).__name__}: {str(e)}"
                        )
                        raise
                    
                    # Calculate wait time with exponential backoff
                    wait_time = initial_wait * (backoff_factor ** (attempt - 1))
                    
                    # Add jitter if enabled (±20%)
                    if jitter:
                        wait_time *= random.uniform(0.8, 1.2)
                    
                    local_logger.warning(
                        f"Attempt {attempt}/{max_attempts} for {func.__name__} failed: "
                        f"{type(e).__name__}: {str(e)}. Retrying in {wait_time:.2f}s..."
                    )
                    
                    time.sleep(wait_time)
            
            # This should never happen, but just in case
            raise RuntimeError(f"Unexpected exit from retry loop in {func.__name__}")
            
        return wrapper
    return decorator


def is_process_running(process_name: str) -> bool:
    """
    Check if a process is running by name.
    
    Args:
        process_name: Name of the process to check
        
    Returns:
        bool: True if process is running, False otherwise
    """
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if process_name.lower() in proc.info['name'].lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return False


def _cleanup_orphaned_chrome_processes(profile_id: Optional[str] = None) -> int:
    """
    Clean up orphaned Chrome processes related to profile_id.
    This is a fallback implementation when chrome_utils module is not available.
    
    Args:
        profile_id: ID of the GenLogin profile to clean up processes for.
                  If None, all GenLogin-related Chrome processes will be cleaned up.
    
    Returns:
        int: Number of processes terminated
    """
    logger = logging.getLogger(__name__)
    terminated_count = 0
    
    try:
        if platform.system() == "Windows":
            # Windows process handling
            try:
                chrome_processes = []
                
                # Find all chrome.exe processes
                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                        try:
                            cmdline = proc.info['cmdline']
                            if cmdline and any("GenLogin" in arg for arg in cmdline if arg):
                                # If profile_id is provided, only terminate processes related to that profile
                                if profile_id:
                                    if any(profile_id in arg for arg in cmdline if arg):
                                        chrome_processes.append(proc)
                                else:
                                    chrome_processes.append(proc)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                
                # Terminate the found processes
                for proc in chrome_processes:
                    try:
                        proc.terminate()
                        terminated_count += 1
                        logger.debug(f"Terminated Chrome process, PID={proc.pid}")
                    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                        logger.debug(f"Could not terminate Chrome process, PID={proc.pid}: {str(e)}")
                
                # Log the number of terminated processes
                if chrome_processes:
                    logger.info(f"Cleaned up {terminated_count} orphaned Chrome processes")
            except ImportError:
                logger.warning("Could not import psutil module to clean up Chrome processes")
                
                # Alternative method using taskkill
                if profile_id:
                    logger.info(f"Trying taskkill to clean up Chrome for profile {profile_id}")
                    try:
                        result = os.system(f'taskkill /f /im chrome.exe /fi "WINDOWTITLE eq *{profile_id}*"')
                        if result == 0:
                            terminated_count = 1  # Approximate count
                    except Exception as e:
                        logger.warning(f"Error running taskkill: {str(e)}")
        else:
            # Linux/macOS process handling
            try:
                if profile_id:
                    cmd = f"pkill -f 'chrome.*{profile_id}'"
                else:
                    cmd = "pkill -f 'chrome.*GenLogin'"
                
                result = subprocess.run(cmd, shell=True, capture_output=True)
                if result.returncode == 0:
                    terminated_count = 1  # Approximate count
                logger.info(f"Ran Chrome cleanup command: {cmd}")
            except Exception as e:
                logger.warning(f"Error cleaning up Chrome processes: {str(e)}")
    except Exception as e:
        logger.warning(f"Error cleaning up orphaned Chrome processes: {str(e)}")
    
    return terminated_count


def validate_profile_id(profile_id: Any) -> str:
    """
    Validate profile_id parameter.
    
    Args:
        profile_id: Value to validate as profile ID
    
    Returns:
        str: Validated profile ID
    
    Raises:
        ValueError: If profile_id is invalid
    """
    if not isinstance(profile_id, str) or not profile_id.strip():
        raise ValueError("profile_id must be a non-empty string")
    return profile_id.strip()

# -------------------------------
# GenLogin Profile Manager Class
# -------------------------------
class GenloginProfileManager:
    """
    Enhanced class for managing GenLogin profiles with improved performance,
    error handling, and resource management.
    """
    
    def __init__(self, config: Optional[Union[Dict[str, Any], ProfileManagerConfig]] = None):
        """
        Initialize GenloginProfileManager with improved configuration.
        
        Args:
            config: Configuration as a ProfileManagerConfig object or dictionary.
                   If None, default configuration will be used.
        """
        # Set up logger
        self.logger = logging.getLogger(__name__)
        
        # Initialize configuration
        if config is None:
            self.config = ProfileManagerConfig()
        elif isinstance(config, dict):
            self.config = ProfileManagerConfig.from_dict(config)
        elif isinstance(config, ProfileManagerConfig):
            self.config = config
        else:
            raise ConfigurationError(
                f"Invalid configuration type: {type(config)}. Expected dict or ProfileManagerConfig."
            )
        
        # GenLogin API client
        self.gen = None
        
        # Cache for running profiles to reduce API calls
        self._running_profiles_cache = {
            "data": [],
            "timestamp": datetime.min,
            "ttl": timedelta(seconds=self.config.cache["running_profiles_ttl"])
        }
        
        # Initialize GenLogin client if possible
        if self.is_genlogin_running():
            self._init_genlogin_client()
    
    @retry_with_backoff(
        max_attempts=3,
        backoff_factor=2.0,
        jitter=True,
        retry_exceptions=(GenloginConnectionError, GenloginApiError)
    )
    def _init_genlogin_client(self) -> bool:
        """
        Initialize GenLogin API client with retry mechanism.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        if self.gen is not None:
            return True  # Already initialized
        
        self.logger.info("Initializing GenLogin API client...")
        
        try:
            # Create GenLogin instance with adjusted timeout
            self.gen = Genlogin(
                self.config.api["key"],
                base_url=self.config.api["base_url"]
            )
            
            # Set timeout for GenLogin requests
            if hasattr(self.gen, 'request_timeout'):
                self.gen.request_timeout = self.config.api["timeout"]
            
            # Test API by fetching profiles
            self.logger.debug("Testing GenLogin connection by fetching profiles...")
            start_time = time.time()
            test_profiles = self.gen.getProfiles(0, 1)
            elapsed_time = time.time() - start_time
            
            # Validate API response format
            if not isinstance(test_profiles, dict) or "profiles" not in test_profiles:
                error_msg = f"GenLogin API returned invalid format: {test_profiles}"
                self.logger.warning(error_msg)
                raise GenloginApiError(error_msg, api_response=test_profiles)
            
            # Check if profiles data exists
            if not test_profiles.get("profiles") and not test_profiles.get("profiles") == []:
                error_msg = "GenLogin API did not return profiles list"
                self.logger.warning(error_msg)
                raise GenloginApiError(error_msg, api_response=test_profiles)
            
            self.logger.info(f"GenLogin API connection successful (time: {elapsed_time:.2f}s)")
            return True
        
        except Exception as e:
            self.logger.error(f"Failed to connect to GenLogin API: {type(e).__name__}: {str(e)}")
            
            # Release memory for failed GenLogin instance
            self.gen = None
            
            # Re-raise as GenloginApiError for consistent error handling
            if not isinstance(e, GenloginApiError):
                raise GenloginApiError(f"GenLogin API connection error: {str(e)}")
            raise
    
    def is_genlogin_running(self) -> bool:
        """
        Check if GenLogin application is running.
        
        Returns:
            bool: True if GenLogin is running, False otherwise
        """
        # Check process
        process_running = is_process_running("GenLogin")
        
        # Check port
        port_listening = False
        try:
            port = int(self.config.api["base_url"].split(':')[-1].split('/')[0])
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                result = s.connect_ex(('localhost', port))
                port_listening = (result == 0)
        except Exception as e:
            self.logger.debug(f"Error checking GenLogin port: {str(e)}")
        
        is_running = process_running and port_listening
        if not is_running:
            self.logger.warning(
                f"GenLogin not running: process_running={process_running}, "
                f"port_listening={port_listening}"
            )
        
        return is_running
    
    def get_profile_list(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Get list of GenLogin profiles.
        
        Args:
            limit: Maximum number of profiles to return
            offset: Starting position
            
        Returns:
            List[Dict[str, Any]]: List of profiles
            
        Raises:
            GenloginApiError: If unable to get profile list
        """
        if not self._init_genlogin_client():
            raise GenloginApiError("Cannot connect to GenLogin API")
        
        try:
            profiles_data = self.gen.getProfiles(offset, limit)
            profiles = profiles_data.get("profiles", [])
            return profiles
        except Exception as e:
            self.logger.error(f"Error getting profile list: {type(e).__name__}: {str(e)}")
            raise GenloginApiError(f"Cannot get profile list: {str(e)}")
    
    def get_running_profiles(self, use_cache: bool = True) -> List[str]:
        """
        Get list of IDs of running profiles with caching to reduce API calls.
        
        Args:
            use_cache: Whether to use cached results if available
            
        Returns:
            List[str]: List of running profile IDs
            
        Raises:
            GenloginApiError: If unable to get list of running profiles
        """
        # Check if cached data is still valid
        cache_valid = (
            use_cache and
            self._running_profiles_cache["timestamp"] + self._running_profiles_cache["ttl"] > datetime.now()
        )
        
        if cache_valid:
            self.logger.debug(f"Using cached running profiles (age: {(datetime.now() - self._running_profiles_cache['timestamp']).total_seconds():.1f}s)")
            return self._running_profiles_cache["data"]
        
        if not self._init_genlogin_client():
            raise GenloginApiError("Cannot connect to GenLogin API")
        
        try:
            # Set shorter timeout for this request
            original_timeout = None
            if hasattr(self.gen, 'request_timeout'):
                original_timeout = self.gen.request_timeout
                self.gen.request_timeout = 8  # Shorter timeout for this request
            
            try:
                running_profiles = self.gen.getProfilesRunning()
                items = running_profiles.get("data", {}).get("items", [])
                running_ids = [p.get("id") for p in items if isinstance(p, dict) and p.get("id")]
                
                # Update cache
                self._running_profiles_cache["data"] = running_ids
                self._running_profiles_cache["timestamp"] = datetime.now()
                
                return running_ids
            finally:
                # Restore original timeout
                if hasattr(self.gen, 'request_timeout') and original_timeout is not None:
                    self.gen.request_timeout = original_timeout
        except Exception as e:
            self.logger.error(f"Error getting running profiles: {type(e).__name__}: {str(e)}")
            raise GenloginApiError(f"Cannot get running profiles: {str(e)}")
    
    def _check_profile_status(self, profile_id: str) -> Dict[str, Any]:
        """
        Comprehensive check of profile status using multiple methods.
        
        Args:
            profile_id: ID of profile to check
            
        Returns:
            Dict[str, Any]: Dictionary with status information
        """
        if not profile_id:
            raise ValueError("profile_id cannot be empty")
        
        if not self._init_genlogin_client():
            return {"running": False, "error": "Cannot connect to GenLogin API"}
        
        result = {
            "running": False,
            "running_method": None,  # Which method detected the profile is running
            "ws_endpoint": None,
            "error": None
        }
        
        try:
            # Method 1: Check if profile is in running profiles list
            running_ids = self.get_running_profiles(use_cache=True)
            if profile_id in running_ids:
                result["running"] = True
                result["running_method"] = "running_profiles_list"
                
                # Try to get wsEndpoint if profile is running
                try:
                    ws_data = self.gen.getWsEndpoint(profile_id)
                    if ws_data.get("success", False) and ws_data.get("data", {}).get("wsEndpoint"):
                        result["ws_endpoint"] = ws_data.get("data", {}).get("wsEndpoint", "")
                except Exception as ws_e:
                    self.logger.debug(f"Error getting wsEndpoint for running profile: {str(ws_e)}")
                
                return result
            
            # Method 2: Try to get wsEndpoint directly
            self.logger.debug(f"Checking profile {profile_id} status via getWsEndpoint...")
            try:
                # Set shorter timeout for this request
                original_timeout = None
                if hasattr(self.gen, 'request_timeout'):
                    original_timeout = self.gen.request_timeout
                    self.gen.request_timeout = 5  # Shorter timeout
                
                try:
                    ws_data = self.gen.getWsEndpoint(profile_id)
                    if ws_data.get("success", False) and ws_data.get("data", {}).get("wsEndpoint"):
                        result["running"] = True
                        result["running_method"] = "ws_endpoint"
                        result["ws_endpoint"] = ws_data.get("data", {}).get("wsEndpoint", "")
                        self.logger.debug(f"Profile {profile_id} detected as running via getWsEndpoint")
                finally:
                    # Restore original timeout
                    if hasattr(self.gen, 'request_timeout') and original_timeout is not None:
                        self.gen.request_timeout = original_timeout
            except Exception as ws_e:
                self.logger.debug(f"Error checking getWsEndpoint: {str(ws_e)}")
                # Continue with the result from getProfilesRunning
        
        except Exception as e:
            self.logger.warning(f"Error checking profile {profile_id} status: {str(e)}")
            result["error"] = str(e)
        
        return result
    
    def is_profile_running(self, profile_id: str) -> bool:
        """
        Check if a profile is running.
        
        Args:
            profile_id: ID of profile to check
            
        Returns:
            bool: True if profile is running, False otherwise
        """
        if not profile_id:
            return False
        
        if not self._init_genlogin_client():
            return False
        
        try:
            status = self._check_profile_status(profile_id)
            return status["running"]
        except Exception as e:
            self.logger.warning(f"Error checking if profile {profile_id} is running: {str(e)}")
            return False
    
    def select_available_profile(self) -> Optional[str]:
        """
        Select an available profile, preferring one that is not currently running.
        
        Returns:
            Optional[str]: ID of selected profile or None if not found
            
        Raises:
            GenloginApiError: If unable to get profile list
        """
        try:
            profiles = self.get_profile_list(limit=10)
            if not profiles:
                self.logger.error("No GenLogin profiles found!")
                return None
            
            self.logger.debug(f"Found {len(profiles)} profiles")
            
            # Check running profiles
            try:
                running_ids = self.get_running_profiles()
                self.logger.debug(f"Running profiles: {len(running_ids)}")
                
                # Filter profiles that are not running
                available_profiles = [p for p in profiles if p["id"] not in running_ids]
                
                # If there are available profiles, select the first one
                if available_profiles:
                    profile_id = available_profiles[0]["id"]
                    self.logger.info(f"Using available profile ID={profile_id}")
                    return profile_id
                else:
                    profile_id = profiles[0]["id"]
                    self.logger.info(f"All profiles are running, using ID={profile_id}")
                    return profile_id
            except Exception as running_e:
                # Handle error when unable to get list of running profiles
                self.logger.warning(f"Unable to get list of running profiles: {str(running_e)}")
                profile_id = profiles[0]["id"]
                self.logger.info(f"Selected first profile ID={profile_id} (status not checked)")
                return profile_id
        except Exception as e:
            self.logger.error(f"Error selecting profile: {type(e).__name__}: {str(e)}")
            raise GenloginApiError(f"Cannot select profile: {str(e)}")
    
    def stop_profile(
        self, 
        profile_id: str, 
        force_kill: Optional[bool] = None, 
        max_stop_wait: Optional[int] = None
    ) -> bool:
        """
        Stop a GenLogin profile with improved resource management.
        
        Args:
            profile_id: ID of profile to stop
            force_kill: If True, will use stopBrowser and kill processes if needed.
                        If None, uses value from config.
            max_stop_wait: Maximum time to wait for profile to stop completely (seconds).
                          If None, uses value from config.
            
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        # Parameter validation
        try:
            profile_id = validate_profile_id(profile_id)
        except ValueError as e:
            self.logger.warning(f"Cannot stop profile: {str(e)}")
            return False
        
        # Use config values if parameters are not provided
        if force_kill is None:
            force_kill = self.config.shutdown["force_kill"]
        
        if max_stop_wait is None:
            max_stop_wait = self.config.shutdown["max_wait"]
        
        if not self._init_genlogin_client():
            self.logger.warning("Cannot stop profile: Unable to connect to GenLogin API")
            return False
        
        # Check if profile is running before stopping
        status = self._check_profile_status(profile_id)
        if not status["running"]:
            self.logger.info(f"Profile {profile_id} is not running, no need to stop")
            return True
        
        # Stop profile if running
        try:
            self.logger.info(f"Attempting to stop profile {profile_id} via GenLogin...")
            
            # Set timeout for stop operation
            original_timeout = None
            if hasattr(self.gen, 'request_timeout'):
                original_timeout = self.gen.request_timeout
                self.gen.request_timeout = 10  # 10 seconds for stop command
            
            try:
                # Use stopProfile command
                stop_result = self.gen.stopProfile(profile_id)
                stop_success = stop_result.get("success", False)
                
                if stop_success:
                    self.logger.info(f"Successfully sent stop command for profile {profile_id}")
                else:
                    error_msg = stop_result.get("message", "Unknown error")
                    self.logger.warning(f"Cannot stop profile {profile_id}: {error_msg}")
                    
                    # Try stopBrowser if stopProfile failed and force_kill=True
                    if force_kill:
                        try:
                            self.logger.info(f"Trying stopBrowser for profile {profile_id}...")
                            self.gen.stopBrowser(profile_id)
                            self.logger.info(f"Stopped browser for profile {profile_id}")
                            stop_success = True
                        except Exception as sb_e:
                            self.logger.warning(f"Error using stopBrowser: {str(sb_e)}")
            finally:
                # Restore original timeout
                if hasattr(self.gen, 'request_timeout') and original_timeout is not None:
                    self.gen.request_timeout = original_timeout
            
            # Wait for profile to stop completely
            stop_attempts = 0
            profile_stopped = False
            
            self.logger.info(f"Waiting for profile {profile_id} to stop completely (max {max_stop_wait}s)...")
            while stop_attempts < max_stop_wait:
                try:
                    # Check if profile is still running
                    status = self._check_profile_status(profile_id)
                    if not status["running"]:
                        self.logger.info(f"Profile {profile_id} stopped successfully after {stop_attempts}s")
                        profile_stopped = True
                        
                        # Wait additional time to ensure resources are released
                        additional_wait = self.config.shutdown["post_stop_delay"]
                        self.logger.debug(f"Waiting additional {additional_wait}s to ensure resources are released...")
                        time.sleep(additional_wait)
                        
                        # Clean up remaining Chrome processes
                        if force_kill and self.config.processes["cleanup_orphaned"]:
                            self.logger.info("Thoroughly checking for related Chrome processes...")
                            try:
                                cleanup_chrome_processes(profile_id)
                            except Exception as cleanup_e:
                                self.logger.warning(f"Error during process cleanup: {str(cleanup_e)}")
                        
                        break
                except Exception as check_e:
                    self.logger.debug(f"Error checking profile status: {str(check_e)}")
                    # Ignore error, continue waiting
                
                time.sleep(1)
                stop_attempts += 1
            
            # If still not stopped after maximum wait
            if not profile_stopped and force_kill:
                self.logger.warning(f"Could not stop profile {profile_id} after {max_stop_wait}s")
                self.logger.info("Trying stronger stopBrowser command...")
                
                try:
                    # Use stronger stopBrowser command
                    stop_browser_result = self.gen.stopBrowser(profile_id)
                    self.logger.debug(f"stopBrowser result: {stop_browser_result}")
                    
                    # Wait additional time after using strong command
                    self.logger.info("Waiting additional time after using stopBrowser...")
                    time.sleep(3)
                    
                    # Clean up orphaned Chrome processes related to profile
                    if self.config.processes["cleanup_orphaned"]:
                        try:
                            cleanup_chrome_processes(profile_id)
                        except Exception as cleanup_e:
                            self.logger.warning(f"Error during process cleanup: {str(cleanup_e)}")
                except Exception as sb_e:
                    self.logger.warning(f"Error calling stopBrowser: {str(sb_e)}")
            
            # Final check if profile has stopped
            final_status = self._check_profile_status(profile_id)
            profile_still_running = final_status["running"]
            
            # If force_kill=True, ensure thorough cleanup of processes
            if force_kill and profile_still_running and self.config.processes["cleanup_orphaned"]:
                try:
                    cleanup_chrome_processes(profile_id)
                except Exception as cleanup_e:
                    self.logger.warning(f"Error during final process cleanup: {str(cleanup_e)}")
                # Return True if thorough cleanup has been attempted
                return True
            
            # Remove from cache if stopped
            if not profile_still_running and profile_id in self._running_profiles_cache["data"]:
                self._running_profiles_cache["data"].remove(profile_id)
                self.logger.debug(f"Removed profile {profile_id} from running profiles cache")
            
            return not profile_still_running
            
        except Exception as e:
            self.logger.error(f"Error stopping profile {profile_id}: {type(e).__name__}: {str(e)}")
            
            # Clean up processes if force_kill=True
            if force_kill and self.config.processes["cleanup_orphaned"]:
                try:
                    cleanup_chrome_processes(profile_id)
                except Exception as cleanup_e:
                    self.logger.warning(f"Error during process cleanup after exception: {str(cleanup_e)}")
                return True
            
            return False
    
    def _stop_profile_if_running(
        self, 
        profile_id: str, 
        force_stop_running: bool = True,
        force_kill: Optional[bool] = None
    ) -> bool:
        """
        Check and stop profile if running.
        
        Args:
            profile_id: ID of profile to check and stop
            force_stop_running: If True, will stop profile if running
            force_kill: If True, will use stopBrowser and kill processes if needed.
                        If None, uses value from config.
            
        Returns:
            bool: True if profile is not running or was stopped successfully, False otherwise
            
        Raises:
            ProfileRunningError: If profile is running and force_stop_running=False
        """
        if not profile_id:
            return True  # No profile_id, no need to check
        
        if not self._init_genlogin_client():
            self.logger.warning("Cannot check profile: Unable to connect to GenLogin API")
            return False
        
        # Check if profile is running
        status = self._check_profile_status(profile_id)
        
        if status["running"]:
            self.logger.warning(f"Profile {profile_id} is currently running")
            
            if force_stop_running:
                # Stop profile
                stop_success = self.stop_profile(profile_id, force_kill=force_kill)
                
                # Wait to ensure resources are released
                time.sleep(3)
                
                # Clean up remaining Chrome processes
                if (force_kill or (force_kill is None and self.config.shutdown["force_kill"])) and self.config.processes["cleanup_orphaned"]:
                    try:
                        cleanup_chrome_processes(profile_id)
                    except Exception as cleanup_e:
                        self.logger.warning(f"Error during process cleanup: {str(cleanup_e)}")
                
                return stop_success
            else:
                raise ProfileRunningError(profile_id)
        
        return True  # Profile is not running
    
    def _start_profile_internal(self, profile_id: str, options: Dict[str, Any] = None) -> Optional[str]:
        """
        Internal method to start a GenLogin profile and return wsEndpoint.
        
        Args:
            profile_id: ID of profile to start
            options: Dictionary with startup options, overriding config
            
        Returns:
            Optional[str]: wsEndpoint if successful, None otherwise
            
        Raises:
            ProfileStartError: If unable to start profile
        """
        if not profile_id:
            raise ValueError("Cannot start profile: Invalid ID")
        
        if not self._init_genlogin_client():
            raise ProfileStartError(profile_id, "Unable to connect to GenLogin API")
        
        # Merge options with config
        opts = {
            "max_retries": self.config.retry["max_attempts"],
            "force_restart": False,
            "window_check_delay": self.config.startup["check_delay"],
            "verify_ws_endpoint": self.config.startup["verify_ws_endpoint"],
            "profile_startup_max_time": self.config.startup["max_time"],
            "connection_check_timeout": self.config.startup["connection_check_timeout"]
        }
        
        if options:
            opts.update(options)
        
        # Check if profile is already running
        if not opts["force_restart"]:
            status = self._check_profile_status(profile_id)
            if status["running"] and status["ws_endpoint"]:
                self.logger.info(f"Profile {profile_id} is already running, getting current wsEndpoint")
                wsEndpoint = status["ws_endpoint"].replace("ws://", "").split('/')[0]
                self.logger.info(f"Got current wsEndpoint: {wsEndpoint}")
                return wsEndpoint
        
        # Start profile with retry mechanism
        wsEndpoint = None
        profile_start_attempts = 0
        
        while profile_start_attempts < opts["max_retries"]:
            try:
                self.logger.info(f"Starting profile {profile_id} (attempt {profile_start_attempts+1}/{opts['max_retries']})...")
                
                # Set timeout for startup operation
                original_timeout = None
                if hasattr(self.gen, 'request_timeout'):
                    original_timeout = self.gen.request_timeout
                    self.gen.request_timeout = max(15, opts["profile_startup_max_time"] // 2)  # Large enough timeout
                
                try:
                    # force_restart=True if option is True or this is not the first attempt
                    force_restart_param = profile_start_attempts > 0 or opts["force_restart"]
                    
                    self.logger.debug(f"Calling runProfile with force_restart={force_restart_param}")
                    if force_restart_param:
                        ws_data = self.gen.runProfile(profile_id, force_restart=True)
                    else:
                        ws_data = self.gen.runProfile(profile_id)
                finally:
                    # Restore original timeout
                    if hasattr(self.gen, 'request_timeout') and original_timeout is not None:
                        self.gen.request_timeout = original_timeout
                
                if not ws_data.get("success", False):
                    error_msg = ws_data.get("message", "Unknown error")
                    self.logger.warning(
                        f"Cannot run profile {profile_id}: {error_msg} "
                        f"(attempt {profile_start_attempts+1}/{opts['max_retries']})"
                    )
                    
                    # If this was the last attempt
                    if profile_start_attempts == opts["max_retries"] - 1:
                        raise ProfileStartError(
                            profile_id,
                            f"Cannot start profile after {opts['max_retries']} attempts: {error_msg}"
                        )
                    
                    # Try next attempt with delay
                    profile_start_attempts += 1
                    wait_time = 3.0 * (2.0 ** profile_start_attempts)
                    if random.random() > 0.5:  # Random jitter
                        wait_time *= random.uniform(0.8, 1.2)
                    
                    self.logger.info(f"Waiting {wait_time:.2f}s before retrying profile start...")
                    time.sleep(wait_time)
                    continue
                
                # Check wsEndpoint in result
                ws_endpoint = ws_data.get("wsEndpoint", "")
                if not ws_endpoint:
                    ws_endpoint = ws_data.get("data", {}).get("wsEndpoint", "")
                
                if not ws_endpoint:
                    self.logger.warning(
                        f"runProfile did not return valid wsEndpoint: {ws_data} "
                        f"(attempt {profile_start_attempts+1}/{opts['max_retries']})"
                    )
                    
                    # If this was the last attempt
                    if profile_start_attempts == opts["max_retries"] - 1:
                        raise ProfileStartError(profile_id, "runProfile did not return valid wsEndpoint")
                    
                    # Try next attempt
                    profile_start_attempts += 1
                    wait_time = 3.0 * (2.0 ** profile_start_attempts)
                    if random.random() > 0.5:  # Random jitter
                        wait_time *= random.uniform(0.8, 1.2)
                    
                    self.logger.info(f"Waiting {wait_time:.2f}s before retrying profile start...")
                    time.sleep(wait_time)
                    continue
                
                # Normalize wsEndpoint format
                wsEndpoint = ws_endpoint.replace("ws://", "").split('/')[0]
                self.logger.info(f"Received wsEndpoint: {wsEndpoint}")
                
                # Verify wsEndpoint if needed
                if opts["verify_ws_endpoint"]:
                    self.logger.debug(f"Verifying connection to wsEndpoint: {wsEndpoint}")
                    host, port = wsEndpoint.split(':')
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                            sock.settimeout(opts["connection_check_timeout"])
                            result = sock.connect_ex((host, int(port)))
                        
                        if result != 0:
                            self.logger.warning(f"wsEndpoint {wsEndpoint} cannot be connected (error code: {result})")
                            
                            # If this was the last attempt
                            if profile_start_attempts == opts["max_retries"] - 1:
                                raise ProfileStartError(
                                    profile_id,
                                    f"wsEndpoint {wsEndpoint} cannot be connected after {opts['max_retries']} attempts"
                                )
                            
                            # Try next attempt
                            profile_start_attempts += 1
                            wait_time = 3.0 * (2.0 ** profile_start_attempts)
                            if random.random() > 0.5:  # Random jitter
                                wait_time *= random.uniform(0.8, 1.2)
                            
                            self.logger.info(f"Waiting {wait_time:.2f}s before retrying profile start...")
                            time.sleep(wait_time)
                            continue
                        else:
                            self.logger.debug(f"wsEndpoint {wsEndpoint} can be connected")
                    except Exception as sock_e:
                        self.logger.warning(f"Error verifying wsEndpoint connection: {str(sock_e)}")
                        # Continue even if verification fails
                
                # Wait for profile to start completely - increase wait time for safety
                adaptive_delay = opts["window_check_delay"] * (1 + profile_start_attempts * 0.5)
                self.logger.info(f"Waiting {adaptive_delay:.2f}s for profile to start completely...")
                time.sleep(adaptive_delay)
                
                # Verify profile is actually running
                try:
                    self.logger.debug("Verifying profile status before continuing...")
                    status = self._check_profile_status(profile_id)
                    if not status["running"]:
                        self.logger.warning(f"Profile {profile_id} started but does not appear in running list")
                        
                        # Try to verify wsEndpoint to confirm
                        try:
                            ws_check = self.gen.getWsEndpoint(profile_id)
                            if ws_check.get("success", False) and ws_check.get("data", {}).get("wsEndpoint"):
                                self.logger.info("Profile is running (confirmed via getWsEndpoint)")
                                break  # Valid wsEndpoint, continue
                            else:
                                self.logger.warning("Profile does not return valid wsEndpoint when checked")
                        except Exception as ws_check_e:
                            self.logger.warning(f"Error checking getWsEndpoint: {str(ws_check_e)}")
                        
                        # If this was the last attempt
                        if profile_start_attempts == opts["max_retries"] - 1:
                            raise ProfileStartError(
                                profile_id,
                                f"Profile does not appear in running list after startup"
                            )
                        
                        # Try next attempt
                        profile_start_attempts += 1
                        wait_time = 3.0 * (2.0 ** profile_start_attempts)
                        if random.random() > 0.5:  # Random jitter
                            wait_time *= random.uniform(0.8, 1.2)
                        
                        self.logger.info(f"Waiting {wait_time:.2f}s before retrying profile start...")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.info(f"Confirmed profile {profile_id} is running")
                        
                        # Update running profiles cache
                        if profile_id not in self._running_profiles_cache["data"]:
                            self._running_profiles_cache["data"].append(profile_id)
                            self._running_profiles_cache["timestamp"] = datetime.now()
                except Exception as recheck_e:
                    self.logger.warning(f"Error rechecking profile status: {str(recheck_e)}")
                
                # If everything is fine, return wsEndpoint
                return wsEndpoint
                
            except Exception as e:
                self.logger.warning(
                    f"Error running profile (attempt {profile_start_attempts+1}/{opts['max_retries']}): "
                    f"{type(e).__name__}: {str(e)}"
                )
                
                # If this was the last attempt
                if profile_start_attempts == opts["max_retries"] - 1:
                    self.logger.error(f"Cannot start profile after {opts['max_retries']} attempts")
                    raise ProfileStartError(
                        profile_id,
                        f"Error running profile after {opts['max_retries']} attempts: {str(e)}"
                    )
                
                # Try next attempt
                profile_start_attempts += 1
                wait_time = 3.0 * (2.0 ** profile_start_attempts)
                if random.random() > 0.5:  # Random jitter
                    wait_time *= random.uniform(0.8, 1.2)
                
                self.logger.info(f"Waiting {wait_time:.2f}s before retrying profile start...")
                time.sleep(wait_time)
        
        # Should never reach here due to exceptions in the loop
        raise ProfileStartError(profile_id, "Failed to get valid wsEndpoint after all attempts")
    
    def start_profile(
        self, 
        profile_id: Optional[str] = None,
        force_stop_running: bool = True,
        force_restart: bool = False,
        max_retries: Optional[int] = None,
        start_options: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, str]:
        """
        Start a GenLogin profile. If profile_id is not specified, an available profile will be selected.
        
        Args:
            profile_id: ID of profile to start. If None, an available profile will be selected
            force_stop_running: If True, will stop profile if running on another device
            force_restart: If True, force restart even if profile is already running
            max_retries: Maximum number of retry attempts. If None, uses value from config
            start_options: Additional options for profile startup, overriding config
            
        Returns:
            Tuple[str, str]: (profile_id, wsEndpoint)
            
        Raises:
            ProfileStartError: If unable to start profile
            ProfileRunningError: If profile is running and force_stop_running=False
        """
        # If profile_id is not specified, select an available profile
        actual_profile_id = profile_id
        if not actual_profile_id:
            self.logger.info("No profile_id specified, selecting an available profile")
            actual_profile_id = self.select_available_profile()
            if not actual_profile_id:
                raise ProfileStartError("<unknown>", "No GenLogin profiles found!")
        else:
            try:
                actual_profile_id = validate_profile_id(actual_profile_id)
            except ValueError as e:
                raise ProfileStartError("<invalid>", str(e))
                
        self.logger.info(f"Using profile ID: {actual_profile_id}")
        
        # Prepare start options
        options = start_options or {}
        if max_retries is not None:
            options["max_retries"] = max_retries
        options["force_restart"] = force_restart
        
        # Check and stop profile if running
        if not self._stop_profile_if_running(actual_profile_id, force_stop_running):
            self.logger.warning(f"Cannot stop running profile {actual_profile_id}")
            
            # Clean up orphaned Chrome processes
            if self.config.processes["cleanup_orphaned"]:
                try:
                    cleanup_chrome_processes(actual_profile_id)
                except Exception as cleanup_e:
                    self.logger.warning(f"Error during process cleanup: {str(cleanup_e)}")
            
            # Wait to ensure resources are released
            time.sleep(3)
        
        # Start profile
        wsEndpoint = self._start_profile_internal(
            actual_profile_id,
            options=options
        )
        
        if not wsEndpoint:
            raise ProfileStartError(actual_profile_id, "Could not obtain wsEndpoint")
            
        return actual_profile_id, wsEndpoint

# -------------------------------
# End of Module
# -------------------------------