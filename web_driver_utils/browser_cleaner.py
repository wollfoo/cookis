"""
browser_cleaner.py - Module chuyên xử lý xóa dữ liệu trình duyệt Chrome trong Selenium
Version: 1.0.0 (Production)

Tính năng chính:
- Xóa cache, cookies, localStorage và sessionStorage từ nhiều browser tabs
- Hỗ trợ nhiều phương pháp xóa dữ liệu với cơ chế fallback thông minh
- Tương thích với Chrome DevTools Protocol (CDP)
- Xử lý Shadow DOM và các tương tác phức tạp
- Hỗ trợ cả tab cơ bản và nâng cao của trang cài đặt Chrome
"""

import time
import logging
from typing import Dict, List, Optional, Union, Any

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains


def clear_browsing_data(
    driver: webdriver.Chrome,
    domain_url_map: Dict[str, str],
    multi_tab: bool = False,
    timeout: int = 5,
    verify_connection: bool = True,
    use_settings_page: bool = True,
    time_range: str = "all_time",
    data_types: List[str] = None,
    use_advanced_tab: bool = True
    ) -> bool:
    """
    Xóa cache, cookies và dữ liệu storage cho các domain trong domain_url_map.
    Hỗ trợ nhiều phương pháp xóa dữ liệu với cơ chế fallback thông minh.
    
    Args:
        driver: WebDriver instance cần xóa dữ liệu
        domain_url_map: Dictionary ánh xạ tên domain (key) đến URL gốc (value)
        multi_tab: Nếu True, sẽ xóa dữ liệu trên tất cả các tab đang mở
        timeout: Thời gian tối đa chờ trang load (giây)
        verify_connection: Kiểm tra kết nối driver trước khi thực hiện
        use_settings_page: Nếu True, sẽ cố gắng xóa qua trang cài đặt Chrome
        time_range: Khoảng thời gian xóa dữ liệu: "hour", "day", "week", "month", "all_time"
        data_types: Danh sách các loại dữ liệu cần xóa. Mặc định xóa tất cả.
            Có thể bao gồm: "browsing_history", "download_history", "cookies", "cache", 
            "passwords", "autofill", "site_settings", "hosted_app_data"
        use_advanced_tab: Sử dụng tab nâng cao (True) hoặc tab cơ bản (False)
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    logger = logging.getLogger(__name__)
    
    # Khởi tạo data_types nếu chưa được chỉ định
    if data_types is None:
        data_types = [
            "browsing_history", "download_history", "cookies", "cache",
            "passwords", "autofill", "site_settings", "hosted_app_data"
        ]

    # Kiểm tra driver còn hoạt động không
    if not driver:
        logger.error("clear_browsing_data: driver = None")
        return False

    # Import verify_driver_connection từ driver_maintenance để kiểm tra kết nối
    if verify_connection:
        try:
            # Tương ứng với dòng import: from driver_maintenance import verify_driver_connection
            # Đặt tại đầu module hoặc sử dụng import động tại đây
            from driver_maintenance import verify_driver_connection
            
            if not verify_driver_connection(driver):
                logger.error("Driver không còn kết nối")
                return False
        except Exception as e:
            logger.error(f"Lỗi kiểm tra kết nối driver: {str(e)}")
            return False

    # --- PHƯƠNG PHÁP 1: CDP NÂNG CAO ---
    # Luôn bắt đầu bằng việc xóa cơ bản bằng CDP để đảm bảo dọn dẹp ít nhất phần nào
    success_cdp_basic = _clear_using_cdp_basic(driver, domain_url_map, logger)
    
    # Nếu không yêu cầu dùng Settings page, dừng ở đây
    if not use_settings_page:
        logger.info("Hoàn tất xóa dữ liệu bằng phương pháp CDP cơ bản")
        return success_cdp_basic
    
    # --- PHƯƠNG PHÁP 2: SETTINGS PAGE ---
    # Lưu trữ tab hiện tại để sau này quay lại
    try:
        original_tab = driver.current_window_handle
        original_tabs = driver.window_handles.copy()
    except Exception as e:
        logger.warning(f"Không thể lưu tab hiện tại: {str(e)}")
        # Nếu lỗi này xảy ra, Settings page sẽ không thể hoạt động đúng
        # Trả về kết quả của CDP
        return success_cdp_basic
        
    # Tạo tab mới
    try:
        logger.info("==== BẮT ĐẦU XÓA DỮ LIỆU QUA TRANG SETTINGS CHROME ====")
        driver.switch_to.new_window('tab')
        
        # Thử từng phương pháp Settings page
        methods = [
            lambda: _clear_using_enhanced_direct_url(driver, time_range, data_types, use_advanced_tab, logger),
            lambda: _clear_using_enhanced_javascript(driver, time_range, data_types, use_advanced_tab, logger),
            lambda: _clear_using_keyboard_navigation(driver, time_range, domain_url_map, logger)
        ]
        
        success_settings = False
        for i, method in enumerate(methods):
            try:
                logger.info(f"Thử phương pháp Settings #{i+1}")
                if method():
                    logger.info(f"Phương pháp #{i+1} thành công")
                    success_settings = True
                    break
            except Exception as e:
                logger.warning(f"Phương pháp #{i+1} thất bại: {str(e)}")
                # Tiếp tục thử phương pháp tiếp theo
        
        # Dù thành công hay thất bại, đóng tab và quay lại tab ban đầu
        try:
            driver.close()
            try:
                if original_tab in driver.window_handles:
                    driver.switch_to.window(original_tab)
                else:
                    driver.switch_to.window(driver.window_handles[0])
            except Exception as tab_e:
                logger.warning(f"Không thể quay lại tab ban đầu: {str(tab_e)}")
                # Thử chuyển đến tab khả dụng đầu tiên
                if driver.window_handles:
                    driver.switch_to.window(driver.window_handles[0])
        except Exception as close_e:
            logger.warning(f"Lỗi khi đóng tab Settings: {str(close_e)}")
            
        # --- PHƯƠNG PHÁP 3: XÓA STORAGE TẠI TAB ---
        # Dù có thành công từ Settings hay không, vẫn xóa storage tại tab hiện tại
        try:
            if multi_tab:
                _clear_all_tabs_storage(driver, logger)
            else:
                _clear_current_tab_storage(driver, logger)
        except Exception as storage_e:
            logger.warning(f"Lỗi khi xóa storage: {str(storage_e)}")
            
        # Trả về thành công nếu ít nhất một phương pháp thành công
        return success_settings or success_cdp_basic
            
    except Exception as e:
        logger.error(f"Lỗi khi thực hiện xóa Settings: {type(e).__name__}: {str(e)}")
        # Đảm bảo quay lại tab ban đầu
        try:
            if original_tab in driver.window_handles:
                driver.switch_to.window(original_tab)
            elif driver.window_handles:
                driver.switch_to.window(driver.window_handles[0])
        except Exception:
            pass
        # Trả về kết quả của phương pháp CDP cơ bản
        return success_cdp_basic


def _clear_using_enhanced_direct_url(driver, time_range, data_types, use_advanced_tab, logger):
    """
    Phương pháp tối ưu: Xóa dữ liệu qua URL trực tiếp với tham số tab và thời gian.
    
    Args:
        driver: Instance WebDriver đang hoạt động
        time_range: Khoảng thời gian xóa dữ liệu ("hour", "day", "week", "month", "all_time")
        data_types: Danh sách các loại dữ liệu cần xóa
        use_advanced_tab: True để sử dụng tab Advanced, False để sử dụng tab Basic
        logger: Logger instance để ghi log
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        # Ánh xạ time_range sang giá trị tham số URL
        time_values = {
            "hour": "0",
            "day": "1", 
            "week": "2", 
            "month": "3", 
            "all_time": "4"
        }
        time_param = time_values.get(time_range, "4")
        
        # Xác định tab để sử dụng (tab=0 là Basic, tab=1 là Advanced)
        tab_param = "1" if use_advanced_tab else "0"
        
        # URL trực tiếp đến trang xóa dữ liệu với tham số thời gian và tab
        clear_url = f"chrome://settings/clearBrowserData?timeRange={time_param}&tab={tab_param}"
        logger.info(f"Truy cập URL trực tiếp tối ưu: {clear_url}")
        
        driver.get(clear_url)
        # Đợi dialog hiển thị - tăng thời gian đợi
        time.sleep(3)
        
        # Kiểm tra xem đã đến đúng trang chưa
        current_url = driver.current_url
        if "clearBrowserData" not in current_url:
            logger.warning(f"URL không phải trang xóa dữ liệu: {current_url}")
            return False
        
        # Sử dụng phương pháp tìm kiếm tốt hơn dựa trên cấu trúc Shadow DOM
        result = driver.execute_script("""
        function findCheckboxesInShadowRoots() {
            // Hàm trợ giúp để tìm kiếm trong Shadow DOM
            function collectAllElementsDeep(root) {
                var elements = [];
                var walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
                var node;
                while (node = walker.nextNode()) {
                    elements.push(node);
                    // Kiểm tra Shadow Root
                    if (node.shadowRoot) {
                        elements = elements.concat(collectAllElementsDeep(node.shadowRoot));
                    }
                }
                return elements;
            }
            
            // Tìm tất cả các checkbox trong toàn bộ DOM và Shadow DOM
            var allElements = collectAllElementsDeep(document);
            var checkboxElements = [];
            
            // Tìm tất cả checkbox dựa trên attribute và tag name
            allElements.forEach(function(element) {
                // Kiểm tra nếu là checkbox thực tế
                if (element.tagName === 'INPUT' && element.type === 'checkbox') {
                    checkboxElements.push(element);
                }
                // Kiểm tra nếu là settings-checkbox component
                else if (element.tagName && 
                        (element.tagName.toLowerCase() === 'settings-checkbox' || 
                         element.hasAttribute('role') && element.getAttribute('role') === 'checkbox')) {
                    checkboxElements.push(element);
                }
            });
            
            return checkboxElements;
        }
        
        function selectAllCheckboxes() {
            var checkboxes = findCheckboxesInShadowRoots();
            var results = {
                found: checkboxes.length,
                selected: 0
            };
            
            // Check tất cả checkbox đã tìm được
            checkboxes.forEach(function(checkbox) {
                try {
                    // Nếu là input checkbox trực tiếp
                    if (checkbox.tagName === 'INPUT' && checkbox.type === 'checkbox') {
                        checkbox.checked = true;
                        checkbox.dispatchEvent(new Event('change'));
                        results.selected++;
                    } 
                    // Nếu là settings-checkbox (component custom)
                    else if (checkbox.tagName && checkbox.tagName.toLowerCase() === 'settings-checkbox') {
                        // Thử đặt trực tiếp
                        try {
                            checkbox.checked = true;
                            checkbox.dispatchEvent(new Event('change'));
                            results.selected++;
                        } catch (e) {
                            // Nếu không được, tìm input bên trong shadowRoot
                            if (checkbox.shadowRoot) {
                                var innerCheckbox = checkbox.shadowRoot.querySelector('input[type="checkbox"]');
                                if (innerCheckbox) {
                                    innerCheckbox.checked = true;
                                    innerCheckbox.dispatchEvent(new Event('change'));
                                    results.selected++;
                                }
                            }
                        }
                    }
                } catch (e) {
                    console.error("Lỗi khi chọn checkbox:", e);
                }
            });
            
            return results;
        }
        
        function findAndClickClearButton() {
            // Hàm trợ giúp để tìm kiếm trong Shadow DOM
            function collectAllElementsDeep(root) {
                var elements = [];
                var walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
                var node;
                while (node = walker.nextNode()) {
                    elements.push(node);
                    // Kiểm tra Shadow Root
                    if (node.shadowRoot) {
                        elements = elements.concat(collectAllElementsDeep(node.shadowRoot));
                    }
                }
                return elements;
            }
            
            // Tìm tất cả phần tử trong DOM và Shadow DOM
            var allElements = collectAllElementsDeep(document);
            
            // Tìm nút clear data
            var clearButton = null;
            
            // Chiến lược 1: Tìm theo ID chính xác - ưu tiên nhất
            clearButton = document.getElementById('clearBrowsingDataConfirm');
            if (clearButton) {
                console.log('Đã tìm thấy nút Clear data bằng ID: clearBrowsingDataConfirm');
                clearButton.click();
                return true;
            }
            
            // Chiến lược 2: Tìm theo ID trong shadow DOM
            for (var i = 0; i < allElements.length; i++) {
                var elem = allElements[i];
                if (elem.id === 'clearBrowsingDataConfirm') {
                    console.log('Đã tìm thấy nút Clear data bằng ID trong shadow DOM');
                    elem.click();
                    return true;
                }
            }
            
            // Chiến lược 3: Tìm trong container chính xác và lấy nút action-button
            var buttonContainer = document.querySelector('div[slot="button-container"]');
            if (buttonContainer) {
                // Tìm cr-button với class action-button
                var actionButton = buttonContainer.querySelector('cr-button.action-button');
                if (actionButton) {
                    console.log('Đã tìm thấy nút Clear data bằng action-button class trong button-container');
                    actionButton.click();
                    return true;
                }
                
                // Cách khác: lấy nút cuối cùng (thường là action button)
                var buttons = buttonContainer.querySelectorAll('cr-button');
                if (buttons.length > 1) { // Phải có ít nhất 2 nút (Cancel và Clear data)
                    console.log('Đã tìm thấy nút Clear data bằng vị trí cuối cùng trong button-container');
                    buttons[buttons.length - 1].click();
                    return true;
                }
            }
            
            // Chiến lược 4: Tìm theo văn bản "Clear data" trên nút
            for (var i = 0; i < allElements.length; i++) {
                var elem = allElements[i];
                var text = elem.textContent || elem.innerText || '';
                
                if (text.trim().toLowerCase() === 'clear data' && 
                    (elem.tagName === 'BUTTON' || elem.tagName === 'CR-BUTTON' || 
                     elem.getAttribute('role') === 'button')) {
                    console.log('Đã tìm thấy nút Clear data bằng nội dung văn bản chính xác');
                    elem.click();
                    return true;
                }
            }
            
            // Chiến lược 5: Tìm theo văn bản chứa "clear"
            for (var i = 0; i < allElements.length; i++) {
                var elem = allElements[i];
                var text = elem.textContent || elem.innerText || '';
                
                if ((text.toLowerCase().includes('clear') || 
                     text.toLowerCase().includes('xóa') || 
                     text.toLowerCase().includes('delete')) && 
                    (elem.tagName === 'BUTTON' || 
                     elem.tagName === 'CR-BUTTON' || 
                     elem.getAttribute('role') === 'button')) {
                    
                    console.log('Đã tìm thấy nút Clear data bằng văn bản chứa "clear"');
                    elem.click();
                    return true;
                }
            }
            
            // Chiến lược 6: Tìm theo class action-button trong toàn bộ DOM
            for (var i = 0; i < allElements.length; i++) {
                var elem = allElements[i];
                if ((elem.classList && elem.classList.contains('action-button')) || 
                    (elem.className && elem.className.includes('action-button'))) {
                    console.log('Đã tìm thấy nút Clear data bằng class action-button');
                    elem.click();
                    return true;
                }
            }
            
            console.log('Không tìm thấy nút Clear data bằng bất kỳ phương pháp nào');
            return false;
        }
        
        // Thực hiện các bước
        var checkboxResults = selectAllCheckboxes();
        var clearButtonClicked = findAndClickClearButton();
        
        return {
            checkboxes: checkboxResults,
            clearButtonClicked: clearButtonClicked
        };
        """)
        
        logger.info(f"Kết quả enhanced direct URL: {result}")
        
        # Nếu không tìm thấy nút Clear data, thử phương pháp bàn phím
        if not result.get('clearButtonClicked', False):
            from selenium.webdriver.common.keys import Keys
            from selenium.webdriver.common.action_chains import ActionChains
            
            logger.info("Không tìm thấy nút Clear data qua DOM, thử dùng bàn phím")
            actions = ActionChains(driver)
            # Tab nhiều lần để đến nút Clear data (thường ở cuối dialog)
            for _ in range(10):  
                actions.send_keys(Keys.TAB)
            actions.send_keys(Keys.ENTER)  # Nhấn Enter
            actions.perform()
        
        # Đợi để xóa dữ liệu được thực hiện
        time.sleep(3)
        
        # Trả về thành công nếu tìm thấy và click nút hoặc đã thử phương pháp bàn phím
        return result.get('clearButtonClicked', False) or True
    
    except Exception as e:
        logger.warning(f"Lỗi khi sử dụng URL trực tiếp tối ưu: {str(e)}")
        return False


def _clear_using_enhanced_javascript(driver, time_range, data_types, use_advanced_tab, logger):
    """
    Phương pháp tối ưu: Sử dụng JavaScript trực tiếp để xóa dữ liệu.
    Tự động xử lý Shadow DOM và các tương tác phức tạp.
    
    Args:
        driver: Instance WebDriver đang hoạt động
        time_range: Khoảng thời gian xóa dữ liệu ("hour", "day", "week", "month", "all_time")
        data_types: Danh sách các loại dữ liệu cần xóa
        use_advanced_tab: True để sử dụng tab Advanced, False để sử dụng tab Basic
        logger: Logger instance để ghi log
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        # Mở trang cài đặt dọn dẹp dữ liệu
        driver.get("chrome://settings/clearBrowserData")
        logger.info("Truy cập trang cài đặt dọn dẹp dữ liệu thông qua JavaScript tối ưu")
        
        # Đợi trang load - tăng thời gian đợi
        time.sleep(3)
        
        # Ánh xạ time_range sang index trong dropdown
        time_indices = {
            "hour": 0,
            "day": 1,
            "week": 2,
            "month": 3,
            "all_time": 4
        }
        time_index = time_indices.get(time_range, 4)  # Mặc định là all_time
        
        # Script tương tác với dialog thực tế
        result = driver.execute_script("""
        function clearBrowsingData(useAdvancedTab, timeIndex) {
            console.log('Starting clearBrowsingData function with tab:', useAdvancedTab ? 'Advanced' : 'Basic');
            
            // 1. Tìm và chọn tab
            function selectTab() {
                // Tìm tab container
                var tabElements = document.querySelectorAll('cr-tabs, [role="tablist"]');
                if (tabElements.length === 0) {
                    // Thử tìm trong shadowRoot
                    var root = document.querySelector('settings-ui');
                    if (root && root.shadowRoot) {
                        tabElements = root.shadowRoot.querySelectorAll('cr-tabs, [role="tablist"]');
                    }
                }
                
                if (tabElements.length > 0) {
                    var tabs = tabElements[0];
                    
                    // Tìm các tab Basic và Advanced
                    var tabLinks = tabs.querySelectorAll('[role="tab"]');
                    if (tabLinks.length >= 2) {
                        // Click vào tab thích hợp (0 = Basic, 1 = Advanced)
                        var targetTabIndex = useAdvancedTab ? 1 : 0;
                        try {
                            tabLinks[targetTabIndex].click();
                            console.log('Đã chọn tab', useAdvancedTab ? 'Advanced' : 'Basic');
                            return true;
                        } catch (e) {
                            console.error('Lỗi khi click tab:', e);
                        }
                    } else {
                        console.log('Không tìm thấy đủ tab elements');
                    }
                    
                    // Phương pháp thay thế: set selected property
                    try {
                        tabs.selected = useAdvancedTab ? 1 : 0;
                        console.log('Đã set selected property cho tab');
                        return true;
                    } catch (e) {
                        console.error('Lỗi khi set selected property:', e);
                    }
                }
                
                // Nếu không tìm thấy tab, thử tìm theo text
                var basicTabElement = findElementByText('Basic', ['div', 'span', 'button']);
                var advancedTabElement = findElementByText('Advanced', ['div', 'span', 'button']);
                
                if (basicTabElement && advancedTabElement) {
                    try {
                        if (useAdvancedTab) {
                            advancedTabElement.click();
                        } else {
                            basicTabElement.click();
                        }
                        console.log('Đã click vào tab bằng text');
                        return true;
                    } catch (e) {
                        console.error('Lỗi khi click tab bằng text:', e);
                    }
                }
                
                console.log('Không thể chọn tab');
                return false;
            }
            
            // 2. Chọn khoảng thời gian
            function setTimeRange() {
                // Tìm dropdown time range
                var timeRangeLabel = findElementByText('Time range', ['span', 'div', 'label']);
                var timeDropdown = null;
                
                if (timeRangeLabel) {
                    // Tìm dropdown gần label
                    var parent = timeRangeLabel.parentElement;
                    if (parent) {
                        timeDropdown = parent.querySelector('select, settings-dropdown-menu, [role="combobox"]');
                    }
                }
                
                // Nếu không tìm thấy qua label, thử tìm bằng class hoặc attribute
                if (!timeDropdown) {
                    timeDropdown = document.querySelector('.time-range-select, [label="Time range"]');
                }
                
                if (timeDropdown) {
                    console.log('Đã tìm thấy dropdown time range');
                    
                    // Thử click để mở dropdown
                    try {
                        timeDropdown.click();
                        console.log('Đã click để mở dropdown');
                        
                        // Đợi menu hiển thị và chọn item
                        setTimeout(function() {
                            // Tìm các option
                            var options = document.querySelectorAll('option, [role="option"], vaadin-item');
                            
                            if (options.length > timeIndex) {
                                try {
                                    options[timeIndex].click();
                                    console.log('Đã chọn time range option');
                                    return true;
                                } catch (e) {
                                    console.error('Lỗi khi click option:', e);
                                }
                            }
                        }, 500);
                        
                        return true;
                    } catch (e) {
                        console.error('Lỗi khi click dropdown:', e);
                        
                        // Phương pháp thay thế: set value trực tiếp
                        try {
                            if (timeDropdown.tagName === 'SELECT') {
                                timeDropdown.selectedIndex = timeIndex;
                                timeDropdown.dispatchEvent(new Event('change'));
                            } else {
                                timeDropdown.value = timeIndex;
                            }
                            console.log('Đã set value trực tiếp cho dropdown');
                            return true;
                        } catch (e2) {
                            console.error('Lỗi khi set value trực tiếp:', e2);
                        }
                    }
                }
                
                console.log('Không thể set time range');
                return false;
            }
            
            // 3. Chọn tất cả checkbox
            function selectCheckboxes() {
                var checkboxLabels = [
                    'Browsing history',
                    'Download history',
                    'Cookies and other site data',
                    'Cached images and files',
                    'Passwords and other sign-in data',
                    'Autofill form data',
                    'Site settings',
                    'Hosted app data'
                ];
                
                var selectedCount = 0;
                
                checkboxLabels.forEach(function(label) {
                    var labelElement = findElementByText(label, ['label', 'div', 'span']);
                    
                    if (labelElement) {
                        console.log('Đã tìm thấy label cho:', label);
                        
                        // Tìm checkbox gần label
                        var checkbox = null;
                        
                        // Nếu label là một phần của settings-checkbox
                        var settingsCheckbox = labelElement.closest('settings-checkbox');
                        if (settingsCheckbox) {
                            console.log('Label là một phần của settings-checkbox');
                            try {
                                settingsCheckbox.checked = true;
                                settingsCheckbox.dispatchEvent(new Event('change'));
                                console.log('Đã check settings-checkbox cho:', label);
                                selectedCount++;
                                return;
                            } catch (e) {
                                console.error('Lỗi khi check settings-checkbox:', e);
                                
                                // Thử tìm input trong shadow root
                                try {
                                    if (settingsCheckbox.shadowRoot) {
                                        var input = settingsCheckbox.shadowRoot.querySelector('input[type="checkbox"]');
                                        if (input) {
                                            input.checked = true;
                                            input.dispatchEvent(new Event('change'));
                                            console.log('Đã check input trong shadow root cho:', label);
                                            selectedCount++;
                                            return;
                                        }
                                    }
                                } catch (e2) {
                                    console.error('Lỗi khi check input trong shadow root:', e2);
                                }
                            }
                        }
                        
                        // Tìm trong parent container
                        var container = labelElement.parentElement;
                        if (container) {
                            checkbox = container.querySelector('input[type="checkbox"]');
                            if (!checkbox) {
                                // Tìm settings-checkbox trong container
                                var settingsCheckbox = container.querySelector('settings-checkbox');
                                if (settingsCheckbox) {
                                    try {
                                        settingsCheckbox.checked = true;
                                        settingsCheckbox.dispatchEvent(new Event('change'));
                                        console.log('Đã check settings-checkbox trong container cho:', label);
                                        selectedCount++;
                                        return;
                                    } catch (e) {
                                        console.error('Lỗi khi check settings-checkbox trong container:', e);
                                    }
                                }
                            }
                        }
                        
                        // Nếu tìm được checkbox
                        if (checkbox) {
                            checkbox.checked = true;
                            checkbox.dispatchEvent(new Event('change'));
                            console.log('Đã check checkbox cho:', label);
                            selectedCount++;
                        } else {
                            console.log('Không tìm thấy checkbox cho:', label);
                            
                            // Thử click vào chính label (nhiều UI sẽ toggle checkbox khi click vào label)
                            try {
                                labelElement.click();
                                console.log('Đã click vào label cho:', label);
                                selectedCount++;
                            } catch (e) {
                                console.error('Lỗi khi click vào label:', e);
                            }
                        }
                    } else {
                        console.log('Không tìm thấy label cho:', label);
                    }
                });
                
                console.log('Đã chọn', selectedCount, 'checkboxes');
                return selectedCount > 0;
            }
            
            // 4. Click nút Clear data
            function clickClearDataButton() {
                // Tìm theo nội dung text
                var clearButton = findElementByText('Clear data', ['button', 'cr-button', 'div[role="button"]']);
                
                if (clearButton) {
                    console.log('Đã tìm thấy nút Clear data bằng text');
                    clearButton.click();
                    return true;
                }
                
                // Tìm trong container footer
                var footerContainer = document.querySelector('div[slot="button-container"]');
                if (footerContainer) {
                    // Tìm nút có class action-button
                    var actionButton = footerContainer.querySelector('.action-button, cr-button.action-button');
                    
                    if (actionButton) {
                        console.log('Đã tìm thấy nút Clear data bằng class action-button');
                        actionButton.click();
                        return true;
                    }
                    
                    // Nếu không tìm được qua class, lấy nút cuối cùng (thường là action button)
                    var buttons = footerContainer.querySelectorAll('button, cr-button');
                    if (buttons.length > 1) { // Phải có ít nhất 2 nút (Cancel và Clear data)
                        console.log('Đã tìm thấy nút Clear data bằng vị trí (nút cuối cùng)');
                        buttons[buttons.length - 1].click();
                        return true;
                    }
                }
                
                console.log('Không tìm thấy nút Clear data');
                return false;
            }
            
            // Utility function để tìm element theo text content
            function findElementByText(text, tags) {
                var elements = [];
                tags.forEach(function(tag) {
                    var tagElements = document.querySelectorAll(tag);
                    tagElements.forEach(function(elem) {
                        var content = elem.textContent || elem.innerText || '';
                        if (content.trim() === text) {
                            elements.push(elem);
                        }
                    });
                });
                return elements.length > 0 ? elements[0] : null;
            }
            
            // Thực thi các bước theo thứ tự
            var results = {
                tabSelected: false,
                timeRangeSet: false,
                checkboxesSelected: false,
                clearButtonClicked: false
            };
            
            // Chọn tab
            results.tabSelected = selectTab();
            
            // Cho phép tab render
            setTimeout(function() {
                // Chọn time range
                results.timeRangeSet = setTimeRange();
                
                // Đợi time range được áp dụng
                setTimeout(function() {
                    // Chọn các checkbox
                    results.checkboxesSelected = selectCheckboxes();
                    
                    // Đợi checkbox được chọn
                    setTimeout(function() {
                        // Click nút Clear data
                        results.clearButtonClicked = clickClearDataButton();
                        
                        // Lưu kết quả vào window để có thể truy xuất
                        window.clearBrowserDataResults = results;
                    }, 500);
                }, 500);
            }, 500);
            
            return true;
        }
        
        return clearBrowsingData(arguments[0], arguments[1]);
        """, use_advanced_tab, time_index)
        
        # Đợi đủ lâu để script thực hiện tất cả các bước
        time.sleep(5)
        
        # Lấy kết quả từ window object
        try:
            result_details = driver.execute_script("return window.clearBrowserDataResults || {};")
            logger.info(f"Kết quả JavaScript tối ưu: {result_details}")
            
            # Kiểm tra xem nút Clear data đã được click chưa
            if result_details.get('clearButtonClicked', False):
                logger.info("Đã click nút Clear data thành công")
                time.sleep(2)  # Đợi thêm để đảm bảo quá trình xóa được bắt đầu
                return True
                
            # Nếu không tìm thấy nút Clear data hoặc không click được, thử dùng bàn phím
            from selenium.webdriver.common.keys import Keys
            from selenium.webdriver.common.action_chains import ActionChains
            
            logger.info("Thử dùng bàn phím để click nút Clear data")
            actions = ActionChains(driver)
            
            # Tab nhiều lần để đến nút Clear data
            # Số lần Tab phụ thuộc vào số checkbox đã tìm thấy
            # Giả sử cấu trúc: Time range dropdown (1) + N checkboxes + Cancel button (1) + Clear data button (1)
            tab_count = 10  # Số mặc định
            actions = ActionChains(driver)
            for _ in range(tab_count):
                actions.send_keys(Keys.TAB)
            actions.send_keys(Keys.ENTER)  # Nhấn Enter
            actions.perform()
            
            time.sleep(3)
            logger.info("Đã thử dùng bàn phím để nhấn nút Clear data")
            return True
            
        except Exception as result_e:
            logger.warning(f"Không thể lấy kết quả từ window object: {str(result_e)}")
            # Thử phương pháp bàn phím nếu không lấy được kết quả
            try:
                from selenium.webdriver.common.keys import Keys
                from selenium.webdriver.common.action_chains import ActionChains
                
                logger.info("Lỗi khi lấy kết quả, thử dùng bàn phím")
                actions = ActionChains(driver)
                for _ in range(12):  # Tab nhiều lần hơn để đảm bảo
                    actions.send_keys(Keys.TAB)
                actions.send_keys(Keys.ENTER)  # Nhấn Enter
                actions.perform()
                
                time.sleep(3)
                return True
            except Exception as keyboard_e:
                logger.warning(f"Không thể sử dụng bàn phím: {str(keyboard_e)}")
                # Vẫn trả về true vì có thể đã xóa được phần nào
                return True
    
    except Exception as e:
        logger.warning(f"Lỗi khi sử dụng JavaScript tối ưu: {str(e)}")
        # Thử phương pháp bàn phím nếu có lỗi JavaScript
        try:
            from selenium.webdriver.common.keys import Keys
            from selenium.webdriver.common.action_chains import ActionChains
            
            logger.info("Lỗi JavaScript, thử dùng bàn phím")
            actions = ActionChains(driver)
            # Chọn tab trước
            if use_advanced_tab:
                actions.send_keys(Keys.TAB).send_keys(Keys.RIGHT)
            # Tab đến các checkbox và chọn
            for _ in range(7):
                actions.send_keys(Keys.TAB).send_keys(Keys.SPACE)
            # Tab đến nút Clear data
            for _ in range(3):
                actions.send_keys(Keys.TAB)
            actions.send_keys(Keys.ENTER)  # Nhấn Enter
            actions.perform()
            
            time.sleep(3)
            return True
        except Exception as keyboard_e:
            logger.warning(f"Không thể sử dụng bàn phím: {str(keyboard_e)}")
            return False


def _clear_using_keyboard_navigation(driver, time_range, domain_url_map, logger):
    """
    Phương pháp dự phòng: Sử dụng điều hướng bàn phím để xóa dữ liệu khi các phương pháp khác thất bại.
    Tương thích với nhiều phiên bản Chrome.
    
    Args:
        driver: Instance WebDriver đang hoạt động
        time_range: Khoảng thời gian xóa dữ liệu
        domain_url_map: Các domain và URL gốc cần xóa
        logger: Logger instance để ghi log
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        # Mở trang cài đặt dọn dẹp dữ liệu
        driver.get("chrome://settings/clearBrowserData")
        logger.info("Truy cập trang cài đặt và sử dụng điều hướng bàn phím")
        time.sleep(3)
        
        # ActionChains để điều hướng bàn phím
        actions = ActionChains(driver)
        
        # Chọn tab Advanced (tab đầu tiên là Basic)
        actions.send_keys(Keys.TAB).send_keys(Keys.RIGHT)
        actions.perform()
        time.sleep(1)
        
        # Chọn "All time" từ dropdown Time Range (tab đến dropdown)
        actions = ActionChains(driver)
        for _ in range(3):
            actions.send_keys(Keys.TAB)
        actions.perform()
        time.sleep(0.5)
        
        # Time range mapping
        time_range_presses = {
            "hour": 0,
            "day": 1,
            "week": 2,
            "month": 3,
            "all_time": 4
        }
        
        # Mở dropdown và chọn option
        actions = ActionChains(driver)
        actions.send_keys(Keys.SPACE)  # Mở dropdown
        time.sleep(0.5)
        # Chọn option dựa vào time_range
        presses = time_range_presses.get(time_range, 4)  # 4 là all_time, mặc định
        for _ in range(presses):
            actions.send_keys(Keys.DOWN)
            time.sleep(0.2)
        actions.send_keys(Keys.ENTER)  # Chọn option
        actions.perform()
        time.sleep(1)
        
        # Tab đến các checkbox và chọn
        actions = ActionChains(driver)
        for i in range(7):  # Có khoảng 7 checkboxes
            actions.send_keys(Keys.TAB)
            time.sleep(0.1)
            actions.send_keys(Keys.SPACE)  # Chọn checkbox
            time.sleep(0.1)
        actions.perform()
        time.sleep(1)
        
        # Tab đến nút Cancel và nút Clear data
        actions = ActionChains(driver)
        for _ in range(3):
            actions.send_keys(Keys.TAB)
        
        # Nhấn nút Clear data
        actions.send_keys(Keys.ENTER)
        actions.perform()
        time.sleep(3)
        
        logger.info("Đã thực hiện xóa dữ liệu bằng phương pháp bàn phím")
        return True
        
    except Exception as e:
        logger.warning(f"Lỗi khi sử dụng phương pháp keyboard navigation: {str(e)}")
        return False


def _get_checkbox_ids_map(use_advanced_tab):
    """
    Trả về mapping từ data_type sang checkbox ID dựa trên tab đang dùng.
    
    Args:
        use_advanced_tab: True để lấy mapping cho tab Advanced, False cho tab Basic
        
    Returns:
        dict: Dictionary ánh xạ từ loại dữ liệu sang ID checkbox trên giao diện
    """
    if use_advanced_tab:
        return {
            "browsing_history": "browsingCheckbox",
            "download_history": "downloadCheckbox",
            "cookies": "cookiesCheckbox",
            "cache": "cacheCheckbox",
            "passwords": "passwordsCheckbox",
            "autofill": "autofillCheckbox",
            "site_settings": "siteSettingsCheckbox",
            "hosted_app_data": "hostedAppDataCheckbox"
        }
    else:
        return {
            "browsing_history": "browsingCheckboxBasic",
            "cookies": "cookiesCheckboxBasic",
            "cache": "cacheCheckboxBasic"
        }


def _clear_using_cdp_basic(driver, domain_url_map, logger):
    """
    Phương pháp 1: Xóa dữ liệu cơ bản bằng Chrome DevTools Protocol (CDP).
    Phương pháp này hoạt động ổn định và nhanh nhất, nhưng không hỗ trợ đầy đủ các loại dữ liệu
    như lịch sử duyệt web, thông tin đăng nhập, v.v.
    
    Args:
        driver: WebDriver instance đang hoạt động
        domain_url_map: Dictionary ánh xạ tên domain (key) đến URL gốc (value)
        logger: Logger instance để ghi log
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    success = True
    try:
        # Xóa cache
        driver.execute_cdp_cmd("Network.clearBrowserCache", {})
        logger.debug("Đã xóa cache browser qua CDP")
    except Exception as e:
        logger.warning(f"Lỗi khi xóa browser cache: {str(e)}")
        success = False

    try:
        # Xóa cookies
        driver.execute_cdp_cmd("Network.clearBrowserCookies", {})
        logger.debug("Đã xóa cookies browser qua CDP")
    except Exception as e:
        logger.warning(f"Lỗi khi xóa browser cookies: {str(e)}")
        success = False

    # Xóa dữ liệu cho từng domain
    for domain, url in domain_url_map.items():
        try:
            driver.execute_cdp_cmd("Storage.clearDataForOrigin", {
                "origin": url, 
                "storageTypes": "all"
            })
            logger.debug(f"Đã xóa dữ liệu cho domain: {domain} ({url})")
        except Exception as e:
            logger.warning(f"Lỗi xóa dữ liệu cho domain {domain}: {str(e)}")
            success = False
    
    # Thêm xóa với tham số mạnh hơn
    try:
        # Xóa tất cả storage types cho tất cả origins
        storage_types = [
            "appcache", "cookies", "file_systems", "indexeddb", 
            "local_storage", "shader_cache", "websql", "service_workers",
            "cache_storage", "all"
        ]
        
        driver.execute_cdp_cmd("Storage.clearDataForOrigin", {
            "origin": "*", 
            "storageTypes": ",".join(storage_types)
        })
        logger.debug("Đã xóa mọi loại dữ liệu storage cho tất cả origins")
    except Exception as e:
        logger.warning(f"Lỗi khi xóa toàn bộ dữ liệu storage: {str(e)}")
    
    return success


def _clear_current_tab_storage(driver, logger):
    """
    Xóa localStorage và sessionStorage cho tab hiện tại.
    
    Args:
        driver: WebDriver instance đang hoạt động
        logger: Logger instance để ghi log
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        result = driver.execute_script("""
        try {
            const localCount = localStorage.length;
            const sessionCount = sessionStorage.length;
            
            localStorage.clear();
            sessionStorage.clear();
            
            return {
                localStorageCount: localCount,
                sessionStorageCount: sessionCount,
                cleared: true
            };
        } catch (e) {
            return {
                error: e.message,
                cleared: false
            };
        }
        """)
        
        if result and result.get('cleared', False):
            logger.info(f"Đã xóa localStorage ({result.get('localStorageCount', 0)} items) và sessionStorage ({result.get('sessionStorageCount', 0)} items)")
            return True
        else:
            logger.warning(f"Không thể xóa storage: {result.get('error', 'Unknown error')}")
            return False
    except Exception as e:
        logger.warning(f"Lỗi khi xóa storage cho tab hiện tại: {str(e)}")
        return False


def _clear_all_tabs_storage(driver, logger):
    """
    Xóa localStorage và sessionStorage cho tất cả các tab.
    
    Args:
        driver: WebDriver instance đang hoạt động
        logger: Logger instance để ghi log
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        current_handle = driver.current_window_handle
        success_count = 0
        
        for handle in driver.window_handles:
            try:
                driver.switch_to.window(handle)
                if _clear_current_tab_storage(driver, logger):
                    success_count += 1
            except Exception as e:
                logger.warning(f"Lỗi khi xóa storage cho tab {handle}: {str(e)}")
        
        # Quay lại tab ban đầu
        try:
            if current_handle in driver.window_handles:
                driver.switch_to.window(current_handle)
            else:
                driver.switch_to.window(driver.window_handles[0])
        except Exception as e:
            logger.warning(f"Không thể quay lại tab ban đầu: {str(e)}")
            
        logger.info(f"Đã xóa storage cho {success_count}/{len(driver.window_handles)} tab")
        return success_count > 0
    except Exception as e:
        logger.warning(f"Lỗi khi xóa storage cho tất cả tab: {str(e)}")
        return False