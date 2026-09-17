"""离线验证崩溃恢复，不启动浏览器或连接数据库。"""
import unittest
from unittest.mock import Mock
from selenium.common.exceptions import WebDriverException
from gui_xhs import XHSCrawler


class BrowserRecoveryTest(unittest.TestCase):
    """验证重试边界、停止行为及统计保留。"""

    def setUp(self):
        """构造无外部依赖的采集实例。"""
        self.c = XHSCrawler.__new__(XHSCrawler)
        self.c.logger = Mock()
        self.c.account_name = '测试账号'
        self.c.check_stop = Mock()
        self.c.stop_requested = False
        self.c.last_target_stats = {'inserted_urls': 2}
        self.c.total_crawl_stats = {'inserted_urls': 0}
        self.c.all_links = {'old'}
        self.c.collected_quick_data = [{'old': True}]
        self.c.note_card_meta = {'old': True}
        self.c._restart_browser_session = Mock(return_value=True)
        self.row = {'id': 1, 'brand_name': '测试品牌'}

    def test_retry_same_target(self):
        """重建成功后重试原对象，保留统计并清理页面缓存。"""
        callback = Mock()
        self.c._crawl_target_once = Mock(side_effect=[WebDriverException('tab crashed'), True])
        self.assertTrue(self.c.crawl_target(self.row, callback))
        self.assertEqual(self.c._crawl_target_once.call_count, 2)
        self.c._crawl_target_once.assert_called_with(self.row, callback)
        self.c._restart_browser_session.assert_called_once_with()
        self.assertEqual(self.c.total_crawl_stats['inserted_urls'], 2)
        self.assertFalse(self.c.all_links or self.c.collected_quick_data or self.c.note_card_meta)

    def test_restart_failure(self):
        """重建失败立即停止。"""
        self.c._crawl_target_once = Mock(side_effect=WebDriverException('tab crashed'))
        self.c._restart_browser_session.return_value = False
        with self.assertRaises(KeyboardInterrupt):
            self.c.crawl_target(self.row)
        self.assertTrue(self.c.stop_requested)
        self.assertEqual(self.c._crawl_target_once.call_count, 1)

    def test_repeated_crash(self):
        """持续崩溃最多重启一次。"""
        self.c._crawl_target_once = Mock(side_effect=WebDriverException('invalid session id'))
        with self.assertRaises(KeyboardInterrupt):
            self.c.crawl_target(self.row)
        self.assertTrue(self.c.stop_requested)
        self.assertEqual(self.c._crawl_target_once.call_count, 2)
        self.c._restart_browser_session.assert_called_once_with()

    def test_normal_failure(self):
        """普通采集失败不重建浏览器。"""
        self.c._crawl_target_once = Mock(return_value=False)
        self.assertFalse(self.c.crawl_target(self.row))
        self.c._restart_browser_session.assert_not_called()

    def test_user_stop(self):
        """用户停止优先于恢复。"""
        self.c.check_stop.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.c.crawl_target(self.row)
        self.c._restart_browser_session.assert_not_called()

    def test_classification(self):
        """普通超时与业务异常不算浏览器崩溃。"""
        self.assertFalse(XHSCrawler._is_browser_session_crash(WebDriverException('timeout')))
        self.assertFalse(XHSCrawler._is_browser_session_crash(ValueError('tab crashed')))
        self.assertTrue(XHSCrawler._is_browser_session_crash(WebDriverException('disconnected: not connected to DevTools')))

    def test_restart_reuses_profile_when_crashed_cookies_unavailable(self):
        """旧页面不能读取 Cookie 时仍复用原账号目录。"""
        from unittest.mock import patch
        self.c.driver = Mock()
        self.c.driver.get_cookies.side_effect = WebDriverException('tab crashed')
        old_driver = self.c.driver
        new_driver = Mock()
        new_driver.get_cookies.return_value = []
        self.c.account_profile_dir = '/tmp/test-account-profile'
        self.c.headless = False
        self.c._clean_profile_runtime_locks = Mock()
        self.c._start_browser_with_profile = Mock(return_value=new_driver)
        self.c._setup_started_browser = Mock()
        self.c._driver_get = Mock()
        self.c._wait_for_session_ready = Mock(return_value=(True, 'ready'))
        with patch('gui_xhs.DatabaseManager') as database:
            self.assertTrue(XHSCrawler._restart_browser_session(self.c))
            database.assert_not_called()
        old_driver.quit.assert_called_once_with()
        self.c._start_browser_with_profile.assert_called_once_with('/tmp/test-account-profile', headless=False)
        self.assertIs(self.c.driver, new_driver)

    def test_health_check_rejects_swallowed_crash(self):
        """滚动逻辑吞掉异常后，完成检查仍阻止虚假的成功。"""
        from unittest.mock import patch
        from gui_xhs import TargetType
        self.c.target_type = TargetType.BRAND
        self.c.max_scroll_default = 1
        self.c._driver_get = Mock()
        self.c._detect_and_handle_captcha = Mock()
        self.c.smart_scroll = Mock()
        self.c.driver = Mock()
        self.c.driver.execute_script.side_effect = WebDriverException('tab crashed')
        with patch('gui_xhs.get_rednote_urls', return_value=['https://www.xiaohongshu.com/user/profile/test']):
            with self.assertRaises(WebDriverException):
                self.c._crawl_target_once(self.row)
