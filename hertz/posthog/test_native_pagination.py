"""Pagination from a TLS proxy never changes the authenticated destination."""
import unittest
from recreate_analytics_cohorts import native_page_path


class NativePaginationTest(unittest.TestCase):
    def test_proxy_generated_http_page_stays_on_configured_https_origin(self):
        path = native_page_path('https://posthog.kasanova.io',
                                'http://posthog.kasanova.io/api/projects/1/persons/?limit=100&offset=100',
                                '/api/projects/1/persons/')
        self.assertEqual('https://posthog.kasanova.io' + path,
                         'https://posthog.kasanova.io/api/projects/1/persons/?limit=100&offset=100')

    def test_https_and_explicit_configured_port_are_retained(self):
        self.assertEqual(native_page_path('https://example.test:8443',
                         'https://example.test:8443/api/projects/2/persons/?cursor=x%2Fy',
                         '/api/projects/2/persons/'), '/api/projects/2/persons/?cursor=x%2Fy')
        self.assertIsNone(native_page_path('https://example.test', None, '/api/projects/2/persons/'))

    def test_rejects_other_origin_credentials_project_resource_fragment_and_port(self):
        urls = ['https://attacker.test/api/projects/1/persons/',
                'https://posthog.kasanova.io.attacker.test/api/projects/1/persons/',
                'https://user:password@posthog.kasanova.io/api/projects/1/persons/',
                'https://posthog.kasanova.io:8443/api/projects/1/persons/',
                'https://posthog.kasanova.io/api/projects/2/persons/',
                'https://posthog.kasanova.io/api/projects/1/feature_flags/',
                'https://posthog.kasanova.io/api/projects/1/persons/#different',
                'file://posthog.kasanova.io/api/projects/1/persons/',
                '//posthog.kasanova.io/api/projects/1/persons/',
                '/api/projects/1/persons/']
        for url in urls:
            with self.subTest(url=url), self.assertRaises(RuntimeError):
                native_page_path('https://posthog.kasanova.io', url, '/api/projects/1/persons/')


if __name__ == '__main__':
    unittest.main()
