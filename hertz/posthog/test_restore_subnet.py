import ipaddress
import unittest
from verify_backup_restore import choose_restore_subnet


class RestoreSubnetTest(unittest.TestCase):
    def test_avoids_docker_networks_and_host_routes(self):
        first = choose_restore_subnet([], 'checkpoint')
        blocked = [first, '10.240.0.0/16', '2001:db8::/32']
        result = ipaddress.ip_network(choose_restore_subnet(blocked, 'checkpoint'))
        self.assertTrue(result.is_private)
        self.assertEqual(result.prefixlen, 24)
        for value in blocked:
            network = ipaddress.ip_network(value)
            if network.version == 4: self.assertFalse(result.overlaps(network))

    def test_preserves_existing_networks_when_range_is_unavailable(self):
        with self.assertRaises(RuntimeError): choose_restore_subnet(['10.0.0.0/8'], 'checkpoint')


if __name__ == '__main__': unittest.main()
