import unittest

from ops.netns_sockets import summarize

SAMPLE = '''  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000 10001 0 1 1
   1: 00000000:20FB 00000000:0000 0A 00000000:00000000 00:00000000 00000000 10001 0 2 1
   2: 0200000A:1F90 0300000A:D431 01 00000000:00000000 00:00000000 00000000 10001 0 3 1
   3: 0200000A:1F90 0400000A:D432 06 00000000:00000000 00:00000000 00000000     0 0 0 1
   4: 0200000A:C350 0500000A:0BB8 01 00000000:00000000 00:00000000 00000000 10001 0 5 1'''


class NetnsSocketsTest(unittest.TestCase):
    def test_counts_inbound_per_listener_without_addresses(self):
        result = summarize(SAMPLE.splitlines())
        self.assertEqual(result['listening'], [8080, 8443])
        self.assertEqual(result['inboundByLocalPort'], {'8080': {'established': 1, 'time_wait': 1}})
        self.assertEqual(result['otherConnections'], 1)
        self.assertNotIn('0A00', repr(result))

    def test_idle(self):
        lines = SAMPLE.splitlines()[:3]
        self.assertEqual(summarize(lines)['inboundByLocalPort'], {})


if __name__ == '__main__':
    unittest.main()
