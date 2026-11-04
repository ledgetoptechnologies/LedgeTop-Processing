import copy
import unittest

from tests.pilot_container_guard import stopped_pilot


class PilotContainerGuardTests(unittest.TestCase):
    def setUp(self):
        self.expected = {'id': 'a'*64, 'name': 'ltds-pilot-fake-'+'b'*32,
                         'image': 'sha256:'+'c'*64, 'owner': 'b'*32, 'mountSource': '/owned'}
        self.item = {'Id': self.expected['id'], 'Name': '/'+self.expected['name'],
                     'Image': self.expected['image'],
                     'Config': {'Labels': {'ltds.fixture.owner': self.expected['owner']}},
                     'State': {'Running': False, 'Restarting': False},
                     'Mounts': [{'Destination': '/pilot', 'Source': '/owned', 'Type': 'bind', 'RW': True}]}

    def test_exact_stopped_container_reports_limits(self):
        self.assertEqual(stopped_pilot(self.item, self.expected),
                         {'containerStopped': True, 'otherWritersNotVerified': True,
                          'restartNotPrevented': True, 'databaseCheckpointNotVerified': True})

    def test_running_restarting_or_changed_identity_and_mount_refused(self):
        for section, field, value in [('State', 'Running', True), ('State', 'Restarting', True),
                                       ('Config', 'Labels', {}), ('', 'Id', 'd'*64),
                                       ('', 'Image', 'sha256:'+'d'*64)]:
            with self.subTest(field=field):
                item=copy.deepcopy(self.item)
                (item[section] if section else item)[field]=value
                with self.assertRaises(ValueError): stopped_pilot(item, self.expected)
        for field,value in [('Source','/other'),('RW',False),('Type','volume')]:
            item=copy.deepcopy(self.item); item['Mounts'][0][field]=value
            with self.assertRaises(ValueError): stopped_pilot(item,self.expected)
