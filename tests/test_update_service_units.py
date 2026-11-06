import unittest
from ops.update_service_units import render_units,NAME


class UserUnitTests(unittest.TestCase):
    def test_units_invoke_one_tick_and_main_requires_existing_tunnel(self):
        units=render_units('/home/bkoltz/source','/home/bkoltz/config.json',node_id='main')
        service=units[NAME+'.service'];timer=units[NAME+'.timer']
        self.assertIn('Type=oneshot\n',service)
        self.assertIn('Requires=ledgetop-processing-tunnel.service\n',service)
        self.assertIn(' -m ops.update_service tick --config /home/bkoltz/config.json\n',service)
        self.assertIn('Restart=no\n',service)
        self.assertIn('OnUnitInactiveSec=1min\n',timer)
        for forbidden in ('CPUQuota=','MemoryMax=','TasksMax=','ExecStop=','sudo','reboot','prune'):
            self.assertNotIn(forbidden,service)

    def test_cluster_does_not_require_remote_coordinator_tunnel(self):
        units=render_units('/home/bkoltz/source','/home/bkoltz/config.json',node_id='cluster')
        self.assertNotIn('tunnel.service',units[NAME+'.service'])

    def test_future_node_uses_explicit_interpreter_and_tunnel_dependency(self):
        units=render_units('/opt/processing/source','/home/operator/update.json',node_id='gpu-third',
            python_executable='/opt/processing/venv/bin/python',requires_tunnel=True)
        service=units[NAME+'.service']
        self.assertIn('ExecStart=/opt/processing/venv/bin/python -B -m ops.update_service tick',service)
        self.assertIn('Requires=ledgetop-processing-tunnel.service\n',service)
        self.assertNotIn('ExecStart=/usr/bin/python3',service)
        for path in ('/opt/a%h/python','/opt/a b/python','/opt/../bin/python'):
            with self.subTest(path=path),self.assertRaises(ValueError):
                render_units('/opt/processing/source','/home/operator/update.json',node_id='gpu-third',
                    python_executable=path,requires_tunnel=False)

    def test_paths_cannot_inject_unit_directives_or_specifiers(self):
        for path in ('relative','/home/bkoltz/a b','/home/bkoltz/a%h','/home/bkoltz/a\nExecStart=bad',
                     '/home/bkoltz/../secret','/home//bkoltz/config'):
            with self.subTest(path=path),self.assertRaises(ValueError):
                render_units(path,'/home/bkoltz/config.json',node_id='main')
        with self.assertRaises(ValueError):render_units('/home/bkoltz/source','/home/bkoltz/config.json',node_id='coordinator')


if __name__=='__main__':unittest.main()
