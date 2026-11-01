"""Offline mocked checks; no Docker process or filesystem mutation."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parents[1]/'scripts'
SPEC=importlib.util.spec_from_file_location('broker_rehearsal_launcher',HERE/'synthetic-broker-namespace-runner.py')
launcher=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(launcher)


class SyntheticBrokerLauncherReviewTests(unittest.TestCase):
    def test_volume_inspection_parses_only_requested_identity(self):
        with patch.object(launcher.base,'docker',return_value='"synthetic-volume"|"synthetic-label"') as docker:
            self.assertEqual(launcher.volume_info('synthetic-volume'),('synthetic-volume','synthetic-label'))
            self.assertEqual(docker.call_args.args[:2],('volume','inspect'))
            self.assertNotIn('remove',docker.call_args.args)

    def test_container_ownership_rejects_wrong_label_and_published_ports(self):
        ident='a'*64
        with patch.object(launcher.base,'inspect_container',return_value=(ident,'/synthetic','wrong','none',{},'running')):
            with self.assertRaises(RuntimeError):launcher.base.verified_container(ident,'synthetic','expected','none')
        with patch.object(launcher.base,'inspect_container',return_value=(ident,'/synthetic','expected','none',{'30123/tcp':[]},'running')):
            with self.assertRaises(RuntimeError):launcher.base.verified_container(ident,'synthetic','expected','none')

    def test_failed_create_with_mismatched_label_never_stops_or_deletes(self):
        calls=[]
        def fake_docker(*args,**kwargs):
            calls.append(args)
            raise RuntimeError('synthetic create failure')
        with patch.object(launcher.base,'docker',side_effect=fake_docker), \
             patch.object(launcher.base,'inspect_container',return_value=('a'*64,'/synthetic','other','none',{},'created')):
            with self.assertRaises(RuntimeError):
                launcher.base.create_or_reconcile('container','synthetic','expected',['create'])
        self.assertEqual(calls,[('create',)])


if __name__=='__main__':unittest.main()
