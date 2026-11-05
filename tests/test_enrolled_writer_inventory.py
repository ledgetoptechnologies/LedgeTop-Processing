"""Projected full-inventory batching and unchanged stopped-writer policy."""
import json
import unittest

from ops import enrolled_coordinator_transport as t
from ops.enrolled_coordinator_boundary import BoundaryProbe


def row(number=1,*,mounts=None,running=False):
    return dict(Id=f'{number:064x}',Mounts=[] if mounts is None else mounts,
        State=dict(Running=running,Pid=123 if running else 0,FinishedAt='2026-11-05T00:00:00Z'))


class Docker(t.DockerCLI):
    def __init__(self,rows,*,after=None,raw=None):
        self.rows=rows;self.before=[item['Id'] for item in rows]
        self.after=self.before if after is None else after
        self.raw=raw;self.calls=[];self.lists=0
    def command(self,*args,**kwargs):
        self.calls.append((args,kwargs))
        if args[:2]==('container','ls'):
            self.lists+=1;values=self.before if self.lists==1 else self.after
            return ('\n'.join(values)+'\n' if values else '').encode()
        if args[:2]==('container','inspect'):
            if isinstance(self.raw,Exception):raise self.raw
            return self.raw if self.raw is not None else b'\n'.join(json.dumps(value).encode() for value in self.rows)
        raise AssertionError('unexpected non-read command')


class WriterInventoryTests(unittest.TestCase):
    def test_96_containers_use_one_projected_inspect_and_no_config_environment(self):
        docker=Docker([row(i+1) for i in range(96)])
        self.assertEqual(len(docker.writer_inventory()),96)
        self.assertEqual(len(docker.calls),3)
        inspect,options=docker.calls[1]
        self.assertEqual(inspect[:3],('container','inspect','--format'))
        self.assertEqual(list(inspect[5:]),docker.before)
        self.assertIn('{{json .Mounts}}',inspect[3]);self.assertNotIn('Config',inspect[3]);self.assertNotIn('Env',inspect[3])
        self.assertNotIn('dict',inspect[3]);self.assertEqual(options['output_limit'],16*1024**2)
        self.assertTrue(all('mutation' not in options for _,options in docker.calls))

    def test_inventory_change_during_batch_holds(self):
        with self.assertRaisesRegex(t.TransportHeld,'writer_inventory_changed'):
            Docker([row()],after=[row()['Id'],row(2)['Id']]).writer_inventory()

    def test_duplicate_or_invalid_list_identity_refused_without_inspect(self):
        for values in ([row()['Id'],row()['Id']],['short'],['a'*64+' extra']):
            docker=Docker([]);docker.before=values
            with self.subTest(values=values),self.assertRaises(t.TransportHeld):docker.writer_inventory()
            self.assertEqual(len(docker.calls),1)

    def test_duplicate_or_missing_projected_identity_holds(self):
        for rows in ([row(),row()], [row()], [row(),row(3)]):
            docker=Docker([row(),row(2)],raw=b'\n'.join(json.dumps(value).encode() for value in rows))
            with self.subTest(rows=rows),self.assertRaises(t.TransportHeld):docker.writer_inventory()

    def test_invalid_closed_state_and_extra_config_refused(self):
        mutations=(lambda value:value['State'].update(Pid=True),
            lambda value:value['State'].update(Running='false'),
            lambda value:value['State'].update(Pid=-1),
            lambda value:value['State'].update(Pid=123),
            lambda value:value['State'].update(Running=True,Pid=0),
            lambda value:value['State'].update(FinishedAt=None),
            lambda value:value['State'].update(Extra=True),
            lambda value:value.update(Config={'Env':['never accepted']}))
        for mutation in mutations:
            value=row();mutation(value)
            with self.subTest(value=value),self.assertRaises(t.TransportHeld):Docker([value]).writer_inventory()

    def test_oversize_truncated_duplicate_key_and_nonzero_command_fail_closed(self):
        for raw in (b'x'*(16*1024**2+1),b'{',b'{"Id":"x","Id":"x"}',t.TransportHeld('command_read_unavailable')):
            docker=Docker([row()],raw=raw)
            with self.subTest(size=len(raw) if isinstance(raw,bytes) else None),self.assertRaises(t.TransportHeld):
                docker.writer_inventory()
            self.assertEqual(len([args for args,_ in docker.calls if args[:2]==('container','inspect')]),1)

    def test_512_bound_and_untrusted_mount_shape(self):
        self.assertEqual(len(Docker([row(i+1) for i in range(512)]).writer_inventory()),512)
        with self.assertRaises(t.TransportHeld):Docker([row(i+1) for i in range(513)]).writer_inventory()
        for mount in (dict(Type='future',Source='/a',Destination='/b',RW=True),
                      dict(Type='bind',Source='',Destination='/b',RW=True),
                      dict(Type='bind',Source='/a',Destination='/b',RW=1)):
            with self.subTest(mount=mount),self.assertRaises(t.TransportHeld):
                Docker([row(mounts=[mount])]).writer_inventory()

    def test_empty_inventory_still_checks_after_without_unfiltered_inspect(self):
        docker=Docker([]);self.assertEqual(docker.writer_inventory(),[])
        self.assertEqual(len(docker.calls),2)
        with self.assertRaises(t.TransportHeld):Docker([],after=[row()['Id']]).writer_inventory()

    def test_desktop_drive_bind_is_preserved_without_alias_normalization(self):
        for source in ('C:/fixture/data','D:\\fixture\\data'):
            value=row(mounts=[dict(Type='bind',Source=source,Destination='/fixture',RW=False)])
            self.assertEqual(Docker([value]).writer_inventory()[0]['Mounts'][0]['Source'],source)
        for source in ('C:relative','',r'\\server\share','C:/bad\x00name','D:/bad\nname'):
            value=row(mounts=[dict(Type='bind',Source=source,Destination='/fixture',RW=False)])
            with self.subTest(source=source),self.assertRaises(t.TransportHeld):Docker([value]).writer_inventory()
        with self.assertRaises(t.TransportHeld):
            Docker([row(mounts=[dict(Type='volume',Name='state',Source='C:/fixture',Destination='/fixture',RW=True)])]).writer_inventory()


class AliasPolicyTests(unittest.TestCase):
    def test_volume_source_parent_and_child_bind_aliases_remain_closed(self):
        original=row(1,mounts=[dict(Type='volume',Name='owned-state',Source='/volumes/original',Destination='/prototype-state',RW=True)])
        value=object.__new__(BoundaryProbe)
        value.plan={'predecessor':{'containerId':original['Id']}}
        value.bundle={'predecessor':{'stateVolume':'owned-state'}}
        for source,name in (('/elsewhere','owned-state'),('/volumes/original',None),
                            ('/volumes',None),('/volumes/original/child',None),('/',None)):
            alias=row(2,mounts=[dict(Type='bind',Source=source,Destination='/alias',RW=True)],running=True)
            if name is not None:alias['Mounts'][0]['Name']=name
            class BoundaryDocker:
                def inspect(self,identity):return original
                def writer_inventory(self):return [original,alias]
            value.docker=BoundaryDocker()
            with self.subTest(source=source,name=name),self.assertRaisesRegex(t.TransportHeld,'another_original_state_owner'):
                value.writers('a'*64)
            alias['State']=dict(Running=False,Pid=0,FinishedAt='done')
            self.assertEqual(len(value.writers('a'*64)),2)

    def test_unrelated_empty_tmpfs_is_not_a_root_bind_alias(self):
        original=row(1,mounts=[dict(Type='volume',Name='owned-state',Source='/volumes/original',Destination='/prototype-state')])
        unrelated=row(2,mounts=[dict(Type='tmpfs',Source='',Destination='/tmp',RW=True)],running=True)
        value=object.__new__(BoundaryProbe);value.plan={'predecessor':{'containerId':original['Id']}}
        value.bundle={'predecessor':{'stateVolume':'owned-state'}}
        class BoundaryDocker:
            def inspect(self,identity):return original
            def writer_inventory(self):return [original,unrelated]
        value.docker=BoundaryDocker()
        self.assertEqual(len(value.writers('a'*64)),1)
        value.docker.writer_inventory=lambda:[unrelated]
        with self.assertRaisesRegex(t.TransportHeld,'stopped_original_writer_required'):value.writers('a'*64)


if __name__=='__main__':unittest.main()
