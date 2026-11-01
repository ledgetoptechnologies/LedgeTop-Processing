import copy,unittest
from ops.exclusive_owner_preflight import evaluate,CONTAINER,IMAGE,NAME,REVISION,PORTS

class Tests(unittest.TestCase):
    def setUp(self):
        self.container={'Id':CONTAINER,'Name':'/'+NAME,'Image':IMAGE,
          'Config':{'User':'10001:10001','Labels':{},'Env':['TOP_SECRET=synthetic-do-not-print']},
          'State':{'Running':True,'Health':{'Status':'healthy'}},
          'HostConfig':{'PortBindings':{k:[{'HostIp':a,'HostPort':b} for a,b in v] for k,v in PORTS.items()},
                        'RestartPolicy':{'Name':'unless-stopped'},'NetworkMode':'bridge','ReadonlyRootfs':True,
                        'CapDrop':['ALL'],'SecurityOpt':['no-new-privileges']},
          'Mounts':[{'Type':'volume','Name':'ledgetop-processing-prototype-c3fcb3a-state','Destination':'/prototype-state','RW':True},
                    {'Type':'bind','Source':'/run/processing-reference','Destination':'/run/processing-reference','RW':False},
                    {'Type':'bind','Source':'/run/ledgetop-authority-lease','Destination':'/run/ledgetop-authority-lease','RW':True}]}
        self.image={'Id':IMAGE,'Config':{'Labels':{'org.opencontainers.image.revision':REVISION}}}
    def test_expected_snapshot_never_authorizes_cutover_or_leaks_environment(self):
        result=evaluate(self.container,self.image)
        self.assertTrue(result['historical_snapshot_matches'])
        self.assertFalse(result['cutover_authorized'])
        self.assertFalse(result['exclusive_routing_proven'])
        self.assertNotIn('synthetic-do-not-print',str(result))
    def test_added_direct_binding_or_changed_restart_rejected(self):
        for change in ('port','restart'):
            item=copy.deepcopy(self.container)
            if change=='port': item['HostConfig']['PortBindings']['8080/tcp'].append({'HostIp':'0.0.0.0','HostPort':'3005'})
            else: item['HostConfig']['RestartPolicy']['Name']='always'
            self.assertFalse(evaluate(item,self.image)['historical_snapshot_matches'])
    def test_missing_fields_fail_closed(self):
        del self.container['HostConfig']['PortBindings']
        with self.assertRaises(ValueError): evaluate(self.container,self.image)

if __name__=='__main__': unittest.main()
