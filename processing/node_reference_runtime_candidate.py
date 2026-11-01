"""Disabled explicit remote broker runtime pairing for the reference driver."""
from .node_mutation_broker import NodeMutationBroker
from .node_reference_authority_session import RemoteReferenceSessionFactory


class RemoteReferenceRuntime:
    def __init__(self,broker,factory,*,worker_instance_id):
        if type(broker) is not NodeMutationBroker or type(factory) is not RemoteReferenceSessionFactory or broker.witness is not factory:
            raise ValueError('exact remote broker and session factory required')
        if type(worker_instance_id) is not str or not 1<=len(worker_instance_id)<=128:
            raise ValueError('exact worker instance required')
        self.broker,self.factory,self.worker_instance_id=broker,factory,worker_instance_id

    def close(self):self.broker.close()
