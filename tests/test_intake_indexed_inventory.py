import time
import unittest
from processing.intake_indexed_inventory import compare, InventoryHeld

class InventoryTests(unittest.TestCase):
    def test_ten_thousand_unique_rows_and_lazy_listing(self):
        def metadata():
            for n in range(10000):yield ('photo-%05d.jpg'%n,'input-'+format(n,'032x'))
        def entries():
            for n in reversed(range(10000)):yield ('input-'+format(n,'032x'),True)
        start=time.monotonic();result=compare(metadata(),entries());elapsed=time.monotonic()-start
        self.assertEqual(result,{'files':10000,'entries':10000})
        print('synthetic indexed 10000 inventory seconds:',round(elapsed,4),flush=True)
    def test_duplicate_missing_unexpected_and_nonregular_are_held(self):
        name='input-'+'a'*32
        scenarios=[([('a',name),('a',name)],[(name,True)]),([('a',name)],[]),
                   ([],[(name,True)]),([('a',name)],[(name,False)]),
                   ([('a',name)],[(name,True),(name,True)]),([],[(".incoming-x",True)])]
        for metadata,entries in scenarios:
            with self.subTest(metadata=metadata,entries=entries):
                with self.assertRaises(InventoryHeld):compare(iter(metadata),iter(entries))
    def test_limits_fail_closed(self):
        with self.assertRaises(InventoryHeld):compare([('a','input-'+'a'*32),('b','input-'+'b'*32)],[],max_files=1)
        with self.assertRaises(InventoryHeld):compare([], [('other',True),('other2',True)],max_entries=1)

if __name__=='__main__':unittest.main()
