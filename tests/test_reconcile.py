import unittest
from pipeline.reconcile import aggregate_completion, reconcile, completion_pages

class ReconciliationTests(unittest.TestCase):
    def test_aggregate_no_fanout_and_preserve_unknown(self):
        rows = [{'PRMT_NR':' A001 ','NEW':2,'DEMO':0,'NET_UNITS':2,'FINAL_DATE':1}, {'PRMT_NR':'A001','NEW':None,'DEMO':1,'NET_UNITS':None,'FINAL_DATE':1}]
        result = aggregate_completion(rows)['A001']
        self.assertEqual(result['source_rows'], 2)
        self.assertIsNone(result['NEW'])
        self.assertEqual(result['DEMO'], 1)
        report = reconcile([{'permit_id':'A001','units_removed':1}], rows)
        self.assertEqual(report['matched_permits'], 1)
        self.assertEqual(report['base_permits'], 1)

    def test_conflicting_dates_not_selected(self):
        result = aggregate_completion([{'PRMT_NR':'X','FINAL_DATE':1}, {'PRMT_NR':'X','FINAL_DATE':2}])['X']
        self.assertTrue(result['final_date_conflict'])
        self.assertIsNone(result['FINAL_DATE'])

    def test_fallback_and_differences(self):
        report = reconcile([{'permit_id':'X','units_added':3}], [{'RECORDNUMBER':'X','NEW':2}])
        self.assertEqual(report['unit_differences'][0]['field'], 'units_added')
        with self.assertRaises(ValueError):
            aggregate_completion([{}])
        with self.assertRaises(ValueError):
            reconcile([{'permit_id':'X'}, {'permit_id':'X'}], [])

    def test_pagination_and_api_error(self):
        responses = iter([{'features':[{'attributes':{'PRMT_NR':'A'}}], 'exceededTransferLimit':True}, {'features':[{'attributes':{'PRMT_NR':'B'}}]}])
        self.assertEqual(len(list(completion_pages(lambda url:next(responses)))),2)
        with self.assertRaises(ValueError):
            list(completion_pages(lambda url:{'error':{'message':'bad'}}))

if __name__ == '__main__':
    unittest.main()
