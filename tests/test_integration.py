"""End-to-end checks; run with Java 17 and installed project dependencies."""
import json
import os
import tempfile
import unittest
from pathlib import Path

@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS')=='1','Set RUN_SPARK_TESTS=1 to run Spark integration')
class IntegrationTests(unittest.TestCase):
    def test_pipeline_rerun_rates_and_second_jurisdiction(self):
        import duckdb
        from pipeline.cli import main
        with tempfile.TemporaryDirectory() as tmp:
            common=['--data-dir',tmp,'--run-id','first','--fixture','fixtures/seattle.jsonl']
            self.assertEqual(main(['run',*common,'--rates-fixture','fixtures/rates.csv','--completion-fixture','fixtures/completion.jsonl']),0)
            version=Path(tmp)/'versions'/'first'
            with duckdb.connect() as db:
                permits=db.read_parquet(str(version/'permits'/'*.parquet')).df()
                monthly=db.read_parquet(str(version/'monthly'/'*.parquet')).df()
                self.assertEqual(len(permits),5)
                self.assertEqual(monthly.loc[monthly.event=='completed','permit_count'].sum(),1)
                self.assertEqual(permits.loc[permits.permit_id=='A001','net_units'].iloc[0],3)
                self.assertEqual(monthly.loc[(monthly.event=='issued') & (monthly.building_type=='residential'),'unknown_units'].sum(),1)
                self.assertEqual(db.read_parquet(str(version/'rates.parquet')).count('*').fetchone()[0],5)
            self.assertTrue((version/'reconciliation.json').exists())
            self.assertEqual(main(['run',*common,'--rates-fixture','fixtures/rates.csv','--completion-fixture','fixtures/completion.jsonl']),0)
            self.assertEqual(json.loads((Path(tmp)/'latest.json').read_text())['run_id'],'first')
            self.assertEqual(main(['run','--data-dir',tmp,'--run-id','second','--config','configs/second_city.json','--fixture','fixtures/second_city.jsonl']),0)
            with duckdb.connect() as db:
                rows=db.read_parquet([str(version/'permits'/'*.parquet'),str(Path(tmp)/'versions/second/permits/*.parquet')],union_by_name=True).df()
                self.assertEqual(rows.loc[rows.permit_id=='A001','jurisdiction'].nunique(),2)
            pointer=(Path(tmp)/'latest.json').read_text()
            self.assertNotEqual(main(['publish','--data-dir',tmp,'--run-id','missing']),0)
            self.assertEqual(pointer,(Path(tmp)/'latest.json').read_text())
