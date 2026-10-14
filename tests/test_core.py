import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from pipeline.core import extract, validate, publish, pages, safe_id, request_json

CONFIG = json.loads(Path('configs/seattle.json').read_text())

class CoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
    def tearDown(self): self.temp.cleanup()
    def test_pagination_and_quote_escaping(self):
        config = {**CONFIG, 'page_size': 2}
        urls=[]
        batches=iter([[{'permitnum':'A'},{'permitnum':"B'C"}],[{'permitnum':'D'}]])
        def fetch(url): urls.append(url); return next(batches)
        self.assertEqual(len(list(pages(config,fetch))),3)
        self.assertIn('B%27%27C',urls[1])
    def test_pagination_must_advance(self):
        with self.assertRaises(ValueError): list(pages({**CONFIG,'page_size':1},lambda _: [{'permitnum':'A'}]))
    def test_snapshot_retry_and_integrity(self):
        raw=extract(CONFIG,self.root,'test','fixtures/seattle.jsonl')
        self.assertEqual(validate(CONFIG,raw),5)
        self.assertEqual(extract(CONFIG,self.root,'test','fixtures/seattle.jsonl'),raw)
        with (raw/'permits.jsonl').open('a') as out: out.write('{}\n')
        with self.assertRaises(ValueError): validate(CONFIG,raw)
    def test_duplicate_keys_rejected(self):
        fixture=self.root/'duplicates.jsonl'
        fixture.write_text('{"permitnum":"A"}\n{"permitnum":"A"}\n')
        raw=extract(CONFIG,self.root,'duplicate',fixture)
        with self.assertRaisesRegex(ValueError,'Duplicate'): validate(CONFIG,raw)
    def test_publish_failure_preserves_pointer(self):
        (self.root/'latest.json').write_text('{"run_id":"previous"}')
        with self.assertRaises(ValueError): publish(self.root,'missing')
        self.assertEqual(json.loads((self.root/'latest.json').read_text())['run_id'],'previous')
    def test_snapshot_revision_gets_new_run(self):
        raw=extract(CONFIG,self.root,'old','fixtures/seattle.jsonl')
        fixture=self.root/'revised.jsonl'
        fixture.write_text(Path('fixtures/seattle.jsonl').read_text().replace('1000000','2000000'))
        new=extract(CONFIG,self.root,'new',fixture)
        self.assertNotEqual((raw/'metadata.json').read_text(),(new/'metadata.json').read_text())
        with self.assertRaises(ValueError): extract(CONFIG,self.root,'old',fixture)
    def test_rate_limit_retry(self):
        with patch('pipeline.core.urlopen',side_effect=HTTPError('url',429,'limited',{},None)) as fetch, patch('pipeline.core.time.sleep'):
            with self.assertRaises(HTTPError): request_json('https://example.test',attempts=2)
            self.assertEqual(fetch.call_count,2)
    def test_safe_paths(self):
        for value in ('../oops','', 'bad/name'):
            with self.assertRaises(ValueError): safe_id(value)

if __name__=='__main__': unittest.main()
