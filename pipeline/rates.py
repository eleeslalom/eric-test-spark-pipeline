"""Optional FRED mortgage-rate enrichment before publication."""
import csv
import io
import json
import os
from collections import defaultdict
from pathlib import Path
from urllib.request import urlopen
from pipeline.core import write_json, utcnow


def add_rates(root, run_id, fixture=None):
    import duckdb
    version = Path(root) / 'versions' / run_id
    if not (version / 'manifest.json').exists():
        raise ValueError('Transform permits before adding rates')
    latest = Path(root) / 'latest.json'
    if (version / 'published.json').exists() or (latest.exists() and json.loads(latest.read_text())['run_id'] == run_id):
        if (version / 'rates.parquet').exists():
            return
        raise ValueError('Enrich a new unpublished version to keep dashboard reads stable')
    text = Path(fixture).read_text() if fixture else urlopen('https://fred.stlouisfed.org/graph/fredgraph.csv?id=MORTGAGE30US&cosd=2010-01-01',timeout=60).read().decode()
    groups = defaultdict(list)
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        date = row.get('DATE') or row.get('observation_date')
        value = row.get('MORTGAGE30US')
        if date and date >= '2010-01-01' and value not in (None,'','.'): groups[date[:7]+'-01'].append(float(value))
    if not groups:
        raise ValueError('No mortgage observations returned')
    database = duckdb.connect()
    database.execute('CREATE TABLE rates(month DATE, mortgage_rate DOUBLE)')
    database.executemany('INSERT INTO rates VALUES (?,?)',[(month,sum(values)/len(values)) for month,values in sorted(groups.items())])
    output = version / 'rates.parquet'
    temporary = version / 'rates.partial.parquet'
    database.execute("COPY rates TO '" + str(temporary).replace("'", "''") + "' (FORMAT PARQUET)")
    database.close()
    os.replace(temporary,output)
    manifest=json.loads((version/'manifest.json').read_text())
    manifest['rates']={'source':'FRED MORTGAGE30US','retrieved_at':utcnow(),'fixture':bool(fixture)}
    write_json(version/'manifest.json',manifest)
