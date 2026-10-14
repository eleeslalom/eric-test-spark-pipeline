"""Supplemental residential-source reconciliation, never a multiplicative join."""
import json
from pathlib import Path
from urllib.parse import urlencode
from pipeline.core import request_json, safe_id, utcnow, write_json

ENDPOINT = 'https://services.arcgis.com/ZOyb2t4B0UYuYNYH/arcgis/rest/services/Residential_Building_Permits_Issued_and_Final/FeatureServer/0/query'


def aggregate_completion(rows):
    grouped = {}
    for item in rows:
        row = item.get('attributes', item)
        key = str(row.get('PRMT_NR') or row.get('RECORDNUMBER') or '').strip()
        if not key:
            raise ValueError('Supplemental record lacks permit identifier')
        grouped.setdefault(key, []).append(row)
    result = {}
    for key, records in grouped.items():
        aggregate = {'permit_id': key, 'source_rows': len(records)}
        for field in ('NEW', 'DEMO', 'NET_UNITS'):
            values = [r.get(field) for r in records]
            # An unknown component makes the aggregate unknown rather than a partial sum.
            aggregate[field] = None if any(v is None or v == '' for v in values) else sum(float(v) for v in values)
        dates = sorted({str(r['FINAL_DATE']) for r in records if r.get('FINAL_DATE') not in (None, '')})
        aggregate['final_date_conflict'] = len(dates) > 1
        aggregate['FINAL_DATE'] = dates[0] if len(dates) == 1 else None
        aggregate['observed_final_dates'] = dates
        result[key] = aggregate
    return result


def reconcile(base_rows, supplemental_rows):
    supplemental = aggregate_completion(supplemental_rows)
    base = {}
    for row in base_rows:
        key = str(row['permit_id']).strip()
        if key in base:
            raise ValueError('Base permit identifiers must be unique within Seattle source')
        base[key] = row
    matched = sorted(base.keys() & supplemental.keys())
    differences = []
    for key in matched:
        for field, extra in (('units_added','NEW'), ('units_removed','DEMO'), ('net_units','NET_UNITS')):
            left, right = base[key].get(field), supplemental[key][extra]
            if left is not None and right is not None and float(left) != right:
                differences.append({'permit_id':key, 'field':field, 'base':float(left), 'supplemental':right})
    return {'base_permits':len(base), 'supplemental_permits':len(supplemental), 'matched_permits':len(matched),
            'base_only_permits':len(base.keys()-supplemental.keys()), 'supplemental_only_permits':len(supplemental.keys()-base.keys()),
            'final_date_conflicts':[k for k,v in supplemental.items() if v['final_date_conflict']],
            'unit_differences':differences, 'supplemental_aggregates':list(supplemental.values()),
            'policy':'Diagnostic only; supplemental measures never added to base permit totals.'}


def completion_pages(fetch=request_json, page_size=1000):
    offset = 0
    while True:
        params = {'f':'json','where':'1=1','outFields':'OBJECTID,PRMT_NR,RECORDNUMBER,NEW,DEMO,NET_UNITS,FINAL_DATE',
                  'orderByFields':'OBJECTID ASC','resultOffset':offset,'resultRecordCount':page_size}
        response = fetch(ENDPOINT + '?' + urlencode(params))
        if 'error' in response:
            raise ValueError(f"ArcGIS error: {response['error']}")
        rows = response.get('features')
        if not isinstance(rows, list):
            raise ValueError('ArcGIS response lacks features')
        yield from rows
        if not response.get('exceededTransferLimit', False):
            break
        if not rows:
            raise ValueError('ArcGIS pagination did not advance')
        offset += len(rows)


def run_reconciliation(root, run_id, fixture=None):
    import duckdb
    root = Path(root).resolve()
    run_id = safe_id(run_id)
    latest = root / 'latest.json'
    if (root / 'versions' / run_id / 'published.json').exists() or (latest.exists() and json.loads(latest.read_text()).get('run_id') == run_id):
        if (root / 'versions' / run_id / 'reconciliation.json').exists():
            return json.loads((root / 'versions' / run_id / 'reconciliation.json').read_text())
        raise ValueError('Cannot modify a published run; choose a new run ID')
    version = root / 'versions' / run_id
    manifest = json.loads((version / 'manifest.json').read_text())
    if manifest['config']['jurisdiction'] != 'us-wa-seattle':
        raise ValueError('Completion reconciliation is Seattle-specific')
    path = root / 'raw' / run_id / 'completion.jsonl'
    if path.exists():
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    else:
        rows = [json.loads(line) for line in Path(fixture).read_text().splitlines() if line.strip()] if fixture else list(completion_pages())
        if not rows:
            raise ValueError('Empty supplemental extraction')
        temporary = path.with_suffix('.jsonl.partial')
        temporary.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        temporary.replace(path)
        write_json(path.with_suffix('.metadata.json'), {'endpoint':ENDPOINT,'extracted_at':utcnow(),'row_count':len(rows),'fixture':str(fixture) if fixture else None})
    connection = duckdb.connect()
    try:
        cursor = connection.execute('SELECT * FROM read_parquet(?)', [str(version / 'permits' / '*.parquet')])
        names = [col[0] for col in cursor.description]
        base = [dict(zip(names,row)) for row in cursor.fetchall()]
    finally:
        connection.close()
    report = reconcile(base, rows)
    write_json(version / 'reconciliation.json', report)
    return report
