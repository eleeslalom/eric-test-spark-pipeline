"""Source-independent extraction, validation, and atomic publication."""
import hashlib
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def safe_id(value):
    if not value or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in value):
        raise ValueError('run-id must contain only letters, numbers, underscores, or hyphens')
    return value


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, indent=2))
    os.replace(tmp, path)


def request_json(url, attempts=4):
    headers = {'User-Agent': 'permitting-learning-project/0.1'}
    if os.getenv('SOCRATA_APP_TOKEN'):
        headers['X-App-Token'] = os.environ['SOCRATA_APP_TOKEN']
    for attempt in range(attempts):
        try:
            with urlopen(Request(url, headers=headers), timeout=60) as response:
                return json.load(response)
        except (HTTPError, URLError, TimeoutError) as exc:
            if isinstance(exc, HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == attempts - 1:
                raise
            time.sleep(min(2 ** attempt, 30))


def pages(config, fetch=request_json):
    """Ordered keyset pagination; duplicate/nonunique keys are rejected downstream."""
    last = None
    size = int(config.get('page_size', 1000))
    key = config['key']
    while True:
        params = {'$limit': size, '$order': key + ' ASC'}
        if last is not None:
            escaped = str(last).replace("'", "''")
            params['$where'] = f"{key} > '{escaped}'"
        batch = fetch(config['endpoint'] + '?' + urlencode(params))
        if not isinstance(batch, list):
            raise ValueError('API response must be a list')
        if not batch:
            break
        yield from batch
        next_key = batch[-1].get(key)
        if next_key is None or next_key == last:
            raise ValueError('Pagination key missing or not advancing')
        last = next_key
        if len(batch) < size:
            break


def extract(config, data_dir, run_id, fixture=None):
    directory = Path(data_dir) / 'raw' / safe_id(run_id)
    if directory.exists():
        metadata = json.loads((directory / 'metadata.json').read_text())
        if metadata['config'] != config or metadata.get('fixture') != (str(Path(fixture).resolve()) if fixture else None):
            raise ValueError('Run ID already belongs to different inputs; choose a new ID')
        return directory
    staging = directory.with_name(directory.name + '.partial')
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    rows = (json.loads(line) for line in Path(fixture).read_text().splitlines() if line.strip()) if fixture else pages(config)
    count = 0
    digest = hashlib.sha256()
    with (staging / 'permits.jsonl').open('w') as out:
        for row in rows:
            line = json.dumps(row, sort_keys=True) + '\n'
            out.write(line)
            digest.update(line.encode())
            count += 1
    if not count:
        raise ValueError('Empty extraction; refusing to replace previous data')
    write_json(staging / 'metadata.json', {'run_id':run_id, 'extracted_at':utcnow(), 'row_count':count, 'sha256':digest.hexdigest(), 'config':config, 'fixture':str(Path(fixture).resolve()) if fixture else None, 'completion_available':bool(config.get('fields', {}).get('completed_date'))})
    directory.parent.mkdir(parents=True, exist_ok=True)
    staging.rename(directory)
    return directory


def validate(config, raw_dir):
    seen = set()
    key = config['fields']['permit_id']
    raw = Path(raw_dir) / 'permits.jsonl'
    with raw.open() as stream:
        for line in stream:
            row = json.loads(line)
            value = row.get(key)
            if value is None or not str(value).strip():
                raise ValueError(f'Missing permit identifier: {key}')
            if str(value) in seen:
                raise ValueError(f'Duplicate permit identifier: {value}; define source grain before processing')
            seen.add(str(value))
    metadata = json.loads((Path(raw_dir) / 'metadata.json').read_text())
    if not seen or len(seen) != metadata['row_count'] or hashlib.sha256(raw.read_bytes()).hexdigest() != metadata['sha256']:
        raise ValueError('Snapshot integrity check failed')
    return len(seen)


def publish(data_dir, run_id):
    root = Path(data_dir).resolve()
    version = root / 'versions' / safe_id(run_id)
    if not (version / 'manifest.json').exists():
        raise ValueError('Cannot publish an incomplete version')
    manifest = json.loads((version / 'manifest.json').read_text())
    for name in ('permits', 'monthly'):
        if not (version / name / '_SUCCESS').exists():
            raise ValueError(f'Missing successful Spark output: {name}')
    if not (version / 'published.json').exists():
        write_json(version / 'published.json', {'published_at':utcnow()})
    write_json(root / 'latest.json', {'run_id':run_id, 'path':str(Path('versions') / run_id), 'published_at':utcnow(), **manifest})
