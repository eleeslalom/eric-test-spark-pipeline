"""Run standalone extraction, validation, transformation, and publication commands."""
import argparse
import json
import sys
from pathlib import Path
from pipeline.core import extract, validate, publish, safe_id


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['extract','validate','transform','publish','run','rates','reconcile'])
    parser.add_argument('--config', default='configs/seattle.json')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--data-dir', default='data')
    parser.add_argument('--fixture')
    parser.add_argument('--rates-fixture')
    parser.add_argument('--with-rates',action='store_true')
    parser.add_argument('--reconcile',action='store_true')
    parser.add_argument('--completion-fixture')
    args = parser.parse_args(argv)
    try:
        safe_id(args.run_id)
        config = json.loads(Path(args.config).read_text())
        root = Path(args.data_dir)
        raw = root / 'raw' / args.run_id
        if args.command in ('extract','run'):
            extract(config, root, args.run_id, args.fixture)
        if args.command in ('validate','run'):
            validate(config, raw)
        if args.command in ('transform','run'):
            from pipeline.spark_job import transform
            validate(config, raw)
            transform(config, root, args.run_id)
        if args.command == 'rates' or (args.command == 'run' and (args.rates_fixture or args.with_rates)):
            from pipeline.rates import add_rates
            add_rates(root, args.run_id, args.rates_fixture)
        if args.command == 'reconcile' or (args.command == 'run' and (args.reconcile or args.completion_fixture)):
            from pipeline.reconcile import run_reconciliation
            run_reconciliation(root, args.run_id, args.completion_fixture)
        if args.command in ('publish','run'):
            publish(root, args.run_id)
        print(json.dumps({'command':args.command,'run_id':args.run_id,'ok':True}))
        return 0
    except Exception as exc:
        print(f'{type(exc).__name__}: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
