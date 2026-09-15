#!/usr/bin/env python3
"""cc_sf_merge.py — merge worker JSONL outputs into the local SF cache.

Usage:
  python3 cc_sf_merge.py out1.jsonl out2.jsonl ... [--cache data/cc_repin_sf_sf18.json]

Each input line is {"<fen>|d<depth>": [lines...]}. Only new keys are added (never
overwrites an existing eval), so re-running is safe.
"""
import argparse
import json
import os


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('inputs', nargs='+')
    ap.add_argument('--cache', default=os.path.join('data', 'cc_repin_sf_sf18.json'))
    a = ap.parse_args()
    cache = json.load(open(a.cache)) if os.path.exists(a.cache) else {}
    before = len(cache)
    for p in a.inputs:
        for line in open(p):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            for k, v in obj.items():
                if k not in cache:
                    cache[k] = v
    json.dump(cache, open(a.cache, 'w'))
    print('cache %d -> %d (added %d)' % (before, len(cache), len(cache) - before))


if __name__ == '__main__':
    main()
