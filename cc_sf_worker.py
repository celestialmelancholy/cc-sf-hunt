#!/usr/bin/env python3
"""cc_sf_worker.py — standalone Stockfish batch evaluator for the Chess.com hunt.

Runs on ANY Linux box (ARM or x86). Reads a plan of FENs, evaluates each at a given
depth with MultiPV=2, and writes JSONL in the exact cache format used by cc_repin:

    {"<fen>|d<depth>": [{"kind":"cp","val":<cp>,"uci":"<move>"}, ...], ...}

No dependencies beyond a Stockfish binary. Parallelises with N worker processes
(one Stockfish child each) — use `--workers = number of CPU cores`.

Usage:
  python3 cc_sf_worker.py --plan plan.json --out out.jsonl --sf ./stockfish \\
      --depth 18 --workers 4 [--skip cache.json] [--hash 256]

Then copy out.jsonl back and merge with tools/cc_sf_merge.py.
"""
import argparse
import json
import multiprocessing as mp
import os
import re
import subprocess
import sys
import time


def _spawn(path):
    p = subprocess.Popen([path], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, text=True, bufsize=1)
    def send(c):
        p.stdin.write(c + '\n'); p.stdin.flush()
    def wait(tok, ms=120000):
        end = time.time() + ms / 1000.0
        while time.time() < end:
            line = p.stdout.readline()
            if not line:
                raise RuntimeError('engine died')
            if tok in line:
                return line
        raise TimeoutError(tok)
    send('uci'); wait('uciok')
    return p, send, wait


def evaluate(fen, depth, path, hash_mb):
    p, send, wait = _spawn(path)
    try:
        send('setoption name Threads value 1')
        send('setoption name Hash value %d' % hash_mb)
        send('setoption name MultiPV value 2')
        send('isready'); wait('readyok')
        send('position fen ' + fen)
        send('go depth %d' % depth)
        best = None
        lines = []
        while True:
            line = p.stdout.readline()
            if not line:
                break
            line = line.strip()
            if line.startswith('bestmove'):
                best = line.split()[1] if len(line.split()) > 1 else None
                break
            if line.startswith('info') and ' score ' in line and ' pv ' in line:
                lines.append(line)
    finally:
        try:
            p.stdin.write('quit\n'); p.stdin.flush()
        except Exception:
            pass
        p.kill()
    byrank = {}
    for l in lines:
        md = re.search(r'multipv (\d+)', l)
        rank = int(md.group(1)) if md else 1
        sc = re.search(r'score (cp|mate) (-?\d+)', l)
        dp = re.search(r'depth (\d+)', l)
        pv = l.split(' pv ', 1)[1].strip() if ' pv ' in l else ''
        if not sc or not pv:
            continue
        d = int(dp.group(1)) if dp else 0
        if rank not in byrank or d >= byrank[rank][0]:
            byrank[rank] = (d, {'kind': sc.group(1), 'val': int(sc.group(2)),
                                'uci': pv.split()[0]})
    return fen, [byrank[k][1] for k in sorted(byrank)]


def _worker(args):
    slice_, depth, path, hash_mb = args
    out = []
    for i, fen in enumerate(slice_):
        try:
            fen, lines = evaluate(fen, depth, path, hash_mb)
            out.append((fen, lines))
        except Exception as e:
            out.append((fen, None))
        if (i + 1) % 25 == 0:
            print('  ... %d/%d' % (i + 1, len(slice_)), flush=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--plan', required=True, help='JSON list of FENs')
    ap.add_argument('--out', required=True, help='output JSONL')
    ap.add_argument('--sf', required=True, help='path to Stockfish binary')
    ap.add_argument('--depth', type=int, default=18)
    ap.add_argument('--workers', type=int, default=os.cpu_count() or 1)
    ap.add_argument('--hash', type=int, default=256)
    ap.add_argument('--skip', default=None, help='existing cache json/ jsonl to skip (keys fen|dD)')
    ap.add_argument('--shard', type=int, default=0, help='shard index (for parallel runners)')
    ap.add_argument('--shards', type=int, default=1, help='total shards')
    a = ap.parse_args()

    plan = json.load(open(a.plan))
    if isinstance(plan, dict):
        plan = list(plan)
    if a.shards > 1:
        plan = plan[a.shard::a.shards]
    done = set()
    if a.skip and os.path.exists(a.skip):
        if a.skip.endswith('.jsonl'):
            for line in open(a.skip):
                line = line.strip()
                if line:
                    try:
                        done.add(json.loads(line).split('\t', 1)[0])
                    except Exception:
                        pass
        else:
            done = set(json.load(open(a.skip)))
    todo = [f for f in plan if ('%s|d%d' % (f, a.depth)) not in done]
    print('plan=%d  done=%d  todo=%d  workers=%d  depth=%d' % (len(plan), len(done), len(todo), a.workers, a.depth))

    t0 = time.time()
    n = len(todo)
    chunks = [todo[i::a.workers] for i in range(a.workers)]
    with open(a.out, 'w') as fh:
        with mp.Pool(a.workers) as pool:
            for k, res in enumerate(pool.imap_unordered(_worker, [(c, a.depth, a.sf, a.hash) for c in chunks])):
                for fen, lines in res:
                    if lines is None:
                        continue
                    fh.write(json.dumps({'%s|d%d' % (fen, a.depth): lines}) + '\n')
                fh.flush()
                print('  chunk %d/%d done (%.1fs)' % (k + 1, a.workers, time.time() - t0), flush=True)
    print('DONE %d positions in %.1fs (%.2f s/pos)' % (n, time.time() - t0, (time.time() - t0) / max(1, n)))


if __name__ == '__main__':
    main()
