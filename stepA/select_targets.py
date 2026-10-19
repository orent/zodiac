"""Pick up to N random targets per (sector, camera) from a target table.

The background estimate is limited by noise common to a camera, so beyond
~1000 stamps per camera more stamps add little (stepA/RESULTS.md).

usage: select_targets.py targets.csv out.csv [N] [min_tmag]
"""

import csv, sys
import numpy as np


def main(src, out, n=1000, min_tmag=6.0):
    n, min_tmag = int(n), float(min_tmag)
    rows = [r for r in csv.DictReader(open(src))
            if r['camera'] != '0' and float(r['tmag']) >= min_tmag]
    groups = {}
    for r in rows:
        groups.setdefault((int(r['sector']), int(r['camera'])), []).append(r)
    rng = np.random.default_rng(2026)
    with open(out, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for key in sorted(groups):
            g = groups[key]
            pick = rng.choice(len(g), size=min(n, len(g)), replace=False)
            for i in sorted(pick):
                w.writerow(g[i])
            print(f'sector {key[0]:2d} camera {key[1]}: {min(n, len(g))} of {len(g)}')


if __name__ == '__main__':
    main(*sys.argv[1:])
