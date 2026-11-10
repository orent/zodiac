"""Per-CCD north/south test: is the year-2 (northern) excess in A an
Earthshine (stray-light) effect?

Earthshine reaches the detector as scattered light whose strength varies
strongly across a camera's four CCDs; the zodiacal echo depends only on
the sky direction and is nearly the same for all four. For each CCD and
each group of sectors this fits, at alpha = 1.4:
  A, E free;  A with E fixed at the CCD's pooled value;  the floor.
If the northern excess is Earthshine, it follows the CCDs whose E or
floor changes most between hemispheres; if it is uniform across CCDs,
Earthshine is not the cause.

usage: perccd.py datadir [cameras]
"""

import sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import joint

ALPHA = 1.4
NBOOT = 100
GROUPS = [
    ('year 1 S1-13 (S)', '1-13_2min'),
    ('same season S4-10 (S)', '4-10_2min'),
    ('year 2 S17-23 (N)', '17-23_2min'),
    ('year 3 S27-38 (S) 2-min', '27-38_2min'),
    ('year 3 S27-38 (S) 20-s', '27-38'),
]
POOL = '1-13_2min,17-23_2min,27-38_2min,27-38'


def amps(t, E_fixed, wk):
    chi, AE = t.solve(wk)
    A, E = AE[0, 0]
    m11 = np.sum(wk * t.P[ALPHA])
    m12 = ((wk * t.eps) @ t.Q[ALPHA])[0]
    r1 = (wk @ t.U[ALPHA])[0]
    return A, E, (r1 - E_fixed * m12) / m11


def main(datadir, cameras='1,2,3,4'):
    joint.DTS = np.array([0.0])
    rng = np.random.default_rng(31)
    print(f'per-CCD north/south test at alpha {ALPHA}: A, E free | A with E fixed (pooled per CCD) | floor e-/s/px')
    for cam in (int(c) for c in cameras.split(',')):
        print(f'\ncamera {cam}')
        print(f'  {"ccd":3s} {"group":24s} {"A (E free)":>13s} {"E":>13s} {"A (E fixed)":>13s} {"floor":>6s} {"n":>3s}')
        for ccd in (1, 2, 3, 4):
            unit = f'cam{cam}_ccd{ccd}'
            chunks = {}
            for sec, prod in joint.parse_sectors(POOL):
                try:
                    chunks[(sec, prod)] = joint.Chunk(datadir, sec, prod, unit, 'red')
                except (FileNotFoundError, KeyError, ValueError, IndexError):
                    continue

            def terms(spec):
                cs = [chunks[k] for k in joint.parse_sectors(spec) if k in chunks]
                return (joint.Terms([(c.f, c.cadence, c.X, c.Y, c.eps, c.u) for c in cs], None,
                                    alphas=[ALPHA]), cs) if cs else (None, cs)

            tp, _ = terms(POOL)
            if tp is None:
                continue
            E_pool = tp.solve()[1][0, 0, 1]
            base = None
            for name, spec in GROUPS:
                t, cs = terms(spec)
                if t is None:
                    continue
                n = len(t.eps)
                A, E, Af = amps(t, E_pool, np.ones(n))
                bs = np.array([amps(t, E_pool, joint.block_weights(n, rng)) for _ in range(NBOOT)])
                sA, sE, sAf = bs.std(0)
                floor = np.mean([c.floor for c in cs])
                print(f'  {ccd:3d} {name:24s} {A:6.3f}±{sA:.3f} {E:+6.2f}±{sE:.2f} {Af:6.3f}±{sAf:.3f}'
                      f' {floor:6.1f} {len(cs):3d}')
            print(f'      (pooled E {E_pool:+.2f})')
            del chunks


if __name__ == '__main__':
    main(*sys.argv[1:])
