"""North/south test of the echo amplitude: is the year-2 (northern) excess
in A a property of the dust or of the normalisation?

A is the echo per unit background floor, A ~ 1.15 x (zodiacal share of
the floor). So A x floor / 1.15 is the zodiacal brightness the echo
implies (e-/s/px). Per (sector, camera) chunk this script fits A at
alpha = 1.4 with E fixed at the camera's pooled value (E and A are
degenerate within one chunk), then compares, per group of chunks:
  A, the floor, the implied zodiacal brightness A x floor / 1.15, and the
  model brightness of the sightline (Kelsall/Hong, without and with the
  tilted symmetry plane), calibrated with one constant per camera.
If the echo traces the dust, A x floor follows the model brightness in
every group; a northern excess in A with no excess in A x floor / model
means the floor's non-zodiacal share differs.

usage: northsouth.py datadir [cameras] [spec]
"""

import os, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
os.environ['GEOM_TILT'] = '1'          # sightlines carry Earth's longitude
import geometry
import joint

ALPHA = 1.4
NBOOT = 40
SPEC = os.environ.get('NS_SPEC', '1-38_2min,27-38')
GROUPS = [
    ('year 1, S1-13 (south)', lambda s, p: p == '_2min' and 1 <= s <= 13),
    ('year 1 same season, S4-10 (south)', lambda s, p: p == '_2min' and 4 <= s <= 10),
    ('year 2 std, S17-23 (north)', lambda s, p: p == '_2min' and 17 <= s <= 23),
    ('year 2 far north, S14-16,24-26', lambda s, p: p == '_2min' and s in (14, 15, 16, 24, 25, 26)),
    ('year 3, S27-38 (south), 2-min', lambda s, p: p == '_2min' and 27 <= s <= 38),
    ('year 3, S27-38 (south), 20-s', lambda s, p: p == '' and 27 <= s <= 38),
    ('year 4 std, S47-51,54-55 (N), 2-min', lambda s, p: p == '_2min' and s in (47, 48, 49, 50, 51, 54, 55)),
    ('year 4 std, S47-51,54-55 (N), 20-s', lambda s, p: p == '' and s in (47, 48, 49, 50, 51, 54, 55)),
    ('year 4 far north, S40-41,52-53', lambda s, p: s in (40, 41, 52, 53)),
    ('year 4 ecliptic, S42-46, 2-min', lambda s, p: p == '_2min' and 42 <= s <= 46),
    ('year 4 ecliptic, S42-46, 20-s', lambda s, p: p == '' and 42 <= s <= 46),
]


def chunk_fit(ch, E, rng):
    t = joint.Terms([(ch.f, ch.cadence, ch.X, ch.Y, ch.eps, ch.u)], None, alphas=[ALPHA])

    def amp(wk):
        m11 = np.sum(wk * t.P[ALPHA])
        m12 = ((wk * t.eps) @ t.Q[ALPHA])[0]
        r1 = (wk @ t.U[ALPHA])[0]
        return (r1 - E * m12) / m11

    n = len(t.eps)
    bs = [amp(joint.block_weights(n, rng)) for _ in range(NBOOT)]
    return amp(np.ones(n)), np.std(bs)


def wmean(x, s):
    w = 1 / np.asarray(s) ** 2
    return np.sum(w * x) / np.sum(w), 1 / np.sqrt(np.sum(w))


def main(datadir, cameras='1,2,3,4', spec=SPEC):
    joint.DTS = np.array([0.0])
    rng = np.random.default_rng(21)
    print(f'north/south test: per-chunk A at alpha {ALPHA} (E fixed per camera), floor in e-/s/px,')
    print('implied zodiacal brightness Z = A x floor / 1.15, model brightness B (Kelsall/Hong),')
    print('Z/B normalised per camera to the year-1 (south) group.\n')
    for cam in (int(c) for c in cameras.split(',')):
        rows = []
        chunks = []
        for sec, prod in joint.parse_sectors(spec):
            try:
                chunks.append((sec, prod, joint.Chunk(datadir, sec, prod, cam, 'red')))
            except FileNotFoundError:
                continue
        # Pooled E over the standard-pointing chunks.
        std = [c for s, p, c in chunks if not (p == '_2min' and s in (14, 15, 16, 24, 25, 26))
               and s not in (40, 41, 42, 43, 44, 45, 46, 52, 53)]
        tp = joint.Terms([(c.f, c.cadence, c.X, c.Y, c.eps, c.u) for c in std], None, alphas=[ALPHA])
        E = tp.solve()[1][0, 0, 1]
        for sec, prod, ch in chunks:
            A, sA = chunk_fit(ch, E, rng)
            geometry.TILT = False
            B0 = geometry.brightness(ch.u[:3], ALPHA)
            geometry.TILT = True
            B1 = geometry.brightness(ch.u, ALPHA)
            rows.append((sec, prod, A, sA, ch.floor, B0, B1, np.degrees(np.arcsin(ch.u[2]))))
        print(f'camera {cam}  (E fixed at {E:+.2f}; {len(rows)} chunks)')
        print(f'  {"group":32s} {"n":>3s} {"lat":>5s} {"A":>13s} {"floor":>6s} {"Z":>13s}'
              f' {"Z/B flat":>14s} {"Z/B tilted":>14s}')
        ref = None
        for name, sel in GROUPS:
            g = [r for r in rows if sel(r[0], r[1])]
            if not g:
                continue
            A = np.array([r[2] for r in g]); sA = np.array([r[3] for r in g])
            fl = np.array([r[4] for r in g])
            Z, sZ = A * fl / 1.15, sA * fl / 1.15
            B0 = np.array([r[5] for r in g]); B1 = np.array([r[6] for r in g])
            mA = wmean(A, sA); mZ = wmean(Z, sZ)
            q0 = wmean(Z / B0, sZ / B0); q1 = wmean(Z / B1, sZ / B1)
            if ref is None:
                ref = (q0[0], q1[0])
            lat = np.median([r[7] for r in g])
            print(f'  {name:32s} {len(g):3d} {lat:+5.0f} {mA[0]:6.3f}±{mA[1]:.3f} {np.mean(fl):6.1f}'
                  f' {mZ[0]:6.2f}±{mZ[1]:.2f}  {q0[0] / ref[0]:6.3f}±{q0[1] / ref[0]:.3f}'
                  f'  {q1[0] / ref[1]:6.3f}±{q1[1] / ref[1]:.3f}')
        print()


if __name__ == '__main__':
    main(*sys.argv[1:])
