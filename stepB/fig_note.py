"""Figure for the research note: the delay kernel and the density profile.

Standard-pointing sectors (sectors.STANDARD), 2-min product, one VIRGO
reference (default LOI), tilted Kelsall geometry, fixed dt.
(a) Non-negative log-binned kernel per camera (joint.bin_rows + NNLS),
    per unit delay, against the Kelsall model kernel scaled to the same
    total over the injection-validated bins (0-2000 s).
(b) Dust density along the sightline relative to the model, cameras 1-2,
    vs heliocentric distance: measured bin / (scale x model bin weight).
Errors: bootstrap over whole sectors (resampling chunks), which includes
the sector-to-sector systematic, not only window-level noise.
The model kernel and the mean distance per bin are averaged over the
per-sector sightlines (north and south sectors mirror each other, so a
mean sightline vector would sit near the ecliptic and be wrong).
REPLOT=1 reuses the fitted values saved in the per-camera npz files and
only recomputes the model and the figure.

usage: GEOM_TILT=1 [EPS_MAX=0.3] [REPLOT=1] fig_note.py datadir out.png [ref] [dt] [nboot]
"""

import os, sys
import numpy as np
from scipy.optimize import nnls

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import geometry
import joint
from sectors import STANDARD

CAMS = (1, 2, 3, 4)
USE_BINS = 5            # 0-60 ... 900-2000 s; the 2000-4000 s bin is a common-mode floor


def sightline_r(u):
    s = np.linspace(0, 6.0, 20001)[1:]
    d = np.asarray(u[:3]) / np.linalg.norm(u[:3])
    p = np.array([1.0, 0, 0])[None, :] + s[:, None] * d[None, :]
    return np.linalg.norm(p, axis=1)


def main(datadir, out_png, ref='loi', dt='-10', nboot='200'):
    dt, nboot = float(dt), int(nboot)
    rng = np.random.default_rng(23)
    edges = np.array(joint.BIN_EDGES, float)
    res = {}
    for c in CAMS:
        npz = os.path.splitext(out_png)[0] + f'_cam{c}.npz'
        if os.environ.get('REPLOT'):
            d = np.load(npz)
            sol, bs = d['sol'], d['bs']
            secs = [s for s in sorted(STANDARD) if os.path.exists(os.path.join(datadir, f's{s:04d}_2min', f'cam{c}.npz'))]
            us = [joint.sightline(datadir, s, c) for s in secs]
        else:
            parts = []
            for s in sorted(STANDARD):
                try:
                    ch = joint.Chunk(datadir, s, '_2min', c, ref)
                except FileNotFoundError:
                    continue
                D, y, _ = joint.bin_rows([ch], dt)
                parts.append((D.T @ D, D.T @ y, ch.u))
            M = np.array([p[0] for p in parts]); v = np.array([p[1] for p in parts])

            def solve(w):
                A = np.tensordot(w, M, 1); b = w @ v
                R = np.linalg.cholesky(A + 1e-12 * np.eye(len(A))).T
                return nnls(R, np.linalg.solve(R.T, b))[0]

            sol = solve(np.ones(len(parts)))
            bs = np.array([solve(np.bincount(rng.integers(0, len(parts), len(parts)), minlength=len(parts)))
                           for _ in range(nboot)])
            us = [p[2] for p in parts]
        mbs, rms = [], []
        for u in us:
            tau, w = geometry.kernel(u, 1.34, 1.0)
            r = sightline_r(u)
            sel = [(tau >= a) & (tau < b) for a, b in zip(edges[:-1], edges[1:])]
            mbs.append([w[m].sum() for m in sel])
            rms.append([np.sum(w[m] * r[m]) / w[m].sum() if m.any() else np.nan for m in sel])
        mb, rmean = np.mean(mbs, axis=0), np.nanmean(rms, axis=0)
        scale = np.sum(sol[:USE_BINS] * mb[:USE_BINS]) / np.sum(mb[:USE_BINS] ** 2)
        lat = np.mean([abs(np.degrees(np.arcsin(u[2] / np.linalg.norm(u[:3])))) for u in us])
        res[c] = dict(sol=sol, bs=bs, mb=mb, scale=scale, rmean=rmean, n=len(us), lat=lat)
        lo, hi = np.percentile(bs, [16, 84], axis=0)
        print(f'camera {c} ({len(us)} sectors, mean |sightline latitude| {lat:.0f} deg): '
              f'model scale {scale:.3f}, E {sol[-1]:.2f} [{lo[-1]:.2f},{hi[-1]:.2f}]')
        for b in range(len(mb)):
            print(f'   {edges[b]:5.0f}-{edges[b + 1]:5.0f} s: {sol[b]:.3f} [{lo[b]:.3f},{hi[b]:.3f}]  '
                  f'model {scale * mb[b]:.3f}  ratio {sol[b] / (scale * mb[b]):.2f}  r_mean {rmean[b]:.2f} AU')
        np.savez(npz, **{k: np.asarray(v_) for k, v_ in res[c].items()})

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9.5, 3.8))
    cols = {1: '#1f4e79', 2: '#2e8b57', 3: '#c77c11', 4: '#a23b72'}
    width = np.diff(edges)
    mid = np.sqrt(np.maximum(edges[:-1], 15) * edges[1:])
    for c, d in res.items():
        lo, hi = np.percentile(d['bs'], [16, 84], axis=0)
        k = slice(0, USE_BINS)
        y = d['sol'][k] / width[k] * 1e3
        a1.errorbar(mid[k], y, yerr=[(d['sol'][k] - lo[k]) / width[k] * 1e3, (hi[k] - d['sol'][k]) / width[k] * 1e3],
                    fmt='o', color=cols[c], ms=4, capsize=2, label=f'camera {c} (|b| {d["lat"]:.0f}°)')
        a1.step(edges[:USE_BINS + 1], np.append(d['scale'] * d['mb'][k] / width[k] * 1e3,
                d['scale'] * d['mb'][USE_BINS - 1] / width[USE_BINS - 1] * 1e3), where='post', color=cols[c], lw=1, alpha=.7)
    a1.set_xscale('log'); a1.set_yscale('log')
    a1.set_xlabel('delay τ [s]'); a1.set_ylabel('response per ks of delay (per unit floor)')
    a1.set_title('(a) echo kernel vs Kelsall model (lines)', fontsize=10)
    a1.legend(fontsize=7, frameon=False)
    for c in (1, 2):
        d = res[c]
        lo, hi = np.percentile(d['bs'], [16, 84], axis=0)
        k = slice(0, USE_BINS)
        ratio = d['sol'][k] / (d['scale'] * d['mb'][k])
        a2.errorbar(d['rmean'][k] + (0.01 if c == 2 else 0), ratio,
                    yerr=[ratio - lo[k] / (d['scale'] * d['mb'][k]), hi[k] / (d['scale'] * d['mb'][k]) - ratio],
                    fmt='o', color=cols[c], ms=4, capsize=2, label=f'camera {c}')
    a2.axhline(1, color='k', lw=0.8, ls='--')
    a2.set_xlabel('mean heliocentric distance of the delay bin [AU]')
    a2.set_ylabel('density / Kelsall model')
    a2.set_ylim(0, 2.2)
    a2.set_title('(b) dust density along the sightline', fontsize=10)
    a2.legend(fontsize=8, frameon=False)
    fig.tight_layout()
    fig.savefig(out_png, dpi=200)
    print('figure written', out_png)


if __name__ == '__main__':
    main(*sys.argv[1:])
