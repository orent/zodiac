"""Dust density vs heliocentric distance from the delay bins.

For each camera: non-negative log-binned kernel (joint.bin_rows + NNLS,
block bootstrap) over the standard-pointing sectors, at dt fixed. Each
delay bin spans a range of heliocentric distance r along the camera's
sightline. The measured bin weight, divided by the model's bin weight
(Kelsall: r^-1.34 x fan, Hong phase, 1/r^2 illumination), times the model
density at the bin's weighted mean r, gives a density point. A single
scale per camera (the zodiacal fraction of the floor x band factor) is
fitted over the plotted bins, so the plot shows shape relative to
n(1 AU) = 1 on the sightline.

usage: plot_density.py datadir out.png [dt]
"""

import sys
import numpy as np
from scipy.optimize import nnls

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import joint
import geometry

SECTORS = '1-13_2min,17-23_2min,27-38'     # standard pointing only
CAMS = (1, 2)
USE_BINS = 5                               # 0-60 ... 900-2000 s (injection-validated)


def sightline_profile(u, v=1.0):
    s = np.linspace(0, 6.0, 20001)[1:]
    p = np.array([1.0, 0, 0])[None, :] + s[:, None] * np.asarray(u)[None, :]
    r = np.linalg.norm(p, axis=1)
    zeta = np.abs(p[:, 2] / r) / v
    mu = 0.189
    g = np.where(zeta < mu, zeta ** 2 / (2 * mu), zeta - mu / 2)
    fan = np.exp(-4.14 * g ** 0.942)
    return s, r, fan


def main(datadir, out_png, dt='-10'):
    dt = float(dt)
    rng = np.random.default_rng(11)
    results = {}
    for c in CAMS:
        chunks = []
        for sec, prod in joint.parse_sectors(SECTORS):
            try:
                chunks.append(joint.Chunk(datadir, sec, prod, c, 'red'))
            except FileNotFoundError:
                pass
        D, y, krow = joint.bin_rows(chunks, dt)
        sol = nnls(D, y)[0][:-1]
        nk = krow.max() + 1
        bs = []
        for _ in range(60):
            rw = np.sqrt(joint.block_weights(nk, rng)[krow])   # one draw for D and y
            bs.append(nnls(D * rw[:, None], y * rw)[0][:-1])
        bs = np.array(bs)
        u = np.mean([ch.u for ch in chunks], axis=0)
        u /= np.linalg.norm(u)
        tau, w = geometry.kernel(u, 1.34, 1.0)
        s, r, fan = sightline_profile(u)
        edges = joint.BIN_EDGES
        pts = []
        for b in range(USE_BINS):
            m = (tau >= edges[b]) & (tau < edges[b + 1])
            r_mean = np.sum(w[m] * r[m]) / w[m].sum()
            r_lo, r_hi = r[m].min(), r[m].max()
            n_model = r_mean ** -1.34 * np.interp(r_mean, r, fan)
            pts.append((r_mean, r_lo, r_hi, w[m].sum(), n_model))
        pts = np.array(pts)
        model_bins = pts[:, 3]
        # One scale per camera, least squares over the plotted bins.
        scale = np.sum(sol[:USE_BINS] * model_bins) / np.sum(model_bins ** 2)
        ratio = sol[:USE_BINS] / (scale * model_bins)
        lo, hi = np.percentile(bs[:, :USE_BINS], [16, 84], axis=0) / (scale * model_bins[None, :])
        results[c] = dict(pts=pts, dens=ratio * pts[:, 4], lo=lo * pts[:, 4], hi=hi * pts[:, 4],
                          scale=scale, u=u, r=r, fan=fan)
        print(f'camera {c}: scale {scale:.2f}; r_mean ' + ' '.join(f'{x:.2f}' for x in pts[:, 0]) +
              '; density/model ' + ' '.join(f'{x:.2f}' for x in ratio))

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    ink, ink2, grid, surf = '#0b0b0b', '#52514e', '#e4e3df', '#fcfcfb'
    colors = {1: '#2a78d6', 2: '#eb6834'}
    lats = {1: 18, 2: 42}
    fig, ax = plt.subplots(figsize=(8.5, 5.2), dpi=150)
    fig.patch.set_facecolor(surf)
    ax.set_facecolor(surf)
    rr = np.linspace(1.0, 3.2, 300)
    ax.plot(rr, rr ** -1.34, color=ink2, lw=1.2, ls=':', label='in-plane r$^{-1.34}$ (Kelsall radial law)')
    for c in CAMS:
        R = results[c]
        r, fan = R['r'], R['fan']
        m = r <= 3.2
        ax.plot(r[m], r[m] ** -1.34 * fan[m], color=colors[c], lw=1.5, alpha=0.55,
                label=f'Kelsall model along camera {c} sightline ({lats[c]}°)')
        p = R['pts']
        yerr = np.vstack([R['dens'] - R['lo'], R['hi'] - R['dens']])
        xerr = np.vstack([p[:, 0] - p[:, 1], p[:, 2] - p[:, 0]])
        ax.errorbar(p[:, 0], R['dens'], yerr=np.clip(yerr, 0, None), xerr=xerr, fmt='o', ms=6,
                    color=colors[c], ecolor=colors[c], elinewidth=1.4, capsize=0,
                    markeredgecolor=surf, markeredgewidth=1.5,
                    label=f'TESS × VIRGO echo, camera {c}')
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlim(0.98, 3.2)
    ax.set_ylim(0.15, 1.3)
    ticks = [1.0, 1.2, 1.5, 2.0, 2.5, 3.0]
    ax.set_xticks(ticks)
    ax.set_xticklabels([f'{t:g}' for t in ticks])
    yt = [0.2, 0.3, 0.5, 0.7, 1.0]
    ax.set_yticks(yt)
    ax.set_yticklabels([f'{t:g}' for t in yt])
    ax.minorticks_off()
    ax.set_xlabel('heliocentric distance r [AU]', color=ink)
    ax.set_ylabel('dust density along sightline, n / n(1 AU)', color=ink)
    ax.set_title('Zodiacal dust density from light-travel delay (TESS sectors 1–38, VIRGO red)',
                 color=ink, loc='left', fontsize=11)
    ax.grid(color=grid, lw=0.6)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color(ink2)
    ax.tick_params(colors=ink2)
    leg = ax.legend(fontsize=8, frameon=False, loc='lower left')
    for t in leg.get_texts():
        t.set_color(ink)
    fig.text(0.99, 0.01, 'Points: delay bins 0–60, 60–180, 180–420, 420–900, 900–2000 s; '
             'bars: bootstrap 68% and bin extent. One scale per camera.',
             ha='right', va='bottom', fontsize=7, color=ink2)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out_png, facecolor=surf)


if __name__ == '__main__':
    main(*sys.argv[1:])
