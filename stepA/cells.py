"""Sky-cell product: per camera, the field is cut into 6x6 deg cells (a 4x4
grid in a gnomonic projection about the camera centre, axes along
ecliptic east and north). Each cell gets its own combined fractional
background series, its mean sightline (unit vector, ecliptic J2000) and a
smoothed level (scattered-light proxy). Per camera, the median pointing
corrections POS_CORR1/2 are kept for common-mode regression.

The time-dependent sightline of a cell (the field is fixed on the sky while
the Sun moves ~1 deg/day) is computed at analysis time from the stored
direction and the window time.

Stamp vetting is taken from the noise.py output of the same sector.

usage: cells.py sector reduced_dir noise.npz outdir   (TESS_CADENCE,
       TESS_PRODUCT_SUFFIX as for compact.py)
"""

import ast, glob, os, re, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import noise as nz

CELL_DEG = 6.0
NGRID = 4
LEVEL_DECIMATE_S = 600


def ecl_unit(ra, dec):
    from astropy.coordinates import SkyCoord, BarycentricMeanEcliptic
    import astropy.units as u
    e = SkyCoord(ra=ra * u.deg, dec=dec * u.deg).transform_to(BarycentricMeanEcliptic())
    lon, lat = e.lon.rad, e.lat.rad
    return np.stack([np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)], -1)


def tangent_axes(c):
    """East and north unit vectors at centre c (ecliptic frame)."""
    z = np.array([0.0, 0.0, 1.0])
    east = np.cross(z, c)
    if np.linalg.norm(east) < 1e-6:          # camera centred on the pole
        east = np.array([1.0, 0.0, 0.0])
    east /= np.linalg.norm(east)
    north = np.cross(c, east)
    return east, north


def main(sector, reduced, noise_npz, outdir):
    sec = int(sector)
    out = os.path.join(outdir, f's{sec:04d}' + os.environ.get('TESS_PRODUCT_SUFFIX', ''))
    os.makedirs(out, exist_ok=True)
    d = np.load(noise_npz)
    rows = ast.literal_eval(re.sub(r'\bnan\b', 'None',
                                   re.sub(r'np\.\w+\(([^)]*)\)', r'\1', str(d['rows']))))
    byt = {r['tic']: r for r in rows}
    files = [p for p in sorted(glob.glob(f'{reduced}/*.npz')) if not p.endswith('.part.npz')]
    c0 = min(np.load(p)['cadenceno'].min() for p in files)
    c1 = max(np.load(p)['cadenceno'].max() for p in files)
    ncad = c1 - c0 + 1
    stamps = []
    for p in files:
        try:
            h, s = nz.load(p)
        except Exception:
            continue
        r = byt.get(h['TICID'])
        if r is None:
            continue
        stamps.append((h, s, r))
    # Vetting as in noise.py: in-band excess < 3x camera median; bright targets
    # with raw-flux coherence > 0.05 dropped.
    keep = []
    for cam in sorted({h['CAMERA'] for h, _, _ in stamps}):
        cs = [t for t in stamps if t[0]['CAMERA'] == cam]
        exc = np.array([t[2]['bands'][1] / t[2]['pred'] for t in cs])
        med = np.median(exc)
        keep += [t for t, e in zip(cs, exc) if e < 3 * med
                 and not (t[2]['tmag'] < 10 and t[2]['leak'] > 0.05)]

    cad = float(nz.CADENCE)
    dec_n = max(1, int(round(LEVEL_DECIMATE_S / cad)))
    out_arrays = {'c0': np.int64(c0), 'ncad': np.int64(ncad), 'cadence': np.float32(cad),
                  'cell_deg': np.float32(CELL_DEG), 'level_decimate': np.int32(dec_n)}
    cell_rows = []
    for cam in sorted({h['CAMERA'] for h, _, _ in keep}):
        cs = [t for t in keep if t[0]['CAMERA'] == cam]
        vec = ecl_unit(np.array([t[0]['RA_OBJ'] for t in cs]), np.array([t[0]['DEC_OBJ'] for t in cs]))
        centre = vec.mean(0)
        centre /= np.linalg.norm(centre)
        east, north = tangent_axes(centre)
        dots = vec @ centre
        xi = np.degrees((vec @ east) / dots)
        eta = np.degrees((vec @ north) / dots)
        half = CELL_DEG * NGRID / 2
        ix = np.clip(((xi + half) // CELL_DEG).astype(int), 0, NGRID - 1)
        iy = np.clip(((eta + half) // CELL_DEG).astype(int), 0, NGRID - 1)
        xs, ss, levs, pc1, pc2 = [], [], [], [], []
        for h, s, r in cs:
            x, sigma = nz.fractional(h, s)
            fx = np.full(ncad, np.nan, np.float32)
            fs = np.full(ncad, np.nan, np.float32)
            fl = np.full(ncad, np.nan, np.float32)
            idx = s['cadenceno'] - c0
            fx[idx], fs[idx] = x, sigma
            fl[idx] = s['sap_bkg'] / h['NPIX_AP']
            xs.append(fx), ss.append(fs), levs.append(fl)
            p1 = np.full(ncad, np.nan, np.float32)
            p2 = np.full(ncad, np.nan, np.float32)
            p1[idx], p2[idx] = s['pos_corr1'], s['pos_corr2']
            pc1.append(p1), pc2.append(p2)
        xs, ss, levs = np.array(xs), np.array(ss), np.array(levs)
        with np.errstate(all='ignore'):
            out_arrays[f'cam{cam}_poscorr1'] = np.nanmedian(np.array(pc1), 0).astype(np.float16)
            out_arrays[f'cam{cam}_poscorr2'] = np.nanmedian(np.array(pc2), 0).astype(np.float16)
        for gy in range(NGRID):
            for gx in range(NGRID):
                m = (ix == gx) & (iy == gy)
                if m.sum() < 2:
                    continue
                comb, pred = nz.combine(xs[m], ss[m])
                with np.errstate(all='ignore'):
                    lev = np.nanmedian(levs[m], 0)
                nd = ncad // dec_n * dec_n
                with np.errstate(all='ignore'):
                    lev_dec = np.nanmedian(lev[:nd].reshape(-1, dec_n), 1)
                u = vec[m].mean(0)
                u /= np.linalg.norm(u)
                name = f'cam{cam}_x{gx}y{gy}'
                out_arrays[f'{name}_frac'] = comb.astype(np.float16)
                out_arrays[f'{name}_level'] = lev_dec.astype(np.float32)
                cell_rows.append((cam, gx, gy, int(m.sum()), *u, float(pred),
                                  float(np.mean(xi[m])), float(np.mean(eta[m]))))
    cols = ['cam', 'gx', 'gy', 'n_stamps', 'ux', 'uy', 'uz', 'pred_white_ppm2_uhz', 'xi_deg', 'eta_deg']
    out_arrays['cells'] = np.array(cell_rows, dtype=float)
    out_arrays['cells_columns'] = np.array(cols)
    np.savez_compressed(os.path.join(out, 'cells.npz'), **out_arrays)
    print(f'sector {sec}: {len(cell_rows)} cells from {len(keep)} stamps')


if __name__ == '__main__':
    main(*sys.argv[1:])
