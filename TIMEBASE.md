# Time base

Every sample is referred to **the time its light left the Sun** (TDB). The
499 s Sun→1 AU travel time, the SOHO (L1) vs TESS geometry and TESS's
orbit then drop out. What remains between VIRGO and TESS is only the extra
path through the dust: τ ≈ 2s/c anti-sunward.

Code:
- `stepB/timebase.py`: conversions, Horizons ranges, overlap binning;
- `stepB/aligned.py`: aligned series from the compact data branch.

## Conventions

| Series | Stored time | Scale | Stamp marks | Source |
|---|---|---|---|---|
| TESS 20-s `TIME` | BTJD = BJD − 2457000 | TDB, barycentric toward the target | mid-cadence | TESS SDPDD |
| TESS `TIME − TIMECORR` | spacecraft JD − 2457000 | TDB at the spacecraft | mid-cadence | `TIMECORR` = per-target barycentric correction, up to ±500 s |
| VIRGO SPM red (Montillet F60d) | days since 1996-01-23 | **UTC** | **mid point** | see below |
| VIRGO PMO6 TSI minute (V8) | s since 1958-01-01 | TAI (FITS header) | **mid point** | see below |

Evidence for the VIRGO conventions:
- **Mid point.** Appourchaux et al. 2018 (A&A 617, A108, arXiv:1807.00998),
  Table A.1, footnote 3: for VIRGO, "time given in the file corresponds to
  mid point". This is the only explicit statement found.
  - The PMOD TSI README (2023) mentions a timestamp bug fix and that the
    hourly and daily products are mid-interval.
  - The SPM product files carry no timing keywords.
- **Red in UTC.** The red-vs-TSI cross-spectrum phase is linear in
  frequency, about 40 s. That matches the difference of the stamps'
  sub-minute offsets (red :53 s UTC, TSI :13 s UTC = :50 s TAI) only if
  the red day count is UTC. If it were TAI, the offset would be 3 s.
- That 1996 paper's p-mode timing compares intensity against velocity
  instruments. It cannot calibrate intensity timing absolutely, because
  the intrinsic I–V phase difference is about 80 s at 3 mHz.

## Corrections applied

1. **Barycentric correction undone.** TESS uses `TIME − TIMECORR`, fitted
   linearly against cadence number. All targets share cadences; the
   scatter across targets is 0.63 s.
2. **Light time to the Sun.**
   - t_emit = t_obs − r_helio/c, with r from JPL Horizons (TESS = −95,
     SOHO = −21, centre = Sun body centre).
   - Stored as `sNNNN/ephem.npz` on the data branch, at 10-min sampling.
   - Sector 27: TESS 505.7–507.5 s, SOHO 502.3–503.1 s. **TESS − SOHO =
     3.1–4.5 s.**
3. **Integration-window matching** (`bin_to_minutes`).
   - Each TESS 20-s box is weighted by its overlap with a 60-s box centred
     on the VIRGO stamp.
   - The first-look binning (`quicklook.to_minutes`) assigned samples by
     centre. That gave a hidden, constant shift of up to ±10 s, depending
     on the relative phase of the two grids.
   - Synthetic test at 3 mHz: the new binning is within 0.05 s for any
     grid phase; the old one was off by up to 10 s.
4. **UTC→TDB and TAI→TDB** via astropy.

## Residual timing uncertainty

| Term | Size |
|---|---|
| VIRGO stamp convention (mid point assumed; start/end would be ∓30 s) | ±30 s if the 2018 statement doesn't hold for the 2023 products |
| SoHO halo-orbit timing modulation (Appourchaux 2018) | ±0.8 s, 178-d period |
| SPM timing jumps | none found 1996–2017 vs GONG (Appourchaux 2018) |
| TESS clock / `TIMECORR` fit | < 1 s |
| Light-time model | ≪ 1 s |

## Onset vs response shape: a degeneracy

Sector 27, both cameras, VIRGO red, after the corrections. The onset scan
(`stepB/onset.py`) slides a fixed template and fits only its amplitude:

| Template | All windows | Quiet windows (level < floor + 30 e⁻/s/px) |
|---|---|---|
| model kernel (density r^−1.34) | −60 s [−84, −33] | −100 s [−182, −32] |
| compact kernel (r^−3) | −30 s [−55, −6] | −40 s [−108, +23] |
| zero-delay spike | +70 s [+50, +92] | +120 s [+74, +176] |

With TSI as the reference (all windows, model kernel): −42 s [−57, −26].

**The onset from a fixed template is degenerate with the response shape.**
Noise-free simulations with the real spectral weights show it: the
difference between the best onsets of two templates is the same for any
true response (model minus spike = −150 s; the data give −130 s). A clock
error and a more compact response are indistinguishable in this scan.

The degeneracy is broken by **|H(f)|, which depends on shape only**.
|H| per band, normalised to the 0.1–0.3 mHz band:

| band [mHz] | all windows | quiet windows | model r^−1.34 | r^−3 | r^−5 |
|---|---|---|---|---|---|
| 0.3–0.6 | 1.21 | 0.71 | 0.75 | 0.84 | 0.91 |
| 0.6–1.2 | 0.42 | 0.19 | 0.50 | 0.63 | 0.74 |
| 1.2–2.4 | 0.48 | 0.32 | 0.29 | 0.39 | 0.50 |
| 2.4–4.8 | 0.33 | 0.12 | 0.15 | 0.21 | 0.29 |

- Quiet windows follow the smooth-cloud model.
- All windows carry excess at high frequency: the flat zero-delay
  Earthshine term (see the stepB Earthshine test).

So the negative onset in all windows is mainly template mismatch from
Earthshine. In quiet windows the onset is −100 s [−182, −32], 1–3σ: no
compelling clock error. **The timing offset should be a free parameter in
the joint fit:**
- kernel shape;
- Earthshine term ∝ level excess;
- Δt;
- amplitude and phase over all windows.

The fixed-template scan should not be used to infer it.

Correction to an earlier note: the first onset scan quoted a "1σ range"
of −150..+60 s. That was the range where S/N dropped by 1. The correct
1σ interval is where S/N² drops by 1, which is much narrower (fixed in
`onset.py`).
