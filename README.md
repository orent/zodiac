# zodiac: compact preprocessed data

This is the data-only orphan branch of
[orent/zodiac](https://github.com/orent/zodiac). It holds the compact
per-sector products of the TESS-background vs SOHO/VIRGO reflectometry
analysis. With these files, everything downstream runs without downloading
anything:
- timing correction;
- cross-spectra and deconvolution;
- the Earthshine regression;
- null tests.

The code lives on the analysis branch
(`claude/zodiacal-dust-depth-mapping-48b287`): `PLAN.md`, `stepA/`,
`stepB/`. `stepA/compact.py` produced every file here.

## Layout

    s0027/
      cam1.npz           camera-combined background series, camera 1
      cam2.npz           same, camera 2
      stamps.csv         per-stamp metadata and noise
      noise_spectra.npz  step-A camera PSDs (total / half-sum / half-diff)
      ephem.npz          JPL Horizons heliocentric range of TESS (-95) and SOHO (-21)
    virgo/
      spm_red_1min.npz   VIRGO SPM red channel, 1-min, 2018-07-01 -> 2021-05-15
      tsi_pmo6_1min.npz  VIRGO PMO6 TSI, 1-min, 2018-07-01 -> 2026-05-05

Each sector is about 3 MB. The VIRGO cut is 31 MB and is shared by all
sectors.

## `sNNNN/camK.npz`: one row per 20-s cadence

| array | dtype | meaning |
|---|---|---|
| `cadenceno` | int32 | TESS cadence number. Every target in the sector shares it. |
| `t_sc` | float64 | Spacecraft time, TDB, JD − 2457000. This is `TIME − TIMECORR`, i.e. the per-target barycentric correction undone. It comes from a linear fit of cadence number vs `TIME − TIMECORR` over 50 stamps; scatter across targets is 0.63 s. **Not yet** corrected for light time to the Sun. |
| `frac` | float32 | Camera-combined fractional background fluctuation (see below). NaN = no valid data. |
| `level` | float32 | Median over the camera's stamps of the absolute background, in e⁻/s per pixel. It uses the same quality mask but no spike clipping. |
| `n_stamps` | int16 | Number of stamps with a valid `level` at this cadence. |

How `frac` is built:
1. For each stamp, b = `SAP_BKG / NPIX_AP` (e⁻/s/px).
2. Divide b by its own 3-h running median, then subtract 1. That high-passes
   below about 0.1 mHz and normalises the series.
3. Mask cadences with `QUALITY & 0x9AFF` set: bits 1, 2, 4, 8, 16, 32, 64,
   128, 512, 1024, 2048, 4096 and 32768.
4. Clip 5σ single-cadence spikes (cosmic rays) against a 9-cadence median,
   ±1 cadence.
5. Combine stamps with inverse-variance weights. The weights come from the
   photon + read-noise prediction (10 e⁻ read noise per 2-s frame), using
   each stamp's background-pixel count.
6. Stamp vetting: drop stamps whose 0.5–2 mHz noise excess is more than 3×
   the camera median, and stamps with Tmag < 10 whose coherence with their
   raw aperture flux exceeds 0.05. S27 keeps 197 of 200 stamps on camera 1
   and 199 of 200 on camera 2.
7. Cadences where the summed weight is below half its median are NaN.

To get absolute units (e⁻/s/px) for the Earthshine regression, multiply
`frac` by `level`. The quiet-floor estimate is the 5th percentile of
`level` over 6-h windows.

## `sNNNN/stamps.csv`

One row per 20-s stamp on cameras 1 and 2. Units are in brackets.

| column | meaning |
|---|---|
| `tic` | TIC ID |
| `cam` | TESS camera |
| `tmag` | TESS magnitude |
| `nbkg` | number of background pixels: aperture bit 4 |
| `level` | median background [e⁻/s/px] |
| `pred` | photon + read-noise prediction for the stamp's fractional background [ppm²/µHz] |
| `leak` | 0.1–2 mHz coherence between the background and the raw aperture flux (`SAP_FLUX + SAP_BKG`), which tests for target-light leakage |
| `psd_*` | band-averaged PSD of the stamp's fractional background [ppm²/µHz] |

## `sNNNN/noise_spectra.npz`

Step-A Welch PSDs (6-h windows, ppm²/µHz) of each camera combination.

| array | meaning |
|---|---|
| `camK_f` | frequency grid [Hz] |
| `camK_total` | PSD of the combination of all kept stamps |
| `camK_half_sum` | PSD of the mean of two random halves of the stamps |
| `camK_half_diff` | PSD of the half-difference, i.e. independent noise only |
| `camK_pred` | photon-noise floor |

## `sNNNN/ephem.npz`

Heliocentric distances for the light-time correction to emission time at
the Sun. See `TIMEBASE.md` on the analysis branch.

| array | meaning |
|---|---|
| `tess_jd_tdb`, `tess_r_au` | TESS, TDB JD at 10-min steps, range from the Sun body centre [AU] |
| `soho_jd_tdb`, `soho_r_au` | the same for SOHO |

Produced by `stepB/timebase.py ephem <datadir> <sector>`.

## `virgo/`

These are unmodified values, only cut in time (and stored as float32).

| file | array | meaning |
|---|---|---|
| `spm_red_1min.npz` | `days_since_1996_01_23_utc` | Time, as in the source file |
| | `red_ppm` | Red-channel fractional variation [ppm]. High-pass filtered on a 60-day scale by the provider. |
| `tsi_pmo6_1min.npz` | `tai_seconds_since_1958` | Time [TAI s] |
| | `tsi_wm2_wrr` | TSI, original VIRGO scale (WRR) [W/m²] |
| | `tsi_wm2_si` | TSI, rescaled to SI [W/m²] |

Sources:
- SPM red: `projects.pmodwrc.ch/virgo/SSI/Montillet/RED-960123-210515-F60d-NEW.fits`
- TSI: `projects.pmodwrc.ch/virgo/TSI/VIRGO_TSI_Minute_V8_August2026_2026-08-26_13-55-06.fits`

Cite Finsterle et al. 2021 (Sci. Rep. 11, 7835) and acknowledge "the VIRGO
Experiment on the cooperative ESA/NASA Mission SOHO from VIRGO Team through
PMOD/WRC, Davos, Switzerland".

Timing caveats:
- The two VIRGO files time-stamp their minutes about 40 s apart: red at
  :53 s, TSI at :13 s.
- The cross-spectrum phase between them confirms that each file's own
  timestamps are right.
- **Always use the stored timestamps**, never a rounded minute grid.
- Stamps are taken as the **mid point** of the 1-min sample, per
  Appourchaux et al. 2018 (A&A 617, A108), Table A.1.
- The red day count is **UTC**. The TSI file is TAI.
- Details and the residual uncertainty are in `TIMEBASE.md` on the
  analysis branch.

## Sectors

| sector | dates | cameras | stamps (kept) | TESS source | SPOC |
|---|---|---|---|---|---|
| 27 | 2020-07-04 → 07-30 | 1, 2 | 197/200, 199/200 | MAST 20-s `*_fast-lc.fits` | 5.0.9, data release 38 |

## Regenerating or adding a sector

Run on the analysis branch, with the venv from `initenv` plus numpy,
scipy, astropy and tess-point:

    python stepA/tic_coords.py targets.csv tesscurl_sector_*_fast-lc.sh
    python stepA/fetch_reduce.py targets.csv <sector> 1 reduced/
    python stepA/fetch_reduce.py targets.csv <sector> 2 reduced/
    python stepA/noise.py reduced/ noise.npz
    python stepA/compact.py sector <sector> reduced/ noise.npz <data-branch-checkout>
    python stepA/compact.py virgo red.fits tsi.fits <data-branch-checkout>/virgo
    python stepB/timebase.py ephem <data-branch-checkout> <sector>

Per sector:
- download about 4.7 GB (about 2 min at the speed seen so far);
- reduced stamp files about 1.5 GB (not kept);
- compact output about 3 MB.
