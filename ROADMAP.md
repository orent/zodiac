# Roadmap and ideas for later

This file collects design decisions and ideas from the planning
discussion. Results so far are in `stepA/RESULTS.md`, `stepB/RESULTS.md`
and `TIMEBASE.md`.

## Near-term order

1. ~~Time base~~: done, see `TIMEBASE.md`.
2. ~~Stacking pipeline + joint estimator on S27~~: done (`stepB/joint.py` takes a list
   of sectors and works directly from the compact data). Original plan:
   **Stacking pipeline on the compact data branch.** For each sector and
   camera, store per-window cross/auto spectra plus each window's mean
   level, then delete the stamp files. Stacking = summing spectra.
   - Analysis scripts move to `stepB/aligned.py`. `onset.py` is done;
     `quicklook`, `lagfit`, `curve` and `earthshine` still read the
     per-stamp files.
3. **Joint estimator** (frequency-domain weighted least squares on the
   accumulated spectra):
   - kernel shape: parametric and log-binned;
   - Earthshine zero-delay term tied to (level − floor), not free;
   - a free timing offset Δt;
   - noise weights from null shifts;
   - jackknife by sector.
4. **Download sectors 28–38** (cameras 1–2, then 3–4): about 50 GB per
   camera pair, streamed, keeping only the compact output.
   - Expected: delay scale ±~10% on camera 1 with 12 sectors.
5. Optional: regress the low-frequency common mode against
   `POS_CORR`/`MOM_CENTR` (the tail lives below 0.5 mHz, where that noise
   is 66× photon).

## Deconvolution, with emphasis on the tail

Tail information is at 0.05–0.3 mHz, where noise is worst, so priors buy
the most.

1. **Parametric forward model as the baseline.**
   - Density r^−α × vertical profile, plus amplitude and Earthshine
     nuisance.
   - Every frequency constrains the tail through α.
2. **Constrained non-parametric fit** (NNLS / quadratic programming in the
   frequency domain):
   - log-spaced delay bins (0–60, 60–180, 180–420, 420–900, 900–2000,
     2000–4000 s);
   - positivity;
   - monotone decreasing beyond ~600 s. Leave the inner zone free, so a
     resonant-ring bump stays visible.
3. **Total-area prior.**
   - Response at zero frequency = zodiacal brightness × band factor, from
     the quiet floor in absolute units. Then tail = total − near part.
   - Use it as a prior with ~5–10% width (zodiacal fraction of the floor;
     TESS/862 nm band ratio).
4. **Bayesian alternative:** a Gaussian-process prior on log K(τ) with
   correlation length ∝ τ. It makes visible where the tail is
   prior-dominated.
- **Avoid:**
  - Richardson–Lucy and maximum entropy: they assume simple noise, while
    ours is coloured and correlated between windows;
  - Tikhonov smoothing on a linear delay grid;
  - fine free bins.
- **Prerequisites for the tail:**
  - 12–24 h windows and a gentler high-pass, applied identically to both
    series;
  - common-mode regression.
- **Validation: injection–recovery** of synthetic kernels (different α, a
  bump at 1500 s) into null data. With constrained estimators this is the
  only trustworthy error and resolution check.

## Better reference than raw VIRGO red

Write s = G·x + u (x = VIRGO, G = spectral-response difference,
u = part VIRGO doesn't see).
- u costs only signal-to-noise.
- **G(f) is degenerate with the dust response H(f)**, so it must be
  constrained externally.
- A fully blind reference from TESS is infeasible: per-frequency S/N is
  ~0.01–0.1.

1. **Earthshine as an in-situ calibrator.** Its response is a known
   zero-delay spike scaled by the level excess. Fitting its amplitude per
   frequency band measures G(f) in the TESS band directly.
2. **Multi-channel VIRGO input** (red + green SPM, + TSI):
   y = Σ B_c x_c with B_c = H·a_c.
   - The ratios B_c/B_red are independent of the dust response, so the
     channel blend that best matches the TESS band is identifiable.
   - Use frequency-constant weights.
   - Physics check: for granulation and p-modes, amplitude ∝ ∂B_λ/∂T,
     integrated over TESS response × solar spectrum.
3. **Three-cornered hat** (SPM red, SPM green, PMO6): pairwise
   cross-spectra give each instrument's own noise. Subtract it from S_xx
   to undo the attenuation from reference noise. Essential for TSI, whose
   floor is ~0.1 ppm²/µHz above 4.5 mHz.
4. Viewing angle: negligible for camera 1 (< ~10°). HMI-synthesised
   Sun-as-a-star views from other directions are possible, but not worth
   it.

**TSI as the post-2021 reference:** calibrate the red→TSI transfer
function on the 2018–2021 overlap first. The red–TSI coherence falls from
0.85 (< 1 mHz) to 0.5 (4 mHz), which would otherwise mimic extra
smearing.

## Non-axisymmetric and 3-D science

The measurement is inherently in Earth's co-rotating frame. The lines of
sight are always roughly anti-sunward, at ecliptic latitudes 18° (cam 1),
42° (cam 2), 66° (cam 3) and 90° (cam 4).

| Target | Feasibility | Notes |
|---|---|---|
| **Vertical structure / latitude-resolved profiles** | **Feasible** (S27–38) | Cam 4 is close to the vertical density profile at 1 AU; cam 1 constrains the radial index. About 400 20-s targets per sector on cam 4 vs ~200 on cam 1. |
| Symmetry-plane tilt (~2°) and **warp** with heliocentric distance | Marginal | Effective latitude varies ±2° with Earth's longitude, in opposite senses north and south, giving a few-% shape change. Needs the TSI-era stack (~100 sectors). |
| Earth's resonant ring (r ≈ 1.03–1.15 AU) | Hard | τ ≈ 30–150 s, +10% density. Needs ~3% inner-bin precision, and that bin is where Earthshine lands. Stacking is automatically co-rotating. |
| Trailing blob, Earth L4/L5 | Not reachable | ~60–90° elongation, never in the field of view. |
| Earth L2 dust | Not reachable | τ ≈ 10 s, degenerate with Earthshine. |
| Venus ring | Not reachable | Inside 1 AU. |
| Mars resonances, asteroid bands, Jupiter's Trojans | Not reachable | τ ≈ 1000–8000 s, a few % of the light, in the worst noise band. |
| Cloud centre offset (~0.01 AU) | Marginal | Annual few-% modulation of the inner bins. |

## Bookkeeping

- Compact data: orphan branch `data` (see its README). About 3 MB per
  sector plus a 31 MB VIRGO cut.
- Compute is not a constraint: ~15 CPU-min so far, and under ~1 h for the
  full 12-sector stack.
- Disk: 28 GB free. Keep per-sector spectra, not stamp files.

## Other data sources

| Source | Value | Cost and issues |
|---|---|---|
| **TESS 2-min, S1–38** (queued for S1–26, ≤1000 random targets per camera per sector) | Northern hemisphere with red SPM (S14–26), so the north/south tilt test with the same reference. Sectors 1–26 on top of 27–38. ~20× more stamps per sector (little gain past ~1000 per camera, because of the common mode). | ~8 GB per sector with the cap. 120-s grid (Nyquist 4.2 mHz). Product `sNNNN_2min`. |
| **Kepler short cadence** (58.85 s, 2009–2013, red SPM era) | **No Earthshine** (Earth-trailing orbit). Fixed field at ecliptic latitude ~+55–65°, so the solar elongation sweeps through the year, giving depth profiles in many directions (tomography). Kepler drifts through the region of Earth's trailing blob, so the inner bins may change over the mission. | Short-cadence background appears to be interpolated from the 30-min model (to verify), so recompute from pixel files (large). ~512 targets per quarter, high read noise per pixel. Sun seen up to ~30° from VIRGO's view (coherence loss). |
| **K2 short cadence** (2014–2018) | Ecliptic-plane lines of sight at changing elongation: radial structure within the dust plane. No Earthshine. | Heavy pointing jitter (6-h thruster cycle), few short-cadence targets, same background recomputation. |
| **PLATO** (L2, launch ~end 2026) | 26 cameras, 25-s cadence for bright targets, long pointings, minimal Earthshine at L2. | Depends on what background products the pipeline keeps. |
| TESS full-frame images | | Not useful: the common mode, not photons, is the limit; 200-s FFIs only start in the TSI era. |
| Ground-based survey arrays | | **Not viable.** Airglow (OH, O₂, 557.7 nm) at 1–10% and transparency changes at 0.1–1%, both on minute timescales, are ~3×10⁴ ppm²/µHz: 3–4 orders of magnitude above the TESS floor, and common to the whole field. Not solar-correlated at mHz (no bias), just far too large. |
| LASCO, STEREO-HI | | Cadence too slow. |

## Hypothetical TESS onboard mode: superpixel full-frame background

The idea: bin each CCD onboard into, e.g., 64×64-pixel superpixels at 20 s
with a robust statistic (clipped mean or low percentile), giving ~16k
values per cadence. That is ~1.6 kB/s, ~2 GB per orbit, small next to
the FFIs.

The gain is **spatial information, not photons**:
- **Space–time separation of the common mode.** The solar signal goes as
  ZL brightness × a smooth response; jitter follows star density;
  scattered light follows Earth/Moon gradients; electronics follow the
  CCD outputs. Removing the low-frequency common mode (66× photon now)
  turns the requirements from ~1 sector (detection), ~6 sectors
  (2-parameter) and ~600 sectors (3-bin non-parametric) into roughly a
  day each when photon-limited. Realistically, 10–100× better.
- **Earthshine fitted per superpixel and subtracted, not cut**, which
  frees the inner bins.
- **A continuous map**: latitude 6–90°, ±12° in elongation per camera.
  Plane tilt and warp per sector (seasonal); resonant-ring test
  plausible; time resolution toward ~1 min (≈0.03 AU) using 3–8 mHz.
- **Short-lived dust** (comet trails, meteor streams) in single sectors.

New limits:
- The VIRGO reference: its incoherent part acts as ~0.05 ppm²/µHz of
  noise, which needs the multi-channel and three-cornered-hat work.
- No red-channel SPM after 2021.
- Variable stars in superpixels.

## Local solar reference (no VIRGO), mainly for the superpixel mode

1. **Moonshine.** Sunlight reflected off a constant-albedo surface, at
   zero delay (seconds), seen in the TESS band (only slight lunar
   reddening). The 2:1 lunar resonance repeats the geometry every orbit.
   Present a few days per orbit. Fit the moonlight spatial pattern per
   cadence; its amplitude time series is the reference.
2. **Earthshine.** More duty cycle. Earth's albedo changes as it rotates
   and clouds evolve, mostly on hour timescales (affects < ~0.3 mHz).
   Already shown to carry the solar signal (E ≈ 0.75–0.85).
3. **Reference-free identification across lines of sight**
   (cross-relation): y_i = K_i ∗ s + n_i, with responses that share no
   common factors, determines all K_i up to one scale. An Earthshine or
   moonshine channel (K = δ) anchors it and gives s directly. Needs high
   S/N per line of sight across the band (superpixel mode).
- **Archive feasibility test now:** in Earthshine-rich windows, build a
   reference from the camera-wide scattered-light excess (gradients
   between CCDs and stamps). Check its coherence with VIRGO, and whether
   the dust response can be recovered with it instead of VIRGO.
- Not viable: the spacecraft coarse sun sensors (analog, coarse
   digitisation, thermal drift; ~10″ jitter at 45° incidence ≈ 35 ppm).

## Direct-sunlight leakage test

- No direct path (boresights ≥ 90° from the Sun, sunshade and bus). Even
  10⁻¹⁹ suppression per pixel would be ~1% of the background, so worth
  bounding.
- Signature:
  - a zero-delay term present in quiet windows (not ∝ level excess);
  - varying with the Sun angle to the spacecraft (~27° per sector);
  - a fixed spatial pattern per camera.
- Now: S27 quiet-window zero-delay term −4 ± 22 e⁻/s/px (cam 1, ~15% of
  the background, 1σ). The total area under the response (~1.0 per
  unit floor vs ~1.15 × ZL fraction) leaves little room for a big leak.
- **To do:** add a constant zero-delay term E₀ (and E₀ vs Sun angle) to
  `joint.py`, and bound it with S27–38 × 4 cameras (expect a few % of the
  background). Separating it from the nearest dust relies on frequency
  dependence (flat vs ~1/f).
- As a reference: unlikely (small fraction, mixed into every pixel). Only
  with spatial data, if it dominates a region.
