# Roadmap and ideas for later

This file collects design decisions and ideas from the planning
discussion. Results so far are in `stepA/RESULTS.md`, `stepB/RESULTS.md`
and `TIMEBASE.md`.

## Near-term order

1. ~~Time base~~: done, see `TIMEBASE.md`.
2. **Stacking pipeline on the compact data branch.** For each sector and
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
