# Step A results: noise in the TESS 20-s background, sector 27

Data:
- Sector 27 (2020-07-04 → 07-30), all 20-s light curves on cameras 1 and 2:
  400 files, 4.7 GB downloaded, 1.5 GB after keeping only the needed
  columns.
- Typical stamp: 32 background pixels (bit 4 of the aperture image).
- Sky level: about 180 e⁻/s/px.
- SPOC version: 5.0.

Pipeline:
- Fractional `SAP_BKG` per pixel, divided by a 3-h running median.
- Quality mask applied; cosmic-ray spikes clipped at 5σ.
- Welch PSDs over 6-h windows; 184 windows per camera.

Reproduce:

    python stepA/tic_coords.py targets.csv scripts/s*.sh
    python stepA/fetch_reduce.py targets.csv 27 1 reduced/
    python stepA/noise.py reduced/ noise.npz
    python stepA/requirements.py noise.npz

## Per stamp

Median over stamps, in ppm²/µHz. The number in brackets is the measured
noise divided by the photon + read-noise prediction.

| cam | stamps | predicted | 0.1–0.5 mHz | 0.5–2 mHz | 2–5 mHz | 5–25 mHz |
|---|---|---|---|---|---|---|
| 1 | 200 | 482 | 1066 (2.1×) | 822 (1.7×) | 819 (1.7×) | 831 (1.7×) |
| 2 | 200 | 792 | 1662 (2.1×) | 1414 (1.8×) | 1394 (1.8×) | 1394 (1.8×) |

- Single stamps are within about 2× of the photon limit, and nearly white.
- Target light leaking into `SAP_BKG` is negligible: the median coherence
  with the raw aperture flux in 0.1–2 mHz is 0.007.
- A first version of the veto used the background-subtracted `SAP_FLUX`.
  That shows a spurious anti-correlation that grows for faint targets,
  because the background noise is subtracted into it. Don't use it.

## Camera combination

Inverse-variance weighted combination of 197 stamps (camera 1) and 199
stamps (camera 2). The stamps are split into random halves:
- the half-difference keeps only noise that is independent between stamps;
- the half-sum also keeps the common mode;
- "common" is the half-sum minus the half-difference.

All values in ppm²/µHz.

| cam | band | total | independent | common | total / photon |
|---|---|---|---|---|---|
| 1 | 0.1–0.5 mHz | 163 | 45 | **117** | 66× |
| 1 | 0.5–2 mHz | 9.4 | 6.4 | 2.9 | 3.8× |
| 1 | 2–5 mHz | 6.6 | 4.5 | 2.0 | 2.7× |
| 2 | 0.1–0.5 mHz | 177 | 18 | **160** | 46× |
| 2 | 0.5–2 mHz | 11.0 | 7.3 | 3.7 | 2.9× |

Coherence between the camera 1 and camera 2 combinations:

| Band | Coherence |
|---|---|
| 0.1–0.5 mHz | 0.235 |
| 0.5–2 mHz | 0.028 |
| 2–5 mHz | 0.018 |

The estimator bias is 0.005.

- **The low-frequency common mode is spacecraft-wide**, not per camera.
  Pointing jitter, thermal effects and scattered light are the suspects.
- Above 0.5 mHz the combination stays within 3–4× of photon noise.
- A small white common term of about 2–3 ppm²/µHz is not explained yet.
  Candidates are the SPOC background or smear correction, or overlapping
  background pixels.

## What it means for the PoC

These numbers use the measured noise spectra (`requirements.py`) with the
model solar spectrum and dust kernel from `sensitivity.py`. They assume 80%
of the background is zodiacal light. One sector is about 23 usable days.

| Data used | 5σ detection | Delay scale ±20% (2-param profile) | 3-bin non-parametric |
|---|---|---|---|
| Camera 1, as measured | 1.0 sector | 5.9 sectors | ~600 sectors |
| Camera 1, f > 0.5 mHz only | 1.4 | 9.6 | ~1300 |
| Camera 1, common-mode floor (infinite stamps) | 0.3 | 1.8 | ~200 |
| Camera 2, as measured | 1.1 | 5.9 | ~600 |

**Conclusions:**

1. **Detection is within reach of the data already downloaded.** One sector
   per camera gives about 5σ. Cameras 1 and 2 together give roughly 7σ,
   assuming the model solar amplitude and zodiacal fraction hold.
2. **A 2-parameter depth profile needs about 6 sectors of camera 1.** That
   is roughly 70 GB to download, or about 2 sectors if the low-frequency
   common mode can be removed.
3. **A non-parametric profile is out of reach.** The far delay bin
   (1000–4000 s) lives below 0.5 mHz, which is exactly where the
   spacecraft-wide common mode sits, 66× above photon noise.
4. **The lever is the common mode, not more stamps.** Possible fixes:
   - regress against `POS_CORR`/`MOM_CENTR` and a scattered-light proxy;
   - fit the common mode jointly with the solar terms.

   Don't subtract the camera 2 series: it contains the solar signal too,
   with a different kernel.

## VIRGO references (downloaded, not used yet)

- **SPM red, 862 nm, 1-min** (`SSI/Montillet/RED-960123-210515-F60d-NEW.fits`):
  - Columns: days since 1996-01-23, and the fractional variation in ppm.
    The variation has been high-pass filtered on a 60-day scale.
  - Ends 2021-05-15.
  - Sector 27: 95% coverage, rms 43 ppm.
- **TSI PMO6, 1-min** (`TSI/VIRGO_TSI_Minute_V8_...fits`):
  - Columns: TAI seconds since 1958, TSI.
  - Runs 1996-02 → 2026-05.
  - Sector 27: 97% coverage, rms 66 ppm.

  So 1-min VIRGO does reach the present, but only as total irradiance.
  The red channel stops in 2021.

## VIRGO red channel vs TSI as the reference (one year around S27)

The two series were put on a common minute grid, high-passed with a 3-h
running median, and compared with Welch over 6-h windows (2651 windows).

| f [mHz] | PSD red | PSD TSI | TSI/red | coherence |
|---|---|---|---|---|
| 0.1–0.3 | 0.75 | 1.54 | 2.0 | 0.81 |
| 0.3–0.6 | 0.65 | 1.19 | 1.9 | 0.88 |
| 0.6–1.0 | 0.39 | 0.76 | 2.0 | 0.85 |
| 1–2 | 0.17 | 0.39 | 2.3 | 0.77 |
| 2–2.7 | 0.09 | 0.22 | 2.5 | 0.71 |
| 2.7–3.5 | 0.09 | 0.29 | 3.1 | 0.70 |
| 3.5–4.5 | 0.03 | 0.10 | 3.5 | 0.50 |
| 4.5–6 | 0.008 | 0.08 | 10 | 0.22 |

PSDs in ppm²/µHz.

- **Solar power:** TSI has about 2× the red channel's power (1.4× the
  amplitude). That is expected, since solar variability is larger at
  shorter wavelengths. It doesn't matter for an uncalibrated profile.
- **Instrument noise:** TSI (PMO6) has a white floor of about
  0.1 ppm²/µHz, which dominates above ~4.5 mHz. The red channel's floor is
  about 50× lower.
- **Coherence** between the two is only 0.8–0.88 below 1 mHz, falling to
  0.5 at 4 mHz. Part of that is the TSI noise floor; part is genuine
  differences in the signal between wavelengths.
- **Timing:** the cross-spectrum phase rises linearly with frequency,
  corresponding to about 40 s. That matches the difference between the
  sub-minute offsets in the two files' own timestamps (red :53 s,
  TSI :13 s). So the timestamps in the two files are consistent with each
  other. The time base must use each file's timestamps as given, never a
  rounded minute grid.

The TESS band (600–1000 nm) is close to the red channel (862 nm). Using TSI
as the reference therefore costs:
1. SNR² by about the red–TSI coherence: roughly 15–20% more data below
   2 mHz.
2. **A shape bias.** The estimate S_yx/S_xx with x = TSI is multiplied by
   a factor that falls with frequency, and that mimics extra smearing, i.e.
   apparently deeper dust.

Fix: measure the red→TSI transfer function over the 2018–2021 overlap and
divide it out when TSI is the only reference. Alternatively, use the red
channel for S27–S38 and treat later sectors as a cross-check.

## Next

Quick-look detection on sector 27:
- correct the time base (`TIMECORR`, and light time via Horizons);
- cross-spectrum and phase of the camera 1 and 2 combinations against
  VIRGO red and TSI;
- null tests with VIRGO shifted by ±N days.

## Compact data

`compact.py` writes the compact per-sector product (about 3 MB per sector)
and the TESS-era VIRGO cut (31 MB). These are stored on the orphan branch
`data`, documented in that branch's `README.md`.
