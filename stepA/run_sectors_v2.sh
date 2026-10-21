#!/bin/bash
# Download, reduce and compact TESS sectors (20-s, or 2-min with
# TESS_CADENCE=120 TESS_PRODUCT_SUFFIX=_2min), one at a time, committing
# each sector's compact product to the data branch and deleting the stamps.
# Resumable: a sector whose compact files already exist for every camera
# is skipped (with REQUIRE_CCD=1, only if per-CCD files exist too).
#
# usage: run_sectors.sh targets.csv workdir databranch_checkout sector...
set -u
TARGETS=$1 WORK=$2 DATA=$3
shift 3
HERE=$(cd "$(dirname "$0")/.." && pwd)
PY="$HERE/.venv/bin/python"
CAMS="1 2 3 4"
SUFFIX=${TESS_PRODUCT_SUFFIX:-}

for S in "$@"; do
    SD=$(printf "%s/s%04d%s" "$DATA" "$S" "$SUFFIX")
    done_all=1
    for C in $CAMS; do [ -f "$SD/cam$C.npz" ] || done_all=0; done
    # REQUIRE_CCD=1: also redo sectors made before the per-CCD split.
    if [ "${REQUIRE_CCD:-0}" = 1 ]; then ls "$SD"/cam*_ccd*.npz > /dev/null 2>&1 || done_all=0; fi
    if [ $done_all = 1 ] && [ -f "$SD/ephem.npz" ]; then
        echo "sector $S: already done"; continue
    fi
    echo "sector $S: start $(date +%H:%M:%S)"
    RED="$WORK/s$S$SUFFIX"
    # Partial outputs from an interrupted run would be picked up as data.
    rm -f "$RED"/*.part.npz 2>/dev/null
    for C in $CAMS; do
        "$PY" "$HERE/stepA/fetch_reduce.py" "$TARGETS" "$S" "$C" "$RED" 16 > "$WORK/fetch_s${S}_c$C.log" 2>&1
        echo "  cam $C: $(grep -c ' ok\| cached' "$WORK/fetch_s${S}_c$C.log") files, $(grep -c failed "$WORK/fetch_s${S}_c$C.log") failed"
    done
    echo "  reduced $(du -sh "$RED" | cut -f1), $(date +%H:%M:%S)"
    "$PY" "$HERE/stepA/noise.py" "$RED" "$WORK/noise_s$S$SUFFIX.npz" > "$WORK/noise_s$S$SUFFIX.log" 2>&1 || { echo "  noise failed"; continue; }
    "$PY" "$HERE/stepA/compact.py" sector "$S" "$RED" "$WORK/noise_s$S$SUFFIX.npz" "$DATA" > "$WORK/compact_s$S$SUFFIX.log" 2>&1 || { echo "  compact failed"; continue; }
    if [ "${CELLS:-0}" = 1 ]; then
        "$PY" "$HERE/stepA/cells.py" "$S" "$RED" "$WORK/noise_s$S$SUFFIX.npz" "$DATA" > "$WORK/cells_s$S$SUFFIX.log" 2>&1 || echo "  cells failed"
    fi
    "$PY" "$HERE/stepB/timebase.py" ephem "$DATA" "$S" "$SUFFIX" > /dev/null 2>&1 || echo "  ephem failed"
    (cd "$DATA" && git add -A && git commit -qm "Compact data: sector $S$SUFFIX, cameras 1-4

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01SSmC1MH5t31radZp4oBcbF" && git push -q origin data) || echo "  git failed"
    rm -rf "$RED"
    echo "sector $S: done $(date +%H:%M:%S), compact $(du -sh "$SD" | cut -f1)"
done
echo "all done $(date +%H:%M:%S)"
