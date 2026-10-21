#!/bin/bash
# Add the sky-cell product (cells.py) to sectors that already have the
# compact camera/CCD product: re-download the stamps, build cells from the
# existing noise.py output, commit, delete the stamps. Resumable.
#
# usage: run_cells.sh targets.csv workdir databranch_checkout sector...
#        (TESS_CADENCE / TESS_PRODUCT_SUFFIX as for run_sectors_v2.sh)
set -u
TARGETS=$1 WORK=$2 DATA=$3
shift 3
HERE=$(cd "$(dirname "$0")/.." && pwd)
PY="$HERE/.venv/bin/python"
SUFFIX=${TESS_PRODUCT_SUFFIX:-}
for S in "$@"; do
    SD=$(printf "%s/s%04d%s" "$DATA" "$S" "$SUFFIX")
    if [ -f "$SD/cells.npz" ]; then echo "cells $S$SUFFIX: already done"; continue; fi
    NZ="$WORK/noise_s$S$SUFFIX.npz"
    [ -f "$NZ" ] || NZ="$WORK/noise_s$S.npz"
    echo "cells $S$SUFFIX: start $(date +%H:%M:%S)"
    RED="$WORK/s$S$SUFFIX"
    rm -f "$RED"/*.part.npz 2>/dev/null
    for C in 1 2 3 4; do
        "$PY" "$HERE/stepA/fetch_reduce.py" "$TARGETS" "$S" "$C" "$RED" 16 > "$WORK/fetchc_s${S}_c$C.log" 2>&1
        echo "  cam $C: $(grep -c ' ok\| cached' "$WORK/fetchc_s${S}_c$C.log") files, $(grep -c failed "$WORK/fetchc_s${S}_c$C.log") failed"
    done
    "$PY" "$HERE/stepA/cells.py" "$S" "$RED" "$NZ" "$DATA" > "$WORK/cells_s$S$SUFFIX.log" 2>&1 || { echo "  cells failed"; continue; }
    (cd "$DATA" && git add -A && git commit -qm "Sky cells: sector $S$SUFFIX

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01SSmC1MH5t31radZp4oBcbF" && git push -q origin data) || echo "  git failed"
    rm -rf "$RED"
    echo "cells $S$SUFFIX: done $(date +%H:%M:%S), $(du -sh "$SD/cells.npz" | cut -f1)"
done
echo "cells all done $(date +%H:%M:%S)"
