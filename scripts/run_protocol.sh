#!/usr/bin/env bash
# Adaptive-scheduling experiment driver.
#
# Usage: scripts/run_protocol.sh <set> [protocol ...]
#   <set>      ∈ { degraded, crash, all }   — which scenario set to run
#   [protocol] ∈ { mysticeti, blue-bottle, nemo-nemo, mahi-mahi-4, mahi-mahi-5 }
#              one or more; default is all of them
#
# For each (set × protocol) it runs the adaptive, fixed-n=1 and fixed-n=5
# variants RUNS times each, sequentially, then plots that set's pooled
# per-protocol comparison (shared y-axis across the set's protocols). The
# experiment tests are `#[ignore]`d (long-running), so they are invoked here
# with `--ignored`.
#
# Nothing is cleared: results accumulate under
# results/<set>/<protocol>/<variant>/, so re-running simply adds more runs to the
# pool. Runs are sequential so each sees the previous run's CSV — the in-test
# `run_index` then auto-increments, giving every run a distinct rotation/seed.
# Clear the pool (rm -r crates/simulator/results/<set>) whenever the scenario,
# the simulator's network model or the latency metric changes — pooled runs
# must all come from the same configuration to be comparable.
set -u

ALL_PROTOCOLS=(mysticeti blue-bottle nemo-nemo mahi-mahi-4 mahi-mahi-5 dag-hydrangea orcaella)
RUNS=9
RESULTS_ROOT="crates/simulator/results"

usage() {
    echo "usage: $0 <set> [protocol ...]"
    echo "  <set>      ∈ { degraded, crash, all }"
    echo "  [protocol] ∈ { ${ALL_PROTOCOLS[*]} }   (default: all)"
    exit 1
}

# --- Which scenario set(s)? ---
SET="${1:-}"
case "$SET" in
    degraded) SCENARIOS=(degraded) ;;
    crash)    SCENARIOS=(crash) ;;
    all)      SCENARIOS=(degraded crash) ;;
    *)        usage ;;
esac
shift

# --- Which protocol(s)? (default: all) ---
if [ "$#" -gt 0 ]; then
    PROTOCOLS=("$@")
else
    PROTOCOLS=("${ALL_PROTOCOLS[@]}")
fi
for p in "${PROTOCOLS[@]}"; do
    ok=0
    for ap in "${ALL_PROTOCOLS[@]}"; do [ "$p" = "$ap" ] && ok=1 && break; done
    [ "$ok" -eq 1 ] || { echo "unknown protocol: $p"; usage; }
done

echo "=== Building experiment test binary (no run) ==="
cargo test -p simulator --test adaptive_scheduling --no-run 2>&1 | tail -5
if [ "${PIPESTATUS[0]}" -ne 0 ]; then
    echo "Build failed — aborting."
    exit 1
fi

for scenario in "${SCENARIOS[@]}"; do
    # Crash test fns are named <ident>_crash_<variant>; degraded ones <ident>_<variant>.
    if [ "$scenario" = "crash" ]; then infix="_crash"; else infix=""; fi

    for proto in "${PROTOCOLS[@]}"; do
        ident="${proto//-/_}"   # test fns use underscores; folders use hyphens
        variants=("${ident}${infix}_adaptive" "${ident}${infix}_fixed_1" "${ident}${infix}_fixed_5")
        echo ""
        echo "=== [$scenario] $proto: $RUNS runs x ${#variants[@]} variants ==="
        for i in $(seq 1 "$RUNS"); do
            for v in "${variants[@]}"; do
                echo "--- [$scenario/$proto] run $i/$RUNS: $v ---"
                cargo test -p simulator --test adaptive_scheduling "$v" \
                    -- --ignored --exact --nocapture 2>&1 \
                    | grep -E "outcome|wrote|Leader scheduler|FAILED|error" \
                    || true
            done
        done
    done

    echo ""
    echo "=== [$scenario] plotting ${PROTOCOLS[*]} ==="
    python3 scripts/plot_adaptive_scheduling_normalized.py \
        --results-root "$RESULTS_ROOT/$scenario" --protocol "${PROTOCOLS[@]}"
done

echo ""
echo "=== All done. ==="
