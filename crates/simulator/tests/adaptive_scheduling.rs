// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Adaptive leader-scheduling experiments.
//!
//! Each test runs an oscillating scenario — a healthy network, a sustained
//! period of stress, then healthy again — under three consensus configurations:
//!
//! * `adaptive`  — the leader scheduler is on. It ramps the leader count *up*
//!   while the network is healthy (direct commits keep landing, so the extra
//!   leaders pay off) and backs it *off* once the direct-commit rate drops,
//!   shedding the extra leader slots that would otherwise sit undecided/skipped
//!   and stall the commit pipeline.
//! * `fixed_1`   — a single leader per round. Few slots to stall, but no
//!   redundancy if that one leader is slow.
//! * `fixed_5`   — five leaders per round. More redundancy, but every extra
//!   leader is another slot that can stall when the network is stressed.
//!
//! Two stress modes are exercised, as separate `#[ignore]`d experiments:
//! * **high latency** (`<proto>_adaptive` / `_fixed_*`) — every link touching
//!   a few validators (capped at each protocol's fault tolerance, like the
//!   crash set) is slowed to ~1 s, so their blocks arrive rounds late and
//!   their leader slots are cleanly skipped. The cost of a skipped slot is the
//!   leader timeout every other validator spends waiting for it, and with more
//!   leaders per round more rounds contain one of the slow leaders — which is
//!   what the adaptive scheduler should back off from.
//! * **crash faults** (`<proto>-crash` variants, e.g. `mysticeti_crash_*`) —
//!   a few validators (capped at each protocol's fault tolerance) are fully
//!   isolated via a network partition, so their leader slots must be skipped.
//!   The more leaders per round, the more rounds wait out the timeout for a
//!   dead leader, so the adaptive scheduler should back off here too.
//!
//! Each run writes a CSV under `results/<set>/<protocol>/<variant>/` (`<set>`
//! is `degraded` or `crash`) with one row per replica per sample window:
//! `time_s,validator,p90_ms,avg_ms,phase,commits,leaders,timeouts,direct_commits,direct_skips,indirect`.
//! `leaders` is the committer's leaders-per-round at sample time (the adaptive
//! scheduler's trace); `commits` is how many of the replica's own transactions
//! committed in the window (`0` = stalled); the last four are the window's
//! leader timeouts and leader-slot decisions by type, read from the replica's
//! metrics — `timeouts` is the cost of waiting on slow or dead leaders, and
//! `indirect` counts slots the direct rule left undecided.
//! `scripts/plot_adaptive_scheduling_normalized.py` pools those CSVs into the
//! per-protocol comparison plots (latency over time, per-phase means, and the
//! leaders-per-round trace with the scheduler's lag), which show how
//! `adaptive` tracks each regime relative to the two fixed baselines.
//!
//! These are long-running experiments, not CI checks. Run them via
//! `scripts/run_protocol.sh <set> [protocol...]` — `<set>` is `degraded`,
//! `crash`, or `all`; protocols default to all five. Results for each set land
//! under `results/<set>/` and are plotted separately.

use std::{
    fs,
    io::Write,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use consensus::protocol::ConsensusProtocol;
use replica::{
    config::ReplicaParameters,
    result::{LatencySample, Outcome},
};
use simulator::{NetworkTopology, SimulationConfig, SimulationPhase, SimulationRunner};

/// Mysticeti protocol selection with explicit scheduler settings.
fn mysticeti(leader_count: usize, adaptive_scheduler: bool) -> ConsensusProtocol {
    ConsensusProtocol::Mysticeti {
        leader_count: NonZeroUsize::new(leader_count).expect("leader_count must be non-zero"),
        adaptive_scheduler,
        max_leaders_per_round: NonZeroUsize::new(5).unwrap(),
        scheduler_interval: 25,
    }
}

/// Blue Bottle protocol selection with explicit scheduler settings.
fn blue_bottle(leader_count: usize, adaptive_scheduler: bool) -> ConsensusProtocol {
    ConsensusProtocol::BlueBottle {
        leader_count: NonZeroUsize::new(leader_count).expect("leader_count must be non-zero"),
        adaptive_scheduler,
        max_leaders_per_round: NonZeroUsize::new(5).unwrap(),
        scheduler_interval: 25,
    }
}

/// Nemo-Nemo protocol selection with explicit scheduler settings.
fn nemo_nemo(leader_count: usize, adaptive_scheduler: bool) -> ConsensusProtocol {
    ConsensusProtocol::NemoNemo {
        leader_count: NonZeroUsize::new(leader_count).expect("leader_count must be non-zero"),
        adaptive_scheduler,
        max_leaders_per_round: NonZeroUsize::new(5).unwrap(),
        scheduler_interval: 25,
    }
}

/// DagHydrangea protocol selection with explicit scheduler settings, at the
/// fault configuration that matches the other protocols on a 10-validator
/// committee: `f = 3, c = 0, k = 0` meets its bound `n >= 3f + 2c + k + 1`
/// exactly, and its quorum `n - f - c = 7` tolerates the same three slow or
/// crashed validators as Mysticeti.
fn dag_hydrangea(leader_count: usize, adaptive_scheduler: bool) -> ConsensusProtocol {
    ConsensusProtocol::DagHydrangea {
        leader_count: NonZeroUsize::new(leader_count).expect("leader_count must be non-zero"),
        f: 3,
        c: 0,
        k: 0,
        adaptive_scheduler,
        max_leaders_per_round: NonZeroUsize::new(5).unwrap(),
        scheduler_interval: 25,
    }
}

/// Orcaella protocol selection with explicit scheduler settings, in its hybrid
/// configuration on a 10-validator committee: `f = 1, c = 1` meets its bound
/// `n >= 5f + 3c + 1`, and its quorum `n - f - c = 8` tolerates two slow or
/// crashed validators. (Matching Mysticeti's three would need the crash-only
/// `f = 0, c = 3`; any `f > 0` caps the tolerance at two when n = 10.)
fn orcaella(leader_count: usize, adaptive_scheduler: bool) -> ConsensusProtocol {
    ConsensusProtocol::Orcaella {
        leader_count: NonZeroUsize::new(leader_count).expect("leader_count must be non-zero"),
        f: 1,
        c: 1,
        adaptive_scheduler,
        max_leaders_per_round: NonZeroUsize::new(5).unwrap(),
        scheduler_interval: 25,
    }
}

/// Mahi-Mahi protocol selection with explicit wave length and scheduler settings.
fn mahi_mahi(leader_count: usize, wave_length: u64, adaptive_scheduler: bool) -> ConsensusProtocol {
    ConsensusProtocol::MahiMahi {
        leader_count: NonZeroUsize::new(leader_count).expect("leader_count must be non-zero"),
        wave_length,
        adaptive_scheduler,
        max_leaders_per_round: NonZeroUsize::new(5).unwrap(),
        scheduler_interval: 25,
    }
}

/// Committee size for the experiment: 3f+1 with f = 3.
const COMMITTEE_SIZE: usize = 10;
/// Validators whose links are slow in the degraded phase (the first `slow_count`
/// of this block, rotated by the run index), and the start of the crash set.
const SLOW_VALIDATORS: [usize; 3] = [7, 8, 9];

/// Directory holding the CSVs for one scenario/protocol/variant triple:
/// `results/<scenario>/<protocol>/<variant>/`.
fn results_dir(scenario: &str, protocol: &str, variant: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("results")
        .join(scenario)
        .join(protocol)
        .join(variant)
}

/// Per-run index used to pick the rotation offset and RNG seed. Honors the
/// `RUN_INDEX` environment variable when set (e.g. by a parallel driver
/// script); otherwise it is the count of CSVs already written for this
/// scenario/protocol/variant, so sequential `cargo test` invocations
/// auto-increment with no setup — each run gets a fresh rotation and seed.
fn run_index(scenario: &str, protocol: &str, variant: &str) -> usize {
    if let Some(k) = std::env::var("RUN_INDEX").ok().and_then(|v| v.parse().ok()) {
        return k;
    }
    fs::read_dir(results_dir(scenario, protocol, variant))
        .map(|entries| entries.filter_map(|e| e.ok()).count())
        .unwrap_or(0)
}

/// Healthy → degraded → healthy, all full-mesh.
///
/// `slow_count` validators (a block starting in `SLOW_VALIDATORS`, rotated by
/// `k`) are slow in the degraded phase. Like the crash set's `crash_count`, it
/// is capped at each protocol's fault tolerance (`n - commit_quorum`): the DAG
/// round clock advances on the committer's commit quorum, so a protocol whose
/// quorum needs the slow validators' blocks — blue-bottle's 9 of 10 — would
/// otherwise run every round at the slow links' pace for *any* leader count,
/// which measures the fault-tolerance mismatch rather than the scheduler.
///
/// In the degraded phase every link touching a slow validator runs at
/// 1.0–1.3 s in *both* directions (the slow validator is symmetrically slow),
/// while the remaining validators keep their 50–100 ms links and form a
/// quorum on their own. A slow validator therefore falls rounds behind and
/// its leader block reaches nobody in time: the fast quorum blames it and the
/// slot is cleanly skipped. What a skipped slot costs is the leader timeout
/// the fast validators spend waiting for it before they propose — one
/// `round_timeout` per round that has a slow leader. With three slow validators
/// and one leader per round that is 3 rounds in 10; with five leaders it is 7
/// in 10, so the round period (and commit latency) grows with the leader
/// count, and the adaptive scheduler is expected to shed leaders until it
/// matches `fixed_1`.
///
/// A *one-way* slowdown (`slow_blind`, i.e. only the slow validator's outgoing
/// links to part of the committee) was tried first and rejected: the slow
/// leader's block then arrives late at only part of the committee, and whether
/// that leaves the slot undecided is a race against the round timeout that
/// *more* leaders win more often (waiting on a leader nearly every round keeps
/// the receivers in step with the slow validators), so extra leader slots
/// were not a liability there.
fn three_phase_config(
    consensus: ConsensusProtocol,
    k: usize,
    slow_count: usize,
) -> SimulationConfig {
    let healthy = |duration_secs| SimulationPhase {
        name: Some("healthy".to_string()),
        latency_min_ms: 50,
        latency_max_ms: 100,
        duration_secs,
        sample_interval_secs: 3,
        ..Default::default()
    };
    let degraded = SimulationPhase {
        name: Some("high-latency".to_string()),
        latency_min_ms: 50,
        latency_max_ms: 100,
        slow_validators: (0..slow_count)
            .map(|i| (SLOW_VALIDATORS[0] + i + k) % COMMITTEE_SIZE)
            .collect(),
        slow_latency_min_ms: 1000,
        slow_latency_max_ms: 1300,
        duration_secs: 400,
        sample_interval_secs: 5,
        ..Default::default()
    };

    SimulationConfig {
        committee_size: COMMITTEE_SIZE,
        rng_seed: k as u64,
        replica_parameters: ReplicaParameters {
            consensus,
            ..Default::default()
        },
        phases: vec![healthy(100), degraded, healthy(100)],
        ..Default::default()
    }
}

/// Healthy → `crash_count` validators crash → healthy.
///
/// The crashed validators (a `crash_count`-sized block starting in the slow
/// region, rotated by `k`) are placed in no partition group, so every link
/// touching them is torn down — they neither send nor receive blocks, modelling
/// a clean crash fault (and they rejoin when the final healthy phase restores
/// the full mesh). `crash_count` is chosen per protocol so the live set is still
/// a commit quorum and consensus keeps progressing: a crashed validator's leader
/// slot is blamed by the live quorum and skipped, after the live validators have
/// waited out the leader timeout for it. The more leaders per round, the more
/// rounds contain a dead leader to wait for, so the adaptive scheduler should
/// back off — behaving like `fixed_1` here while `fixed_5` pays for its dead
/// leaders.
fn crash_phase_config(
    consensus: ConsensusProtocol,
    k: usize,
    crash_count: usize,
) -> SimulationConfig {
    let healthy = |duration_secs| SimulationPhase {
        name: Some("healthy".to_string()),
        latency_min_ms: 50,
        latency_max_ms: 100,
        duration_secs,
        sample_interval_secs: 3,
        ..Default::default()
    };

    let crashed_set: Vec<usize> = (0..crash_count)
        .map(|i| (SLOW_VALIDATORS[0] + i + k) % COMMITTEE_SIZE)
        .collect();
    let live: Vec<usize> = (0..COMMITTEE_SIZE)
        .filter(|validator| !crashed_set.contains(validator))
        .collect();
    let crashed = SimulationPhase {
        name: Some("crashed".to_string()),
        latency_min_ms: 50,
        latency_max_ms: 100,
        topology: NetworkTopology::Partition(vec![live]),
        duration_secs: 90,
        sample_interval_secs: 5,
        ..Default::default()
    };

    // The final healthy phase is long: crashed validators froze for the whole
    // outage and must catch up a large backlog before they reappear at the
    // leader frontier. Until they do, ~`crash_count` leader slots stay dead and
    // the commit rate sits below the scheduler's increase threshold, so recovery
    // back up to `max_leaders_per_round` needs much more time than the backoff
    // down took (AIMD: fast multiplicative down, slow additive up).
    SimulationConfig {
        committee_size: COMMITTEE_SIZE,
        rng_seed: k as u64,
        replica_parameters: ReplicaParameters {
            consensus,
            ..Default::default()
        },
        phases: vec![healthy(100), crashed, healthy(100)],
        ..Default::default()
    }
}

/// Next free `results/<scenario>/<protocol>/<variant>/<n>.csv` path
fn next_csv_path(scenario: &str, protocol: &str, variant: &str) -> PathBuf {
    let dir = results_dir(scenario, protocol, variant);
    fs::create_dir_all(&dir).expect("failed to create CSV directory");
    let existing = fs::read_dir(&dir)
        .map(|entries| entries.filter_map(|e| e.ok()).count())
        .unwrap_or(0);
    dir.join(format!("{}.csv", existing + 1))
}

fn write_csv(path: &Path, samples: &[LatencySample]) {
    let mut file = fs::File::create(path).expect("failed to create latency CSV");
    writeln!(
        file,
        "time_s,validator,p90_ms,avg_ms,phase,commits,leaders,timeouts,direct_commits,direct_skips,indirect"
    )
    .unwrap();
    for s in samples {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{}",
            s.time_s,
            s.validator,
            s.p90_ms,
            s.avg_ms,
            s.phase,
            s.commits,
            s.leaders,
            s.timeouts,
            s.direct_commits,
            s.direct_skips,
            s.indirect
        )
        .unwrap();
    }
}

/// Run one experiment (config built by `build_config`) and append its latency
/// CSV under `results/<scenario>/<protocol>/<variant>/`.
fn run_scenario(
    scenario: &str,
    protocol: &str,
    variant: &str,
    consensus: ConsensusProtocol,
    build_config: impl Fn(ConsensusProtocol, usize) -> SimulationConfig,
) {
    let k = run_index(scenario, protocol, variant);
    let config = build_config(consensus, k);
    let results = SimulationRunner::new(config)
        .run()
        .expect("simulation run failed");

    assert_ne!(
        results.outcome,
        Outcome::Diverged,
        "{scenario}/{protocol}/{variant}: replicas diverged — safety violation"
    );

    let path = next_csv_path(scenario, protocol, variant);
    write_csv(&path, &results.latency_series);
    println!(
        "{scenario}/{protocol}/{variant}: run_index={k}, outcome={:?}, wrote {} latency samples to {}",
        results.outcome,
        results.latency_series.len(),
        path.display()
    );
}

/// Run the high-latency "degraded" three-phase scenario with `slow_count`
/// validators on slow links (see `three_phase_config` for the per-protocol cap).
fn run_variant(protocol: &str, variant: &str, consensus: ConsensusProtocol, slow_count: usize) {
    run_scenario(
        "degraded",
        protocol,
        variant,
        consensus,
        move |consensus, k| three_phase_config(consensus, k, slow_count),
    );
}

/// Run the "crash" scenario with `crash_count` validators isolated mid-run.
fn run_crash_variant(
    protocol: &str,
    variant: &str,
    consensus: ConsensusProtocol,
    crash_count: usize,
) {
    run_scenario(
        "crash",
        protocol,
        variant,
        consensus,
        move |consensus, k| crash_phase_config(consensus, k, crash_count),
    );
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn mysticeti_adaptive() {
    run_variant("mysticeti", "adaptive", mysticeti(5, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn mysticeti_fixed_1() {
    run_variant("mysticeti", "fixed_1", mysticeti(1, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn mysticeti_fixed_5() {
    run_variant("mysticeti", "fixed_5", mysticeti(5, false), 3);
}

// --- Crash-fault scenario: validators go offline mid-run. ---
//
// One adaptive-vs-fixed comparison per protocol, each writing to its own
// `results/crash/<protocol>/` folder (so it gets its own pooled graph). The
// crash count is capped at each protocol's fault tolerance (`n - commit_quorum`)
// at COMMITTEE_SIZE = 10, so the live set is still a quorum and consensus stays
// live: mysticeti / mahi-mahi / dag-hydrangea (f=3, c=0, k=0) tolerate 3,
// nemo-nemo 4 (we use 3), orcaella (f=1, c=1) 2 (its `n-f-c = 8` quorum must
// stay live), and blue-bottle only 1 (its `4n/5+1 = 9` quorum must stay live).

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn mysticeti_crash_adaptive() {
    run_crash_variant("mysticeti", "adaptive", mysticeti(5, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn mysticeti_crash_fixed_1() {
    run_crash_variant("mysticeti", "fixed_1", mysticeti(1, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn mysticeti_crash_fixed_5() {
    run_crash_variant("mysticeti", "fixed_5", mysticeti(5, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn blue_bottle_crash_adaptive() {
    run_crash_variant("blue-bottle", "adaptive", blue_bottle(5, true), 1);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn blue_bottle_crash_fixed_1() {
    run_crash_variant("blue-bottle", "fixed_1", blue_bottle(1, false), 1);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn blue_bottle_crash_fixed_5() {
    run_crash_variant("blue-bottle", "fixed_5", blue_bottle(5, false), 1);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn nemo_nemo_crash_adaptive() {
    run_crash_variant("nemo-nemo", "adaptive", nemo_nemo(5, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn nemo_nemo_crash_fixed_1() {
    run_crash_variant("nemo-nemo", "fixed_1", nemo_nemo(1, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn nemo_nemo_crash_fixed_5() {
    run_crash_variant("nemo-nemo", "fixed_5", nemo_nemo(5, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn mahi_mahi_4_crash_adaptive() {
    run_crash_variant("mahi-mahi-4", "adaptive", mahi_mahi(5, 4, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn mahi_mahi_4_crash_fixed_1() {
    run_crash_variant("mahi-mahi-4", "fixed_1", mahi_mahi(1, 4, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn mahi_mahi_4_crash_fixed_5() {
    run_crash_variant("mahi-mahi-4", "fixed_5", mahi_mahi(5, 4, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn mahi_mahi_5_crash_adaptive() {
    run_crash_variant("mahi-mahi-5", "adaptive", mahi_mahi(5, 5, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn mahi_mahi_5_crash_fixed_1() {
    run_crash_variant("mahi-mahi-5", "fixed_1", mahi_mahi(1, 5, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn mahi_mahi_5_crash_fixed_5() {
    run_crash_variant("mahi-mahi-5", "fixed_5", mahi_mahi(5, 5, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn dag_hydrangea_crash_adaptive() {
    run_crash_variant("dag-hydrangea", "adaptive", dag_hydrangea(5, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn dag_hydrangea_crash_fixed_1() {
    run_crash_variant("dag-hydrangea", "fixed_1", dag_hydrangea(1, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn dag_hydrangea_crash_fixed_5() {
    run_crash_variant("dag-hydrangea", "fixed_5", dag_hydrangea(5, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn orcaella_crash_adaptive() {
    run_crash_variant("orcaella", "adaptive", orcaella(5, true), 2);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn orcaella_crash_fixed_1() {
    run_crash_variant("orcaella", "fixed_1", orcaella(1, false), 2);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh crash"]
fn orcaella_crash_fixed_5() {
    run_crash_variant("orcaella", "fixed_5", orcaella(5, false), 2);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn blue_bottle_adaptive() {
    run_variant("blue-bottle", "adaptive", blue_bottle(5, true), 1);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn blue_bottle_fixed_1() {
    run_variant("blue-bottle", "fixed_1", blue_bottle(1, false), 1);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn blue_bottle_fixed_5() {
    run_variant("blue-bottle", "fixed_5", blue_bottle(5, false), 1);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn nemo_nemo_adaptive() {
    run_variant("nemo-nemo", "adaptive", nemo_nemo(5, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn nemo_nemo_fixed_1() {
    run_variant("nemo-nemo", "fixed_1", nemo_nemo(1, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn nemo_nemo_fixed_5() {
    run_variant("nemo-nemo", "fixed_5", nemo_nemo(5, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn mahi_mahi_4_adaptive() {
    run_variant("mahi-mahi-4", "adaptive", mahi_mahi(5, 4, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn mahi_mahi_4_fixed_1() {
    run_variant("mahi-mahi-4", "fixed_1", mahi_mahi(1, 4, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn mahi_mahi_4_fixed_5() {
    run_variant("mahi-mahi-4", "fixed_5", mahi_mahi(5, 4, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn mahi_mahi_5_adaptive() {
    run_variant("mahi-mahi-5", "adaptive", mahi_mahi(5, 5, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn mahi_mahi_5_fixed_1() {
    run_variant("mahi-mahi-5", "fixed_1", mahi_mahi(1, 5, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn mahi_mahi_5_fixed_5() {
    run_variant("mahi-mahi-5", "fixed_5", mahi_mahi(5, 5, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn dag_hydrangea_adaptive() {
    run_variant("dag-hydrangea", "adaptive", dag_hydrangea(5, true), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn dag_hydrangea_fixed_1() {
    run_variant("dag-hydrangea", "fixed_1", dag_hydrangea(1, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn dag_hydrangea_fixed_5() {
    run_variant("dag-hydrangea", "fixed_5", dag_hydrangea(5, false), 3);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn orcaella_adaptive() {
    run_variant("orcaella", "adaptive", orcaella(5, true), 2);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn orcaella_fixed_1() {
    run_variant("orcaella", "fixed_1", orcaella(1, false), 2);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn orcaella_fixed_5() {
    run_variant("orcaella", "fixed_5", orcaella(5, false), 2);
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn cordial_miners_ps() {
    run_variant(
        "cordial-miners-ps",
        "default",
        ConsensusProtocol::CordialMinersPartiallySynchronous,
        3,
    );
}

#[test]
#[ignore = "long-running experiment; run via scripts/run_protocol.sh degraded"]
fn cordial_miners_async() {
    run_variant(
        "cordial-miners-async",
        "default",
        ConsensusProtocol::CordialMinersAsynchronous,
        3,
    );
}
