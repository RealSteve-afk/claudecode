"""Count HYPE holders on Hyperliquid by balance tier (spot + staked).

Data sources:
  - Spot balances: Hypurrscan GET https://api.hypurrscan.io/holders/HYPE
    ({"token", "lastUpdate", "holdersCount", "holders": {address: balance}}).
    Spot only; addresses that only stake are absent.
  - Candidate staker addresses: Hypurrscan GET /allDelegations
    (a delegate/undelegate event log, not balances; used only to find addresses).
  - Staked balances: official POST https://api.hyperliquid.xyz/info
    {"type": "delegatorSummary", "user": address}, queried one address at a time.
    Staked = delegated + undelegated + totalPendingWithdrawal.

Addresses queried: every address in the delegation log + every holder with
spot >= MIN_SPOT_TO_QUERY.

Usage:
  python3 hype_holders.py              # full run (~3-4 hours; resumes after an interruption)
  python3 hype_holders.py --spot-only  # spot-only stats, finishes in seconds
  python3 hype_holders.py --refresh    # discard the cached snapshot and start over

Outputs:
  hype_holders.csv                 latest per-address details (address, spot, staked, total)
  data/<date>/hype_holders.csv     per-address details archived for that snapshot
  data/<date>/summary.json         tier counts, shares, top 20, and scope for that snapshot
  data/history.csv                 one tier-summary row per run, for comparing over time
"""
import argparse
import csv
import datetime
import json
import os
import shutil
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HYPURRSCAN = "https://api.hypurrscan.io"
INFO_URL = "https://api.hyperliquid.xyz/info"
TIERS = [10, 100, 1_000, 10_000]
MIN_SPOT_TO_QUERY = 0.01
RATE_PER_SEC = 15  # measured ceiling is ~20/s; leave some headroom
WORKERS = 8
CACHE_DIR = ".cache"
STAKE_CACHE = os.path.join(CACHE_DIR, "stake.jsonl")
CSV_PATH = "hype_holders.csv"
# Each run is archived to data/<snapshot date>/, and data/history.csv gets one row per run
DATA_DIR = "data"
HISTORY_PATH = os.path.join(DATA_DIR, "history.csv")
# System/protocol addresses, reported separately in the summary
SYSTEM_ADDRESSES = {
    "0x2222222222222222222222222222222222222222": "HyperEVM bridge",
    "0xfefefefefefefefefefefefefefefefefefefefe": "Assistance Fund",
}


def http_json(url, body=None, timeout=300, retries=6):
    """GET/POST JSON with exponential backoff on 429, 5xx, and network errors."""
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    for i in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code != 429 and e.code < 500:
                raise
            err = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            err = e
        if i == retries - 1:
            raise err
        time.sleep(2 ** (i + 1))


def cached_fetch(name, url, refresh):
    """Cache Hypurrscan snapshots locally so a resumed run uses the same snapshot."""
    path = os.path.join(CACHE_DIR, name)
    if not refresh and os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    data = http_json(url)
    with open(path, "w") as f:
        json.dump(data, f)
    return data


class RateLimiter:
    """Simple rate limiter shared across threads."""

    def __init__(self, per_sec):
        self.interval = 1 / per_sec
        self.next = time.monotonic()
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            self.next = max(self.next + self.interval, now)
            delay = self.next - now
        if delay > 0:
            time.sleep(delay)


def query_stakes(addresses):
    """Query delegatorSummary for each address, appending results to a JSONL cache (resumable)."""
    done = {}
    if os.path.exists(STAKE_CACHE):
        with open(STAKE_CACHE) as f:
            for line in f:
                rec = json.loads(line)
                done[rec["user"]] = rec["staked"]
    todo = [a for a in addresses if a not in done]
    print(f"Staking queries: {len(addresses):,} total, {len(done):,} cached, {len(todo):,} to go", flush=True)

    limiter = RateLimiter(RATE_PER_SEC)
    lock = threading.Lock()
    start = time.time()
    count = 0

    def work(addr):
        nonlocal count
        limiter.wait()
        s = http_json(INFO_URL, {"type": "delegatorSummary", "user": addr}, timeout=30)
        # Stop if the fields change instead of guessing
        if not isinstance(s, dict) or not {"delegated", "undelegated", "totalPendingWithdrawal"} <= s.keys():
            raise RuntimeError(f"Unexpected delegatorSummary format for {addr}: {s!r}")
        staked = float(s["delegated"]) + float(s["undelegated"]) + float(s["totalPendingWithdrawal"])
        with lock:
            out.write(json.dumps({"user": addr, "staked": staked}) + "\n")
            done[addr] = staked
            count += 1
            if count % 5000 == 0:
                out.flush()
                rate = count / (time.time() - start)
                eta = (len(todo) - count) / rate / 60
                print(f"  {count:,}/{len(todo):,}  {rate:.1f}/s  about {eta:.0f} min left", flush=True)

    with open(STAKE_CACHE, "a") as out, ThreadPoolExecutor(WORKERS) as ex:
        # list() re-raises any exception from a worker
        list(ex.map(work, todo))
    return done


def print_table(title, totals):
    n_all = len(totals)
    print(f"\n{title} (denominator: {n_all:,} addresses with a balance)")
    print(f"{'Tier':<14}{'Addresses':>10}{'Share':>9}")
    print("-" * 33)
    for t in TIERS:
        n = sum(1 for v in totals if v >= t)
        print(f"{'>= ' + format(t, ','):<14}{n:>10,}{n / n_all:>9.2%}")
    bounds = [0] + TIERS + [float("inf")]
    print("  segments:")
    for lo, hi in zip(bounds, bounds[1:]):
        n = sum(1 for v in totals if lo <= v < hi)
        label = f">= {lo:,}" if hi == float("inf") else f"{lo:,}-{hi:,}"
        print(f"  {label:<12}{n:>10,}{n / n_all:>9.2%}")


def tier_stats(values):
    """Count addresses and shares for each cumulative tier."""
    n_all = len(values)
    return {
        "denominator": n_all,
        "tiers": {str(t): {"count": (n := sum(1 for v in values if v >= t)), "pct": round(n / n_all * 100, 4)}
                  for t in TIERS},
    }


def archive(snapshot, rows, mode, scope):
    """Save this run's results to data/<date>/ and update data/history.csv."""
    run_dir = os.path.join(DATA_DIR, f"{snapshot:%Y-%m-%d}")
    os.makedirs(run_dir, exist_ok=True)
    shutil.copyfile(CSV_PATH, os.path.join(run_dir, "hype_holders.csv"))

    spot_stats = tier_stats([r[1] for r in rows if r[1] > 0])
    total_stats = tier_stats([r[3] for r in rows]) if mode == "spot+staked" else None
    summary = {
        "spot_snapshot_utc": snapshot.isoformat(),
        "generated_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "mode": mode,
        "spot_only": spot_stats,
        "spot_plus_staked": total_stats,
        "gte_1000_excluding_system": (sum(1 for r in rows if r[3] >= 1000 and r[0] not in SYSTEM_ADDRESSES)
                                      if total_stats else None),
        "system_addresses": SYSTEM_ADDRESSES,
        "top20": [{"address": a, "spot": s, "staked": k, "total": t, "tag": SYSTEM_ADDRESSES.get(a, "")}
                  for a, s, k, t in rows[:20]],
        "min_spot_queried_for_stake": MIN_SPOT_TO_QUERY,
        "scope": scope,
    }
    with open(os.path.join(run_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # One row per snapshot date; rerunning the same day replaces that row
    fields = ["date", "spot_snapshot_utc", "mode", "spot_holders"] + [f"spot_gte_{t}" for t in TIERS] + \
             ["total_holders"] + [f"total_gte_{t}" for t in TIERS]
    row = {"date": f"{snapshot:%Y-%m-%d}", "spot_snapshot_utc": summary["spot_snapshot_utc"], "mode": mode,
           "spot_holders": spot_stats["denominator"],
           **{f"spot_gte_{t}": spot_stats["tiers"][str(t)]["count"] for t in TIERS}}
    if total_stats:
        row["total_holders"] = total_stats["denominator"]
        row.update({f"total_gte_{t}": total_stats["tiers"][str(t)]["count"] for t in TIERS})
    history = []
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH) as f:
            history = [r for r in csv.DictReader(f) if r["date"] != row["date"]]
    history.append(row)
    history.sort(key=lambda r: r["date"])
    with open(HISTORY_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(history)
    print(f"\nArchived to {run_dir}/ and updated {HISTORY_PATH}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spot-only", action="store_true", help="skip staking queries")
    ap.add_argument("--refresh", action="store_true", help="refetch snapshots and clear the staking cache")
    args = ap.parse_args()

    os.makedirs(CACHE_DIR, exist_ok=True)
    if args.refresh and os.path.exists(STAKE_CACHE):
        os.remove(STAKE_CACHE)

    data = cached_fetch("holders.json", f"{HYPURRSCAN}/holders/HYPE", args.refresh)
    spot = data.get("holders")
    if not isinstance(spot, dict) or not spot:
        sys.exit(f"Unexpected holders format, top-level keys: {list(data)}")
    snapshot = datetime.datetime.fromtimestamp(data["lastUpdate"], datetime.timezone.utc)
    print(f"Spot snapshot: {snapshot:%Y-%m-%d %H:%M} UTC, {len(spot):,} addresses")

    staked = {}
    if not args.spot_only:
        events = cached_fetch("allDelegations.json", f"{HYPURRSCAN}/allDelegations", args.refresh)
        if not isinstance(events, list) or not events or "user" not in events[0]:
            sys.exit("Unexpected allDelegations format")
        log_users = {e["user"] for e in events}
        to_query = sorted(log_users | {a for a, b in spot.items() if b >= MIN_SPOT_TO_QUERY})
        staked = query_stakes(to_query)

    # Merge by address; keep only addresses with a balance
    rows = []
    for addr in set(spot) | set(staked):
        s, k = spot.get(addr, 0.0), staked.get(addr, 0.0)
        if s + k > 0:
            rows.append((addr, s, k, s + k))
    rows.sort(key=lambda r: -r[3])

    with open(CSV_PATH, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["address", "spot_hype", "staked_hype", "total_hype"])
        for addr, s, k, t in rows:
            w.writerow([addr, f"{s:.8f}", f"{k:.8f}" if not args.spot_only else "", f"{t:.8f}"])

    print("\nTop 20 addresses by total balance:")
    for i, (addr, s, k, t) in enumerate(rows[:20], 1):
        tag = SYSTEM_ADDRESSES.get(addr, "")
        print(f"{i:>2} {addr} total {t:>14,.0f}  spot {s:>13,.0f}  staked {k:>13,.0f}  {tag}")

    print_table("Spot only", [r[1] for r in rows if r[1] > 0])
    if not args.spot_only:
        print_table("Spot + staked", [r[3] for r in rows])
        excl = sum(1 for r in rows if r[3] >= 1000 and r[0] not in SYSTEM_ADDRESSES)
        print(f"\n>=1,000 excluding system addresses (HyperEVM bridge / Assistance Fund): {excl:,}")

    scope = (
        "HyperCore spot + staked (delegated + undelegated + unstaking in progress);"
        " excludes HyperEVM-side balances and exchange custody. Counts are addresses, not people."
        " An address that never appears in the delegation log and holds under"
        f" {MIN_SPOT_TO_QUERY} spot HYPE is not queried, so any stake it has is missed."
    )
    print("\nScope: " + scope)
    archive(snapshot, rows, "spot_only" if args.spot_only else "spot+staked", scope)

if __name__ == "__main__":
    main()
