"""Count HYPE holders on Hyperliquid by balance tier.

Source: Hypurrscan public API, GET https://api.hypurrscan.io/holders/HYPE
(returns {"token", "lastUpdate", "holdersCount", "holders": {address: balance}}).

Scope: HyperCore spot balances only. Staked (delegated) HYPE is NOT included,
and addresses that only stake don't appear in the data at all.
Run again at any time to refresh the numbers.
"""
import csv
import datetime
import json
import sys
import time
import urllib.request

HOLDERS_URL = "https://api.hypurrscan.io/holders/HYPE"
TIERS = [10, 100, 1_000, 10_000]
CSV_PATH = "hype_holders.csv"


def fetch_holders(retries=4):
    """Download the holders snapshot, retrying on network errors."""
    for i in range(retries):
        try:
            with urllib.request.urlopen(HOLDERS_URL, timeout=120) as r:
                return json.load(r)
        except Exception as e:
            if i == retries - 1:
                raise
            print(f"Request failed ({e}); retrying in {2 ** (i + 1)}s", file=sys.stderr)
            time.sleep(2 ** (i + 1))


def main():
    data = fetch_holders()
    holders = data.get("holders")
    # Stop if the response format has changed instead of guessing
    if not isinstance(holders, dict) or not holders:
        sys.exit(f"Unexpected response format, top-level keys: {list(data)}")

    snapshot = datetime.datetime.fromtimestamp(data["lastUpdate"], datetime.timezone.utc)
    balances = sorted(holders.items(), key=lambda x: -x[1])
    total = len(balances)

    # Save each address's balance, largest first
    with open(CSV_PATH, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["address", "spot_hype"])
        w.writerows(balances)

    print(f"Snapshot: {snapshot:%Y-%m-%d %H:%M} UTC   Total holder addresses: {total:,}\n")
    print(f"{'Tier':<12}{'Addresses':>10}{'Share':>9}")
    print("-" * 31)
    for t in TIERS:
        n = sum(1 for _, b in balances if b >= t)
        print(f"{'>= ' + format(t, ','):<12}{n:>10,}{n / total:>9.2%}")

    print("\nSegmented distribution")
    print("-" * 31)
    bounds = [0] + TIERS + [float("inf")]
    for lo, hi in zip(bounds, bounds[1:]):
        n = sum(1 for _, b in balances if lo <= b < hi)
        label = f">= {lo:,}" if hi == float("inf") else f"{lo:,}-{hi:,}"
        print(f"{label:<12}{n:>10,}{n / total:>9.2%}")

    print(
        "\nScope: HyperCore spot balances only, excluding staked HYPE, HyperEVM-side balances"
        " and exchange custody. Counts are addresses, not people (one person may hold several"
        " addresses). The denominator includes a lot of dust addresses, so the shares look small."
    )


if __name__ == "__main__":
    main()
