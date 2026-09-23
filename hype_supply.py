"""Break down HYPE circulating supply by where it actually sits.

Depends on hype_holders.py having produced hype_holders.csv (spot + staked per address) first.

Data sources:
  - Official tokenDetails (POST https://api.hyperliquid.xyz/info): total supply, circulating supply,
    future emissions, and the officially non-circulating addresses
  - hype_holders.csv: spot and staked HYPE per address
  - Hypurrscan /fullUnstakingQueue: withdrawals still in the 7-day unstaking period

Output: printed breakdown, plus data/<date>/supply.json and one row upserted into data/supply_history.csv.
Usage: python3 hype_supply.py
"""
import csv
import datetime
import json
import os
import time
import urllib.request

INFO_URL = "https://api.hyperliquid.xyz/info"
HYPE_TOKEN_ID = "0x0d01dc56dcaaca66ad901c959b4011ec"
CSV_PATH = "hype_holders.csv"
DATA_DIR = "data"
HISTORY_PATH = os.path.join(DATA_DIR, "supply_history.csv")
EVM_BRIDGE = "0x2222222222222222222222222222222222222222"
# Tagged "Hyper Foundation" in Hypurrscan globalAliases; officially counted as circulating
FOUNDATION = "0xd57ecca444a9acb7208d286be439de12dd09de5d"


def http_json(url, body=None, retries=5):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    for i in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=300) as r:
                return json.load(r)
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(2 ** (i + 1))


def main():
    td = http_json(INFO_URL, {"type": "tokenDetails", "tokenId": HYPE_TOKEN_ID})
    max_s, total = float(td["maxSupply"]), float(td["totalSupply"])
    circ, future = float(td["circulatingSupply"]), float(td["futureEmissions"])
    noncirc = {a.lower(): float(b) for a, b in td["nonCirculatingUserBalances"]}
    price = float(td["markPx"])

    # Circulating addresses only (drop the officially non-circulating ones)
    spot = staked = found_spot = found_staked = evm = 0.0
    with open(CSV_PATH) as f:
        for r in csv.DictReader(f):
            a = r["address"]
            if a in noncirc:
                continue
            s, k = float(r["spot_hype"]), float(r["staked_hype"] or 0)
            if a == EVM_BRIDGE:
                evm = s
            elif a == FOUNDATION:
                found_spot, found_staked = s, k
            else:
                spot += s
                staked += k

    # Withdrawals still unstaking (become tradable within 7 days)
    now_ms = time.time() * 1000
    queue = http_json("https://api.hypurrscan.io/fullUnstakingQueue")
    unstaking = sum(e["wei"] for e in queue if e.get("time") and e["time"] > now_ms) / 1e8

    # Spot that official circulating implies but Hypurrscan's holder list can't see (addresses it doesn't track)
    untracked = circ - (spot + staked + evm + found_spot + found_staked)
    liquid = spot + evm + untracked
    m = lambda x: round(x / 1e6, 3)
    result = {
        "generated_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "mark_price_usd": price,
        "supply_m": {
            "max": m(max_s),
            "burned": m(max_s - total),
            "total": m(total),
            "future_emissions": m(future),
            "non_circulating_addresses": {a: m(b) for a, b in noncirc.items()},
            "official_circulating": m(circ),
        },
        "official_circulating_breakdown_m": {
            "hyper_foundation": m(found_spot + found_staked),
            "staked_by_others": m(staked),
            "  of_which_unstaking_within_7d": m(unstaking),
            "hyperevm_bridge": m(evm),
            "hypercore_spot_tracked": m(spot),
            "hypercore_spot_untracked_residual": m(untracked),
        },
        "views_m": {
            "official_circulating": m(circ),
            "excluding_foundation": m(circ - found_spot - found_staked),
            "liquid_not_staked_excluding_foundation": m(liquid),
        },
        "notes": "untracked_residual = official circulating minus every balance observed at an address; "
                 "hyperevm_bridge includes HYPE locked in EVM DeFi/LPs; spot and staked snapshots differ by a few hours.",
    }

    snap_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    run_dir = os.path.join(DATA_DIR, snap_date)
    os.makedirs(run_dir, exist_ok=True)
    with open(os.path.join(run_dir, "supply.json"), "w") as f:
        json.dump(result, f, indent=2)

    row = {"date": snap_date, "price": price, **{k: v for k, v in result["views_m"].items()},
           "future_emissions": m(future), "assistance_fund": m(noncirc.get("0xfefefefefefefefefefefefefefefefefefefefe", 0)),
           "staked_by_others": m(staked), "hyperevm": m(evm), "untracked_residual": m(untracked)}
    hist = []
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH) as f:
            hist = [r for r in csv.DictReader(f) if r["date"] != snap_date]
    hist.append(row)
    with open(HISTORY_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row))
        w.writeheader()
        w.writerows(sorted(hist, key=lambda r: r["date"]))

    print(f"Price ${price:,.2f}   (M HYPE)")
    for section in ("supply_m", "official_circulating_breakdown_m", "views_m"):
        print(f"\n[{section}]")
        for k, v in result[section].items():
            if isinstance(v, dict):
                for a, b in v.items():
                    print(f"  {k} {a}: {b:,.2f}")
            else:
                usd = f"   ${v * price / 1e3:,.2f}B" if section == "views_m" else ""
                print(f"  {k:<42}{v:>10,.2f}{usd}")
    print(f"\nSaved to {run_dir}/supply.json and {HISTORY_PATH}")


if __name__ == "__main__":
    main()
