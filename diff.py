"""
Compute differences between two ETF portfolio snapshots.
"""


def _pct_change(start: float | int | None, end: float | int | None) -> float | None:
    if start is None or end is None or start == 0:
        return None
    return round((end - start) / abs(start), 6)


def diff_portfolios(start: dict, end: dict) -> dict:
    """
    Returns a structured diff between two snapshots.
    Both `start` and `end` are the dicts produced by parser.parse_file().
    """

    # --- Fund assets diff ---
    sa = start["fund_assets"]
    ea = end["fund_assets"]
    assets_diff = {
        "nav_total_ntd": {
            "start": sa["nav_total_ntd"],
            "end": ea["nav_total_ntd"],
            "change": (ea["nav_total_ntd"] or 0) - (sa["nav_total_ntd"] or 0),
            "pct_change": _pct_change(sa["nav_total_ntd"], ea["nav_total_ntd"]),
        },
        "units_outstanding": {
            "start": sa["units_outstanding"],
            "end": ea["units_outstanding"],
            "change": ea["units_outstanding"] - sa["units_outstanding"],
            "pct_change": _pct_change(sa["units_outstanding"], ea["units_outstanding"]),
        },
        "nav_per_unit_ntd": {
            "start": sa["nav_per_unit_ntd"],
            "end": ea["nav_per_unit_ntd"],
            "change": round(ea["nav_per_unit_ntd"] - sa["nav_per_unit_ntd"], 4),
            "pct_change": _pct_change(sa["nav_per_unit_ntd"], ea["nav_per_unit_ntd"]),
        },
    }

    # --- Asset allocation diff ---
    saa = start["asset_allocation"]
    eaa = end["asset_allocation"]
    allocation_diff = {}
    for key in saa:
        sv, ev = saa[key], eaa[key]
        if sv is None and ev is None:
            allocation_diff[key] = {"start": None, "end": None, "change": None, "pct_change": None}
        elif isinstance(sv, float) or isinstance(ev, float):
            sv = sv or 0.0
            ev = ev or 0.0
            allocation_diff[key] = {
                "start": sv,
                "end": ev,
                "change": round(ev - sv, 6),
                "pct_change": _pct_change(sv, ev),
            }
        else:
            sv = sv or 0
            ev = ev or 0
            allocation_diff[key] = {
                "start": sv,
                "end": ev,
                "change": ev - sv,
                "pct_change": _pct_change(sv, ev),
            }

    # --- Stock holdings diff ---
    start_stocks = {s["code"]: s for s in start["stocks"]}
    end_stocks = {s["code"]: s for s in end["stocks"]}

    all_codes = set(start_stocks) | set(end_stocks)
    added, removed, changed, unchanged = [], [], [], []

    for code in sorted(all_codes):
        s = start_stocks.get(code)
        e = end_stocks.get(code)
        if s is None:
            added.append({**e, "status": "added"})
        elif e is None:
            removed.append({**s, "status": "removed"})
        else:
            shares_change = e["shares"] - s["shares"]
            weight_change = round((e["weight"] or 0) - (s["weight"] or 0), 6)
            if shares_change != 0 or weight_change != 0:
                changed.append({
                    "code": code,
                    "name": e["name"],
                    "start_shares": s["shares"],
                    "end_shares": e["shares"],
                    "shares_change": shares_change,
                    "start_weight": s["weight"],
                    "end_weight": e["weight"],
                    "weight_change": weight_change,
                    "status": "changed",
                })
            else:
                unchanged.append({
                    "code": code,
                    "name": e["name"],
                    "shares": e["shares"],
                    "weight": e["weight"],
                    "status": "unchanged",
                })

    return {
        "start_date": start["date"],
        "end_date": end["date"],
        "fund_assets": assets_diff,
        "asset_allocation": allocation_diff,
        "stocks": {
            "summary": {
                "added_count": len(added),
                "removed_count": len(removed),
                "changed_count": len(changed),
                "unchanged_count": len(unchanged),
                "start_total": len(start_stocks),
                "end_total": len(end_stocks),
            },
            "added": added,
            "removed": removed,
            "changed": changed,
            "unchanged": unchanged,
        },
    }
