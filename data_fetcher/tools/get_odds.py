import os
import json
import argparse
from typing import Optional, Dict, Any
import requests

from data_fetcher.tools.get_fixture import APIFootballClient


def get_odds_by_fixture_id(fixture_id: int, bookmaker_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    client = APIFootballClient()
    url = f"{client.base_url}/odds"
    params: Dict[str, Any] = {"fixture": int(fixture_id)}
    if bookmaker_id:
        params["bookmaker"] = int(bookmaker_id)
    try:
        response = requests.get(url, headers=client.headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取赔率失败: {e}")
        return None


def _canonical_bookmaker(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    n = name.strip().lower()
    if n in {"william hill", "williamhill"}:
        return "William Hill"
    if n in {"bet365", "bet 365"}:
        return "Bet365"
    if n in {"ladbrokes", "立博"}:
        return "Ladbrokes"
    return None


def _normalize_match_winner(data: Dict[str, Any]) -> Dict[str, Any]:
    resp = data.get("response") or []
    out = {"fixture_id": None, "market": "Match Winner", "bookmakers": []}
    for item in resp:
        fx = item.get("fixture") or {}
        if out["fixture_id"] is None:
            out["fixture_id"] = fx.get("id")
        bms = item.get("bookmakers") or []
        for bm in bms:
            cid = bm.get("id")
            cname = _canonical_bookmaker(bm.get("name"))
            if not cname:
                continue
            bets = bm.get("bets") or []
            for bet in bets:
                bn = (bet.get("name") or "").strip().lower()
                if bn != "match winner":
                    continue
                vals = bet.get("values") or []
                odds: Dict[str, Any] = {}
                for v in vals:
                    lbl = (v.get("value") or "").strip().lower()
                    odd = v.get("odd")
                    if lbl in {"home", "1", "w1"}:
                        odds["home"] = odd
                    elif lbl in {"away", "2", "w2"}:
                        odds["away"] = odd
                    elif lbl in {"draw", "x"}:
                        odds["draw"] = odd
                out["bookmakers"].append({"id": cid, "name": cname, "odds": odds})
    return out


def save_json(data: Any, output_dir: str, filename: Optional[str] = None) -> Optional[str]:
    if not data:
        print("没有数据可保存")
        return None
    os.makedirs(output_dir, exist_ok=True)
    if not filename:
        filename = "odds_fixture.json"
    path = os.path.join(output_dir, filename)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"数据已保存到: {path}")
        return path
    except Exception as e:
        print(f"保存文件失败: {e}")
        return None


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(str(x))
    except Exception:
        return None


def _aggregate_match_winner(data: Dict[str, Any]) -> Dict[str, Any]:
    bms = data.get("bookmakers") or []
    home_vals = []
    away_vals = []
    draw_vals = []
    for bm in bms:
        o = bm.get("odds") or {}
        hv = _to_float(o.get("home"))
        av = _to_float(o.get("away"))
        dv = _to_float(o.get("draw"))
        if hv is not None:
            home_vals.append(hv)
        if av is not None:
            away_vals.append(av)
        if dv is not None:
            draw_vals.append(dv)

    def _mean(vals):
        return None if not vals else sum(vals) / len(vals)

    return {
        "fixture_id": data.get("fixture_id"),
        "home_odd": _mean(home_vals),
        "away_odd": _mean(away_vals),
        "draw_odd": _mean(draw_vals),
    }


def get_aggregated_match_winner_odds(fixture_id: int) -> Dict[str, Any]:
    raw = get_odds_by_fixture_id(fixture_id)
    if not raw:
        return {"fixture_id": fixture_id, "home_odd": None, "away_odd": None, "draw_odd": None}
    filtered = _normalize_match_winner(raw)
    return _aggregate_match_winner(filtered)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture-id", type=int, default=1377987)
    parser.add_argument("--bookmaker-id", type=int)
    parser.add_argument(
        "--output-dir",
        default=os.path.join(
            os.path.expanduser("~"),
            "Documents",
            "MyProjects",
            "work",
            "bc_tele",
            "data_fetcher",
            "output",
        ),
    )
    parser.add_argument("--filename")
    args = parser.parse_args()

    try:
        raw = get_odds_by_fixture_id(args.fixture_id, args.bookmaker_id)
        if not raw:
            print("获取赔率数据失败")
            return
        filtered = _normalize_match_winner(raw)
        aggregated = _aggregate_match_winner(filtered)
        fname = args.filename or f"odds_fixture_{args.fixture_id}_avg.json"
        save_json(aggregated, args.output_dir, fname)
    except Exception as e:
        print(f"程序执行出错: {e}")


if __name__ == "__main__":
    main()
