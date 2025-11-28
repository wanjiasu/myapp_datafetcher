import argparse
from typing import Optional, List, Tuple
import sys
from pathlib import Path

try:
    from data_fetcher.tools.get_fixture import _pg_config, _pg_connect
    from data_fetcher.tools.get_odds import get_aggregated_match_winner_odds
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from data_fetcher.tools.get_fixture import _pg_config, _pg_connect
    from data_fetcher.tools.get_odds import get_aggregated_match_winner_odds


def _ensure_columns(conn) -> None:
    cur = conn.cursor()
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='ai_eval' AND column_name IN ('home_odd','away_odd','draw_odd')")
    cols = {r[0]: r[1] for r in cur.fetchall()}
    for c in ("home_odd", "away_odd", "draw_odd"):
        if c not in cols:
            cur.execute(f"ALTER TABLE ai_eval ADD COLUMN {c} TEXT")
        else:
            dt = cols[c]
            if dt not in ("text", "character varying", "varchar"):
                cur.execute(f"ALTER TABLE ai_eval ALTER COLUMN {c} TYPE TEXT USING {c}::text")
    conn.commit()
    cur.close()


def _rows_to_update(conn, fixture_id: Optional[int], limit: Optional[int]) -> List[int]:
    cur = conn.cursor()
    if fixture_id is not None:
        cur.execute("SELECT fixture_id FROM ai_eval WHERE fixture_id=%s", (fixture_id,))
    else:
        sql = """
        SELECT fixture_id
        FROM ai_eval
        WHERE fixture_id IS NOT NULL
          AND (
            home_odd IS NULL OR home_odd='' OR
            away_odd IS NULL OR away_odd='' OR
            draw_odd IS NULL OR draw_odd=''
          )
        ORDER BY fixture_id ASC
        """
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur.execute(sql)
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    return rows


def _fmt(v: Optional[float]) -> str:
    return "未找到赔率" if v is None else ("%g" % v)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--fixture-id", type=int)
    p.add_argument("--limit", type=int)
    args = p.parse_args()

    cfg = _pg_config()
    if not cfg:
        print("PostgreSQL配置缺失")
        return
    conn = _pg_connect(cfg)
    try:
        _ensure_columns(conn)
        rows = _rows_to_update(conn, args.fixture_id, args.limit)
        if not rows:
            print("没有需要更新的记录")
            return
        cur = conn.cursor()
        for fx in rows:
            agg = get_aggregated_match_winner_odds(int(fx))
            hv = _fmt(agg.get("home_odd"))
            av = _fmt(agg.get("away_odd"))
            dv = _fmt(agg.get("draw_odd"))
            cur.execute(
                "UPDATE ai_eval SET home_odd=%s, away_odd=%s, draw_odd=%s WHERE fixture_id=%s",
                (hv, av, dv, fx),
            )
        conn.commit()
        cur.close()
        print(f"更新完成: {len(rows)} 条")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
