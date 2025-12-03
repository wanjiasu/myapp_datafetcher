#!/usr/bin/env python3
"""
API-Football Fixtures获取脚本
获取指定日期的足球比赛fixtures数据并保存为JSON文件
"""

import os
import json
import requests
from datetime import datetime, date as _date
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional

# 加载环境变量
load_dotenv()

class APIFootballClient:
    """API-Football客户端类"""
    
    def __init__(self):
        """初始化API客户端"""
        self.api_key = os.getenv('API_FOOTBALL_KEY') or os.getenv('X_APISPORTS_KEY') or os.getenv('APISPORTS_KEY')
        if not self.api_key:
            raise ValueError("请在.env文件中设置API_FOOTBALL_KEY")
        
        # API-Football的基础URL和请求头 <mcreference link="https://github.com/petermclagan/footballAPI" index="1">1</mcreference>
        self.base_url = "https://v3.football.api-sports.io"
        self.headers = {
            'x-apisports-key': self.api_key,
            'Accept': 'application/json'
        }
    
    def get_fixtures_by_date(self, date_str, timezone='UTC'):
        """
        根据日期获取fixtures数据
        
        Args:
            date_str (str): 日期字符串，格式为YYYY-MM-DD
            timezone (str): 时区，默认为UTC <mcreference link="https://docs.sportmonks.com/football/tutorials-and-guides/tutorials/timezone-parameters-on-different-endpoints" index="3">3</mcreference>
        
        Returns:
            dict: API响应数据
        """
        endpoint = f"/fixtures"
        url = f"{self.base_url}{endpoint}"
        
        # 设置查询参数
        params = {
            'date': date_str,
            'timezone': timezone
        }
        
        try:
            print(f"正在获取 {date_str} ({timezone}) 的fixtures数据...")
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            print(f"成功获取到 {len(data.get('response', []))} 场比赛数据")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"API请求失败: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None

    def normalize_fixture(self, fx: Dict[str, Any]) -> Dict[str, Any]:
        f = fx.get('fixture', {})
        l = fx.get('league', {})
        t = fx.get('teams', {})
        g = fx.get('goals', {})
        s = fx.get('score', {})
        v = f.get('venue') or {}
        st = f.get('status') or {}
        p = f.get('periods') or {}

        ht = s.get('halftime') or {}
        ft = s.get('fulltime') or {}
        et = s.get('extratime') or {}
        pn = s.get('penalty') or {}

        th = t.get('home') or {}
        ta = t.get('away') or {}

        row: Dict[str, Any] = {
            'fixture_id': f.get('id'),
            'fixture_date': f.get('date'),
            'fixture_timezone': f.get('timezone'),
            'fixture_timestamp': f.get('timestamp'),
            'venue_id': v.get('id'),
            'venue_name': v.get('name'),
            'venue_city': v.get('city'),
            'status_long': st.get('long'),
            'status_short': st.get('short'),
            'status_elapsed': st.get('elapsed'),
            'referee': f.get('referee'),
            'period_first': p.get('first'),
            'period_second': p.get('second'),
            'league_id': l.get('id'),
            'league_name': l.get('name'),
            'league_country': l.get('country'),
            'league_season': l.get('season'),
            'league_round': l.get('round'),
            'league_logo': l.get('logo'),
            'league_flag': l.get('flag'),
            'league_standings': l.get('standings'),
            'home_id': th.get('id'),
            'home_name': th.get('name'),
            'home_logo': th.get('logo'),
            'home_winner': th.get('winner'),
            'away_id': ta.get('id'),
            'away_name': ta.get('name'),
            'away_logo': ta.get('logo'),
            'away_winner': ta.get('winner'),
            'goals_home': g.get('home'),
            'goals_away': g.get('away'),
            'score_halftime_home': ht.get('home'),
            'score_halftime_away': ht.get('away'),
            'score_fulltime_home': ft.get('home'),
            'score_fulltime_away': ft.get('away'),
            'score_extratime_home': et.get('home'),
            'score_extratime_away': et.get('away'),
            'score_penalty_home': pn.get('home'),
            'score_penalty_away': pn.get('away'),
            'teams_vs': (th.get('name') or '') + ' VS ' + (ta.get('name') or ''),
            'raw': fx,
        }
        return row

    def normalize_response(self, fixtures_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not fixtures_data or 'response' not in fixtures_data:
            return []
        out: List[Dict[str, Any]] = []
        for fx in fixtures_data['response']:
            out.append(self.normalize_fixture(fx))
        return out
    
    def extract_fixture_info(self, fixtures_data):
        """
        从API响应中提取所需的fixture信息
        
        Args:
            fixtures_data (dict): API返回的完整数据
        
        Returns:
            list: 提取的fixture信息列表
        """
        if not fixtures_data or 'response' not in fixtures_data:
            return []
        
        extracted_fixtures = []
        
        for fixture in fixtures_data['response']:
            fixture_info = {
                'fixture_id': fixture['fixture']['id'],
                'timezone': fixture['fixture']['timezone'],
                'fixture_date': fixture['fixture']['date'],
                'venue_name': fixture['fixture']['venue']['name'] if fixture['fixture']['venue'] else None,
                'venue_city': fixture['fixture']['venue']['city'] if fixture['fixture']['venue'] else None,
                'league_id': fixture['league']['id'],
                'league_name': fixture['league']['name'],
                'league_country': fixture['league']['country'],
                'league_season': fixture['league']['season'],
                'league_round': fixture['league']['round'],
                'home_id': fixture['teams']['home']['id'],
                'home_name': fixture['teams']['home']['name'],
                'away_id': fixture['teams']['away']['id'],
                'away_name': fixture['teams']['away']['name']
            }
            extracted_fixtures.append(fixture_info)
        
        return extracted_fixtures
    
    def save_fixtures_to_json(self, fixtures_data, output_dir, filename=None):
        """
        将fixtures数据保存为JSON文件
        
        Args:
            fixtures_data (dict): fixtures数据
            output_dir (str): 输出目录路径
            filename (str): 文件名，如果为None则自动生成
        
        Returns:
            str: 保存的文件路径
        """
        if not fixtures_data:
            print("没有数据可保存")
            return None
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 生成文件名
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fixtures_{timestamp}.json"
        
        file_path = os.path.join(output_dir, filename)
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(fixtures_data, f, ensure_ascii=False, indent=2)
            
            print(f"数据已保存到: {file_path}")
            return file_path
            
        except Exception as e:
            print(f"保存文件失败: {e}")
            return None

def _pg_config() -> Optional[Dict[str, Any]]:
    u = os.getenv('POSTGRES_USER')
    p = os.getenv('POSTGRES_PASSWORD')
    h = os.getenv('POSTGRES_HOST')
    pt = os.getenv('POSTGRES_PORT')
    db = os.getenv('POSTGRES_DB')
    if not all([u, p, h, pt, db]):
        return None
    return {'user': u, 'password': p, 'host': h, 'port': int(pt), 'dbname': db}


def _pg_connect(cfg: Dict[str, Any]):
    import psycopg2
    return psycopg2.connect(**cfg)


def _pg_ensure_table(conn) -> None:
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS api_football_fixtures (
            fixture_id BIGINT PRIMARY KEY,
            fixture_date TIMESTAMPTZ,
            fixture_timezone TEXT,
            fixture_timestamp BIGINT,
            venue_id BIGINT,
            venue_name TEXT,
            venue_city TEXT,
            status_long TEXT,
            status_short TEXT,
            status_elapsed INT,
            referee TEXT,
            period_first BIGINT,
            period_second BIGINT,
            league_id INT,
            league_name TEXT,
            league_country TEXT,
            league_season INT,
            league_round TEXT,
            league_logo TEXT,
            league_flag TEXT,
            league_standings BOOLEAN,
            home_id INT,
            home_name TEXT,
            home_logo TEXT,
            home_winner BOOLEAN,
            away_id INT,
            away_name TEXT,
            away_logo TEXT,
            away_winner BOOLEAN,
            goals_home INT,
            goals_away INT,
            score_halftime_home INT,
            score_halftime_away INT,
            score_fulltime_home INT,
            score_fulltime_away INT,
            score_extratime_home INT,
            score_extratime_away INT,
            score_penalty_home INT,
            score_penalty_away INT,
            teams_vs TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    cur.execute("ALTER TABLE api_football_fixtures ADD COLUMN IF NOT EXISTS teams_vs TEXT")
    cur.execute("COMMENT ON TABLE api_football_fixtures IS 'API-Football fixtures结构化数据'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.fixture_id IS '比赛唯一ID'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.fixture_date IS '比赛日期时间(含时区偏移)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.fixture_timezone IS '比赛时间的时区标识(IANA)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.fixture_timestamp IS '比赛时间的Unix秒'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.venue_id IS '球场ID'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.venue_name IS '球场名称'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.venue_city IS '球场所在城市'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.status_long IS '比赛状态(长描述)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.status_short IS '比赛状态(短码)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.status_elapsed IS '已进行分钟数'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.referee IS '裁判姓名'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.period_first IS '上半场开始时间(Unix秒)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.period_second IS '下半场开始时间(Unix秒)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.league_id IS '赛事ID'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.league_name IS '赛事名称'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.league_country IS '赛事国家/地区'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.league_season IS '赛季年份'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.league_round IS '轮次描述'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.league_logo IS '赛事Logo URL'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.league_flag IS '赛事国旗URL(如适用)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.league_standings IS '是否存在积分榜'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.home_id IS '主队ID'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.home_name IS '主队名称'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.home_logo IS '主队Logo URL'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.home_winner IS '主队是否获胜'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.away_id IS '客队ID'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.away_name IS '客队名称'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.away_logo IS '客队Logo URL'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.away_winner IS '客队是否获胜'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.goals_home IS '主队常规/加时进球数'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.goals_away IS '客队常规/加时进球数'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.score_halftime_home IS '半场主队比分'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.score_halftime_away IS '半场客队比分'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.score_fulltime_home IS '全场主队比分(90分钟)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.score_fulltime_away IS '全场客队比分(90分钟)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.score_extratime_home IS '加时主队比分'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.score_extratime_away IS '加时客队比分'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.score_penalty_home IS '点球主队进球数'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.score_penalty_away IS '点球客队进球数'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.teams_vs IS '主客队拼接: <主队> VS <客队>'")
    cur.execute("CREATE INDEX IF NOT EXISTS api_football_fixtures_teams_vs_trgm ON api_football_fixtures USING gin (teams_vs gin_trgm_ops)")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.created_at IS '创建时间(插入时间)'")
    cur.execute("COMMENT ON COLUMN api_football_fixtures.updated_at IS '更新时间(最后刷新时间)'")
    conn.commit()
    cur.close()


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _pg_upsert(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    cols = [
        'fixture_id','fixture_date','fixture_timezone','fixture_timestamp','venue_id','venue_name','venue_city',
        'status_long','status_short','status_elapsed','referee','period_first','period_second','league_id','league_name',
        'league_country','league_season','league_round','league_logo','league_flag','league_standings','home_id','home_name',
        'home_logo','home_winner','away_id','away_name','away_logo','away_winner','goals_home','goals_away',
        'score_halftime_home','score_halftime_away','score_fulltime_home','score_fulltime_away','score_extratime_home',
        'score_extratime_away','score_penalty_home','score_penalty_away','teams_vs'
    ]
    cur = conn.cursor()
    count = 0
    for r in rows:
        r = dict(r)
        r['fixture_date'] = _parse_dt(r.get('fixture_date'))
        values = [r.get(c) for c in cols]
        placeholders = ",".join(["%s"]*len(cols))
        cur.execute(
            f"""
            INSERT INTO api_football_fixtures ({','.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (fixture_id) DO UPDATE SET
            fixture_date=EXCLUDED.fixture_date,
            fixture_timezone=EXCLUDED.fixture_timezone,
            fixture_timestamp=EXCLUDED.fixture_timestamp,
            venue_id=EXCLUDED.venue_id,
            venue_name=EXCLUDED.venue_name,
            venue_city=EXCLUDED.venue_city,
            status_long=EXCLUDED.status_long,
            status_short=EXCLUDED.status_short,
            status_elapsed=EXCLUDED.status_elapsed,
            referee=EXCLUDED.referee,
            period_first=EXCLUDED.period_first,
            period_second=EXCLUDED.period_second,
            league_id=EXCLUDED.league_id,
            league_name=EXCLUDED.league_name,
            league_country=EXCLUDED.league_country,
            league_season=EXCLUDED.league_season,
            league_round=EXCLUDED.league_round,
            league_logo=EXCLUDED.league_logo,
            league_flag=EXCLUDED.league_flag,
            league_standings=EXCLUDED.league_standings,
            home_id=EXCLUDED.home_id,
            home_name=EXCLUDED.home_name,
            home_logo=EXCLUDED.home_logo,
            home_winner=EXCLUDED.home_winner,
            away_id=EXCLUDED.away_id,
            away_name=EXCLUDED.away_name,
            away_logo=EXCLUDED.away_logo,
            away_winner=EXCLUDED.away_winner,
            goals_home=EXCLUDED.goals_home,
            goals_away=EXCLUDED.goals_away,
            score_halftime_home=EXCLUDED.score_halftime_home,
            score_halftime_away=EXCLUDED.score_halftime_away,
            score_fulltime_home=EXCLUDED.score_fulltime_home,
            score_fulltime_away=EXCLUDED.score_fulltime_away,
            score_extratime_home=EXCLUDED.score_extratime_home,
            score_extratime_away=EXCLUDED.score_extratime_away,
            score_penalty_home=EXCLUDED.score_penalty_home,
            score_penalty_away=EXCLUDED.score_penalty_away,
            teams_vs=EXCLUDED.teams_vs,
            updated_at=now()
            """,
            values
        )
        count += 1
    conn.commit()
    cur.close()
    return count


def _pg_set_similarity_limit(conn, limit: float) -> None:
    cur = conn.cursor()
    cur.execute("SELECT set_limit(%s)", (float(limit),))
    conn.commit()
    cur.close()


def _pg_search_teams_vs_similarity(conn, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT fixture_id, teams_vs, league_name, fixture_date, similarity(teams_vs, %s) AS sim
        FROM api_football_fixtures
        WHERE teams_vs % %s
        ORDER BY sim DESC
        LIMIT %s
        """,
        (keyword, keyword, int(limit),)
    )
    rows = cur.fetchall()
    cur.close()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            'fixture_id': r[0],
            'teams_vs': r[1],
            'league_name': r[2],
            'fixture_date': r[3].isoformat() if r[3] else None,
            'similarity': r[4],
        })
    return out


def _pg_search_teams_vs_fuzzy(conn, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT fixture_id, teams_vs, league_name, fixture_date
        FROM api_football_fixtures
        WHERE teams_vs ILIKE %s
        ORDER BY fixture_date DESC NULLS LAST
        LIMIT %s
        """,
        (f"%{keyword}%", int(limit),)
    )
    rows = cur.fetchall()
    cur.close()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            'fixture_id': r[0],
            'teams_vs': r[1],
            'league_name': r[2],
            'fixture_date': r[3].isoformat() if r[3] else None,
        })
    return out


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--date', default=_date.today().isoformat())
    p.add_argument('--timezone', default='UTC')
    p.add_argument('--output-dir', default=os.path.join(os.path.expanduser('~'), 'Documents', 'MyProjects', 'work', 'bc_tele', 'output'))
    p.add_argument('--filename')
    p.add_argument('--write-pg', action='store_true')
    args = p.parse_args()

    try:
        client = APIFootballClient()
        fixtures_data = client.get_fixtures_by_date(args.date, timezone=args.timezone)
        if not fixtures_data:
            print('获取fixtures数据失败')
            return
        rows = client.normalize_response(fixtures_data)
        saved_file = client.save_fixtures_to_json(rows, args.output_dir, args.filename or f"fixtures_{args.date}_{args.timezone.replace('/', '_')}.json")
        if args.write_pg:
            cfg = _pg_config()
            if not cfg:
                print('PostgreSQL配置缺失')
            else:
                try:
                    conn = _pg_connect(cfg)
                    _pg_ensure_table(conn)
                    n = _pg_upsert(conn, rows)
                    conn.close()
                    print(f'写入PostgreSQL: {n} 条')
                except Exception as e:
                    print(f'写入PostgreSQL失败: {e}')
        if saved_file:
            print(f'JSON文件: {saved_file}')
    except Exception as e:
        print(f'程序执行出错: {e}')

if __name__ == "__main__":
    main()
