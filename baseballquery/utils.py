import pandas as pd
from pathlib import Path
from .database import engine
import sqlalchemy
from sqlalchemy import text

def get_year_events(year: int) -> pd.DataFrame:
    with engine.connect() as conn:
                return pd.read_sql("""
                            SELECT *
                           FROM events WHERE year = ?
                        """, conn, params=(year,))
        # return pd.read_sql("""
        #                     SELECT
        #                         events.GAME_ID,
        #                         AWAY_TEAM_ID,
        #                         INN_CT,
        #                         OUTS_CT,
        #                         BALLS_CT,
        #                         STRIKES_CT,
        #                         AWAY_SCORE_CT,
        #                         HOME_SCORE_CT,
        #                         RESP_BAT_ID,
        #                         RESP_BAT_HAND_CD,
        #                         RESP_PIT_ID,
        #                         RESP_PIT_HAND_CD,
        #                         BASE1_RUN_ID,
        #                         BASE2_RUN_ID,
        #                         BASE3_RUN_ID,
        #                         BAT_FLD_CD,
        #                         BAT_LINEUP_ID,
        #                         EVENT_CD,
        #                         AB_FL,
        #                         H_CD,
        #                         SH_FL,
        #                         SF_FL,
        #                         EVENT_OUTS_CT,
        #                         DP_FL,
        #                         TP_FL,
        #                         RBI_CT,
        #                         WP_FL,
        #                         PB_FL,
        #                         BATTEDBALL_CD,
        #                         BAT_DEST_ID,
        #                         RUN1_DEST_ID,
        #                         RUN2_DEST_ID,
        #                         RUN3_DEST_ID,
        #                         RUN1_SB_FL,
        #                         RUN2_SB_FL,
        #                         RUN3_SB_FL,
        #                         RUN1_CS_FL,
        #                         RUN2_CS_FL,
        #                         RUN3_CS_FL,
        #                         RUN1_PK_FL,
        #                         RUN2_PK_FL,
        #                         RUN3_PK_FL,
        #                         RUN1_RESP_PIT_ID,
        #                         RUN2_RESP_PIT_ID,
        #                         RUN3_RESP_PIT_ID,
        #                         HOME_TEAM_ID,
        #                         BAT_TEAM_ID,
        #                         FLD_TEAM_ID,
        #                         PA_TRUNC_FL,
        #                         START_BASES_CD,
        #                         END_BASES_CD,
        #                         RESP_BAT_START_FL,
        #                         RESP_PIT_START_FL,
        #                         PA_BALL_CT,
        #                         PA_OTHER_BALL_CT,
        #                         PA_STRIKE_CT,
        #                         PA_OTHER_STRIKE_CT,
        #                         EVENT_RUNS_CT,
        #                         BAT_SAFE_ERR_FL,
        #                         FATE_RUNS_CT,
        #                         MLB_STATSAPI_APPROX,
        #                         mlbam_id,
        #                         PA,
        #                         AB,
        #                         SH,
        #                         SF,
        #                         R,
        #                         RBI,
        #                         SB,
        #                         CS,
        #                         K,
        #                         BK,
        #                         UBB,
        #                         IBB,
        #                         HBP,
        #                         FC,
        #                         "1B",
        #                         "2B",
        #                         "3B",
        #                         HR,
        #                         H,
        #                         DP,
        #                         TP,
        #                         ROE,
        #                         WP,
        #                         P,
        #                         GB,
        #                         FB,
        #                         LD,
        #                         PU,
        #                         ER,
        #                         T_UER,
        #                         UER,
        #                         year,
        #                         month,
        #                         day,
        #                         file_index,
        #                         GAME_DY,
        #                         START_GAME_TM,
        #                         DAYNIGHT_PARK_CD,
        #                         PARK_ID,
        #                         ATTEND_PARK_CT,
        #                         TEMP_PARK_CT,
        #                         WIND_DIRECTION_PARK_CD,
        #                         WIND_SPEED_PARK_CT,
        #                         FIELD_PARK_CD,
        #                         PRECIP_PARK_CD,
        #                         SKY_PARK_CD,
        #                         MINUTES_GAME_CT,
        #                         WIN_PIT_ID,
        #                         LOSE_PIT_ID,
        #                         SAVE_PIT_ID
        #                    FROM events JOIN cwgame ON events.GAME_ID = cwgame.GAME_ID WHERE year = ?
        #                 """, conn, params=(year,))

def delete_year_events(year: int):
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM baserunning WHERE baserunning.GAME_ID IN (SELECT events.GAME_ID FROM events WHERE year = {year})"))
        conn.execute(text(f"DELETE FROM pitching_runs WHERE pitching_runs.GAME_ID IN (SELECT events.GAME_ID FROM events WHERE year = {year})"))
        conn.execute(text(f"DELETE FROM cwgame WHERE cwgame.GAME_ID IN (SELECT events.GAME_ID FROM events WHERE year = {year})"))
        conn.execute(text(f"DELETE FROM linear_weights WHERE year = {year}"))
        conn.execute(text(f"DELETE FROM events WHERE year = {year}"))

    # Remove the year from the years.txt file
    data_dir = Path("~/.baseballquery").expanduser()
    if (data_dir / "years.txt").exists():
        with open(data_dir / "years.txt", "r") as f:
            years = f.read().splitlines()
        years = [y for y in years if y != str(year)]
        with open(data_dir / "years.txt", "w") as f:
            f.write("\n".join(years))

def get_years() -> list[int]:
    data_dir = Path("~/.baseballquery").expanduser()
    if not (data_dir / "years.txt").exists():
        if not sqlalchemy.inspect(engine).has_table("events"):
            return []
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT year FROM events"))
            years = [row[0] for row in result.fetchall()]
        with open(data_dir / "years.txt", "w") as f:
            f.write("\n".join(map(str, sorted(years))))
    
        return sorted(years)
    with open(data_dir / "years.txt", "r") as f:
        years = f.read().splitlines()
    return [int(year) for year in years if year.isdigit()]


def get_linear_weights() -> pd.DataFrame:
    if not sqlalchemy.inspect(engine).has_table("linear_weights"):
        raise FileNotFoundError(
            "Linear weights not found. Have you run baseballquery.update_data() to download the data?"
        )
    with engine.connect() as conn:
        lin = pd.read_sql("SELECT * FROM linear_weights", conn)
    return lin
