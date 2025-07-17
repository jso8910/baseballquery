import sqlalchemy
from sqlalchemy import text
from .database import engine

def create_tables():
    """
    Run migrations to ensure the database schema is up to date.
    This function should be called at the start of the application.
    """
    # Create tables if they do not exist
    if not sqlalchemy.inspect(engine).has_table("events"):
        with engine.begin() as conn:
            conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS "events" (
                            "GAME_ID" TEXT,
                            "AWAY_TEAM_ID" TEXT,
                            "INN_CT" INTEGER,
                            "OUTS_CT" INTEGER,
                            "BALLS_CT" INTEGER,
                            "STRIKES_CT" INTEGER,
                            "AWAY_SCORE_CT" INTEGER,
                            "HOME_SCORE_CT" INTEGER,
                            "RESP_BAT_ID" TEXT,
                            "RESP_BAT_HAND_CD" TEXT,
                            "RESP_PIT_ID" TEXT,
                            "RESP_PIT_HAND_CD" TEXT,
                            "BASE1_RUN_ID" TEXT,
                            "BASE2_RUN_ID" TEXT,
                            "BASE3_RUN_ID" TEXT,
                            "BAT_FLD_CD" INTEGER,
                            "BAT_LINEUP_ID" INTEGER,
                            "EVENT_CD" INTEGER,
                            "AB_FL" INTEGER,
                            "H_CD" INTEGER,
                            "SH_FL" INTEGER,
                            "SF_FL" INTEGER,
                            "EVENT_OUTS_CT" INTEGER,
                            "DP_FL" INTEGER,
                            "TP_FL" INTEGER,
                            "RBI_CT" INTEGER,
                            "WP_FL" INTEGER,
                            "PB_FL" INTEGER,
                            "BATTEDBALL_CD" TEXT,
                            "BAT_DEST_ID" INTEGER,
                            "RUN1_DEST_ID" INTEGER,
                            "RUN2_DEST_ID" INTEGER,
                            "RUN3_DEST_ID" INTEGER,
                            "RUN1_SB_FL" INTEGER,
                            "RUN2_SB_FL" INTEGER,
                            "RUN3_SB_FL" INTEGER,
                            "RUN1_CS_FL" INTEGER,
                            "RUN2_CS_FL" INTEGER,
                            "RUN3_CS_FL" INTEGER,
                            "RUN1_PK_FL" INTEGER,
                            "RUN2_PK_FL" INTEGER,
                            "RUN3_PK_FL" INTEGER,
                            "RUN1_RESP_PIT_ID" TEXT,
                            "RUN2_RESP_PIT_ID" TEXT,
                            "RUN3_RESP_PIT_ID" TEXT,
                            "HOME_TEAM_ID" TEXT,
                            "BAT_TEAM_ID" TEXT,
                            "FLD_TEAM_ID" TEXT,
                            "PA_TRUNC_FL" INTEGER,
                            "START_BASES_CD" INTEGER,
                            "END_BASES_CD" INTEGER,
                            "RESP_BAT_START_FL" INTEGER,
                            "RESP_PIT_START_FL" INTEGER,
                            "PA_BALL_CT" INTEGER,
                            "PA_OTHER_BALL_CT" INTEGER,
                            "PA_STRIKE_CT" INTEGER,
                            "PA_OTHER_STRIKE_CT" INTEGER,
                            "EVENT_RUNS_CT" INTEGER,
                            "BAT_SAFE_ERR_FL" INTEGER,
                            "FATE_RUNS_CT" INTEGER,
                            "MLB_STATSAPI_APPROX" INTEGER,
                            "mlbam_id" TEXT,
                            "0-0" INTEGER,
                            "0-1" INTEGER,
                            "0-2" INTEGER,
                            "1-0" INTEGER,
                            "1-1" INTEGER,
                            "1-2" INTEGER,
                            "2-0" INTEGER,
                            "2-1" INTEGER,
                            "2-2" INTEGER,
                            "3-0" INTEGER,
                            "3-1" INTEGER,
                            "3-2" INTEGER,
                            "PA" INTEGER,
                            "AB" INTEGER,
                            "SH" INTEGER,
                            "SF" INTEGER,
                            "R" INTEGER,
                            "RBI" INTEGER,
                            "SB" INTEGER,
                            "CS" INTEGER,
                            "K" INTEGER,
                            "BK" INTEGER,
                            "UBB" INTEGER,
                            "IBB" INTEGER,
                            "HBP" INTEGER,
                            "FC" INTEGER,
                            "1B" INTEGER,
                            "2B" INTEGER,
                            "3B" INTEGER,
                            "HR" INTEGER,
                            "H" INTEGER,
                            "DP" INTEGER,
                            "TP" INTEGER,
                            "ROE" INTEGER,
                            "WP" INTEGER,
                            "P" INTEGER,
                            "GB" INTEGER,
                            "FB" INTEGER,
                            "LD" INTEGER,
                            "PU" INTEGER,
                            "ER" INTEGER,
                            "T_UER" INTEGER,
                            "UER" INTEGER,
                            "year" INTEGER,
                            "month" INTEGER,
                            "day" INTEGER,
                            "file_index" INTEGER
                            );"""))
            conn.execute(text("CREATE INDEX IF NOT EXISTS year ON events(year);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS events_game_id ON events(game_id);"))

    if not sqlalchemy.inspect(engine).has_table("baserunning"):
        with engine.begin() as conn:
            conn.execute(text("""
                            CREATE TABLE IF NOT EXISTS "baserunning" (
                                "file_index" INTEGER,
                                "RESP_BAT_ID" TEXT,
                                "GAME_ID" TEXT,
                                "SB_indiv" REAL,
                                "CS_indiv" REAL
                            );
                            """))

    if not sqlalchemy.inspect(engine).has_table("pitching_runs"):
        with engine.begin() as conn:
            conn.execute(text("""
                            CREATE TABLE IF NOT EXISTS "pitching_runs" (
                                "file_index" INTEGER,
                                "RESP_PIT_ID" TEXT,
                                "GAME_ID" TEXT,
                                "BAT_DEST_ID" INTEGER,
                                "RUN1_DEST_ID" INTEGER,
                                "RUN2_DEST_ID" INTEGER,
                                "RUN3_DEST_ID" INTEGER,
                                "R_indiv" INTEGER,
                                "ER_indiv" REAL,
                                "UER_indiv" REAL
                            );
                            """))

    if not sqlalchemy.inspect(engine).has_table("cwgame"):
        with engine.begin() as conn:
            conn.execute(text("""
                            CREATE TABLE IF NOT EXISTS "cwgame" (
                                "GAME_ID" TEXT,
                                "GAME_DY" TEXT,
                                "START_GAME_TM" INTEGER,
                                "DAYNIGHT_PARK_CD" TEXT,
                                "PARK_ID" TEXT,
                                "ATTEND_PARK_CT" INTEGER,
                                "TEMP_PARK_CT" INTEGER,
                                "WIND_DIRECTION_PARK_CD" INTEGER,
                                "WIND_SPEED_PARK_CT" INTEGER,
                                "FIELD_PARK_CD" INTEGER,
                                "PRECIP_PARK_CD" INTEGER,
                                "SKY_PARK_CD" INTEGER,
                                "MINUTES_GAME_CT" INTEGER,
                                "WIN_PIT_ID" TEXT,
                                "LOSE_PIT_ID" TEXT,
                                "SAVE_PIT_ID" TEXT,
                                "FINAL_INN_CT" INTEGER,
                                "FINAL_HOME_SCORE_CT" INTEGER,
                                "FINAL_AWAY_SCORE_CT" INTEGER
                            );
                            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS cwgame_game_id ON cwgame(game_id);"))

    if not sqlalchemy.inspect(engine).has_table("linear_weights"):
        with engine.begin() as conn:
            conn.execute(text("""
                            CREATE TABLE IF NOT EXISTS "linear_weights" (
                                "year" INTEGER,
                                "1B" REAL,
                                "2B" REAL,
                                "3B" REAL,
                                "HR" REAL,
                                "UBB" REAL,
                                "HBP" REAL,
                                "BIP" REAL,
                                "OutRAA" REAL,
                                "woba_scale" REAL,
                                "lg_woba" REAL,
                                "lg_runs_pa" REAL,
                                "lg_era" REAL,
                                "fip_constant" REAL,
                                "lg_hr_fb" REAL
                            );
                            """))