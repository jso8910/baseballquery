import pandas as pd
from .database import engine
import sqlalchemy
from sqlalchemy import text

def calc_linear_weights_from_db(years_list=[]):
    """
    Calculates the linear weights from the database.
    """
    query_linw = f"""
        WITH
            run_expectancy AS (
                SELECT
                    year,
                    START_BASES_CD as base_state,
                    OUTS_CT, avg(EVENT_RUNS_CT + FATE_RUNS_CT) AS run_exp
                FROM events
                WHERE year IN ({', '.join(map(str, years_list))})  -- Filter by years if provided
                GROUP BY year, START_BASES_CD, OUTS_CT
            ),
            event_run_exp AS (
                    SELECT
                        events.year,
                        CASE WHEN events.event_cd IN (2, 3) THEN 2 ELSE events.event_cd END as event_cd,
                        avg(CASE WHEN end.run_exp IS NULL THEN 0 ELSE end.run_exp END + events.EVENT_RUNS_CT - start.run_exp) as run_value
                    FROM events
                    LEFT JOIN run_expectancy AS start ON events.START_BASES_CD = start.base_state AND events.OUTS_CT = start.OUTS_CT AND events.year = start.year
                    LEFT JOIN run_expectancy AS end ON events.END_BASES_CD = end.base_state AND events.OUTS_CT + events.EVENT_OUTS_CT = end.OUTS_CT AND events.year = end.year
                    WHERE events.year IN ({', '.join(map(str, years_list))})  -- Filter by years if provided
                    GROUP BY events.year, CASE WHEN events.event_cd IN (2, 3) THEN 2 ELSE events.event_cd END
                UNION
                    SELECT
                        events.year,
                        25 as event_cd,
                        avg(CASE WHEN end.run_exp IS NULL THEN 0 ELSE end.run_exp END + events.EVENT_RUNS_CT - start.run_exp) as run_value
                    FROM events
                    LEFT JOIN run_expectancy AS start ON events.START_BASES_CD = start.base_state AND events.OUTS_CT = start.OUTS_CT AND events.year = start.year
                    LEFT JOIN run_expectancy AS end ON events.END_BASES_CD = end.base_state AND events.OUTS_CT + events.EVENT_OUTS_CT = end.OUTS_CT AND events.year = end.year
                    WHERE events.year IN ({', '.join(map(str, years_list))}) AND events.event_cd IN (2, 18, 19, 20, 21, 22)  -- BIP events
            ),
            run_exp_norm AS (
                SELECT
                    year,
                    event_run_exp.event_cd,
                    CASE WHEN event_run_exp.event_cd = 2 THEN run_value ELSE run_value - (SELECT run_value FROM event_run_exp WHERE event_run_exp.event_cd = 2 AND year = event_run_exp.year) END AS run_value
                FROM event_run_exp
                GROUP BY event_run_exp.year, event_run_exp.event_cd
            ),
            woba_scale AS (
                SELECT
                    events.year,
                    (((sum("1B") + sum("2B") + sum("3B") + sum(HR) + sum(UBB) + sum(IBB) + sum(HBP)) * 1.0) /
                    ((SELECT run_value FROM run_exp_norm WHERE event_cd = 20 AND events.year = run_exp_norm.year) * sum("1B") +
                    (SELECT run_value FROM run_exp_norm WHERE event_cd = 21 AND events.year = run_exp_norm.year) * sum("2B") +
                    (SELECT run_value FROM run_exp_norm WHERE event_cd = 22 AND events.year = run_exp_norm.year) * sum("3B") +
                    (SELECT run_value FROM run_exp_norm WHERE event_cd = 23 AND events.year = run_exp_norm.year) * sum(HR) +
                    (SELECT run_value FROM run_exp_norm WHERE event_cd = 14 AND events.year = run_exp_norm.year) * sum(UBB) +
                    (SELECT run_value FROM run_exp_norm WHERE event_cd = 16 AND events.year = run_exp_norm.year) * sum(HBP))) AS woba_scale_num
                FROM events
                WHERE events.year IN ({', '.join(map(str, years_list))})  -- Filter by years if provided
                GROUP BY events.year
            )
        SELECT
            run_exp_norm.year,
            event_cd,
            run_value * woba_scale.woba_scale_num AS weight,
            woba_scale.woba_scale_num AS woba_scale
        FROM run_exp_norm
        LEFT JOIN woba_scale ON run_exp_norm.year = woba_scale.year;
    """

    # Execute the query and get the results
    df = pd.read_sql(query_linw, engine)
    linw_df = pd.DataFrame(columns=["year", "1B", "2B", "3B", "HR", "UBB", "HBP", "BIP", "OutRAA", "woba_scale", "lg_woba", "lg_runs_pa", "lg_era", "fip_constant", "lg_hr_fb"])
    for year in df["year"].unique():
        year_df = df[df["year"] == year]
        row = {
            "year": year,
            "1B": year_df[year_df["event_cd"] == 20]["weight"].values[0],
            "2B": year_df[year_df["event_cd"] == 21]["weight"].values[0],
            "3B": year_df[year_df["event_cd"] == 22]["weight"].values[0],
            "HR": year_df[year_df["event_cd"] == 23]["weight"].values[0],
            "UBB": year_df[year_df["event_cd"] == 14]["weight"].values[0],
            "HBP": year_df[year_df["event_cd"] == 16]["weight"].values[0],
            "BIP": year_df[(year_df["event_cd"] == 20) | (year_df["event_cd"] == 21) | (year_df["event_cd"] == 22)]["weight"].sum(),
            # Value of an out above average
            "OutRAA": year_df[year_df["event_cd"] == 2]["weight"].values[0],
            "woba_scale": year_df["woba_scale"].values[0],
            "lg_woba": 0,   # Placeholder, will be calculated later
            "lg_runs_pa": 0,    # Placeholder, will be calculated later
            "lg_era": 0,    # Placeholder, will be calculated later
            "fip_constant": 0,  # Placeholder, will be calculated later
            "lg_hr_fb": 0,  # Placeholder, will be calculated later
        }
        linw_df = pd.concat([linw_df, pd.DataFrame([row])], ignore_index=True)

    # Calculate lg_woba, lg_runs_pa, lg_era, fip_constant, and lg_hr_fb
    query_avg_stats = f"""
        SELECT
            year,
            sum(R) * 1.0 / (sum(PA) * 1.0) as lg_runs_pa,
            (sum("1B") + sum("2B") + sum("3B") + sum(HR) + sum(UBB) + sum(IBB) + sum(HBP)) / (sum(PA) * 1.0) as lg_woba,
            9 * (sum(ER) * 1.0) / (sum(EVENT_OUTS_CT) / 3) as lg_era,
            9 * (sum(ER) * 1.0) / (sum(EVENT_OUTS_CT) / 3) - (13.0 * sum(HR) + 3 * (sum(UBB) + sum(IBB) + sum(HBP)) - 2 * sum(K)) / (sum(EVENT_OUTS_CT) / 3) as fip_constant,
            sum(HR) * 1.0 / (sum(FB) * 1.0 + sum(PU) * 1.0) as lg_hr_fb
        FROM events
        WHERE year in ({', '.join(map(str, years_list))})  -- Filter by years if provided
        GROUP BY year;
    """

    # Execute the query and get the results
    avg_stats_df = pd.read_sql(query_avg_stats, engine)
    for year in linw_df["year"].unique():
        year_df = avg_stats_df[avg_stats_df["year"] == year]
        if not year_df.empty:
            linw_df.loc[linw_df["year"] == year, "lg_woba"] = year_df["lg_woba"].values[0]
            linw_df.loc[linw_df["year"] == year, "lg_runs_pa"] = year_df["lg_runs_pa"].values[0]
            linw_df.loc[linw_df["year"] == year, "lg_era"] = year_df["lg_era"].values[0]
            linw_df.loc[linw_df["year"] == year, "fip_constant"] = year_df["fip_constant"].values[0]
            linw_df.loc[linw_df["year"] == year, "lg_hr_fb"] = year_df["lg_hr_fb"].values[0]

    # Output to SQL database
    if not sqlalchemy.inspect(engine).has_table("linear_weights"):
        query = text(pd.io.sql.get_schema(linw_df, 'linear_weights'))   # type: ignore
        with engine.begin() as conn:
            conn.execute(query)

    table = sqlalchemy.Table("linear_weights", sqlalchemy.MetaData(), autoload_with=engine)
    insert = table.insert()
    with engine.begin() as conn:
        conn.execute(insert, linw_df.to_dict(orient="records")) # type: ignore
