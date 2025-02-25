from tqdm import tqdm
import pandas as pd
from pathlib import Path
from . import utils
import numpy as np


def calc_average_stats(events: pd.DataFrame):
    """
    Calculates the average stats per 600 plate appearances for a given events dataframe
    """
    totals: dict[str, int] = {
        "PA": 0,  # Plate appearance
        "AB": 0,  # At bat
        "1B": 0,  # Single
        "2B": 0,  # Double
        "3B": 0,  # Triple
        "HR": 0,  # Home run
        "UBB": 0,  # Unintentional walk
        "IBB": 0,  # Intentional walk
        "HBP": 0,  # Hit by pitch
        "SF": 0,  # Sacrifice fly
        "SH": 0,  # Sacrifice hit
        "K": 0,  # Strikeout
        "SB": 0,  # Stolen base
        "CS": 0,  # Caught stealing
        "FC": 0,  # Fielder's choice
        "R": 0,  # Runs
        "IP": 0,  # Innings pitched
    }

    # Correspondance of event_cd to totals
    for stat in tqdm(totals.keys(), position=1, desc="Calculating league average", leave=False):
        if stat == "IP":
            totals[stat] = events["EVENT_OUTS_CT"].sum() / 3
            continue
        totals[stat] = events[stat].sum()

    per_600_pa: dict[str, float | str] = totals.copy()  # type: ignore
    scaling = 600 / totals["PA"]
    for field in per_600_pa:
        per_600_pa[field] = totals[field] * scaling

    per_600_pa_pd = pd.DataFrame([per_600_pa])
    return per_600_pa_pd


def calc_linear_weights(events: pd.DataFrame):
    per_600_pa = calc_average_stats(events)
    # The index in this array corresponds to base_state * 3 + outs.
    # A base state of 0 is bases empty, 1 is runner on first, 2 is runner on second, 4 is runner on third, 5 is first and third, etc
    # It's a binary representation (0b001 is first, 010, is second, 100 is third)
    run_exp_by_sit = [
        # ["Outs", "Runner state", "RUNS", "COUNT", "AVG"],
        [n % 3, n // 3, 0, 0, 0.0]
        for n in range(24)
    ]

    # Creates 24 base-out state groups. Excludes events with 3 outs at the end of the play
    groups = events[events["OUTS_CT"] < 3].groupby(["START_BASES_CD", "OUTS_CT"])
    for _, g in groups:
        run_exp_by_sit[g["START_BASES_CD"].iloc[0] * 3 + g["OUTS_CT"].iloc[0]][2] += (
            g["FATE_RUNS_CT"].sum() + g["EVENT_RUNS_CT"].sum()
        )
        run_exp_by_sit[g["START_BASES_CD"].iloc[0] * 3 + g["OUTS_CT"].iloc[0]][3] += g["FATE_RUNS_CT"].count()

    # Calculate the final RE24 matrix
    for idx in range(len(run_exp_by_sit)):
        if run_exp_by_sit[idx][3] == 0:
            run_exp_by_sit[idx][4] = 0
        else:
            run_exp_by_sit[idx][4] = run_exp_by_sit[idx][2] / run_exp_by_sit[idx][3]

    # Total runs added through each event
    run_expectancy_total = {
        "1B": 0.0,
        "2B": 0.0,
        "3B": 0.0,
        "HR": 0.0,
        "UBB": 0.0,
        "HBP": 0.0,
        "K": 0.0,
        "BIP": 0.0,
        "Out": 0.0,
        "HitInPlay": 0.0,
    }

    # Number of times each event happens
    run_expectancy_freq = {
        "1B": 0,
        "2B": 0,
        "3B": 0,
        "HR": 0,
        "UBB": 0,
        "HBP": 0,
        "K": 0,
        "BIP": 0,
        "Out": 0,
        "HitInPlay": 0.0,
    }

    # This will store the wOBA weights
    run_expectancy_avg = {
        "1B": 0.0,
        "2B": 0.0,
        "3B": 0.0,
        "HR": 0.0,
        "UBB": 0.0,
        "HBP": 0.0,
        "K": 0.0,
        "BIP": 0.0,  # BIP = Balls In Park (doesn't include HR or any of the other TTOs)
        "Out": 0.0,
        "HitInPlay": 0.0,
    }

    # Simple lookup table. Source: https://chadwick.sourceforge.net/doc/cwevent.html
    event_code_to_event = {
        2: "Out",
        3: "K",
        14: "UBB",
        16: "HBP",
        20: "1B",
        21: "2B",
        22: "3B",
        23: "HR",
    }

    # This is outs at the end of the play
    events["OUTS_END"] = events["EVENT_OUTS_CT"] + events["OUTS_CT"]

    # Converting RE24 to a numpy array and adding a dummy row in case there's 3 outs and bases loaded
    # This is needed because Chadwick doesn't set END_BASES_CD to 0 when there's 3 outs
    # Then I convert to a dataframe
    run_exp_by_sit = np.array(run_exp_by_sit, dtype=float)
    run_exp_by_sit = np.vstack([run_exp_by_sit, np.array([3, 0, 0, 0, 0])])
    run_exp_by_sit = pd.DataFrame(run_exp_by_sit)

    # Calculate the run expectancy for the start and end of the play
    # This is done by using series as indices for the base-out state
    run_exps_end = run_exp_by_sit.iloc[events["END_BASES_CD"] * 3 + events["OUTS_END"]]
    run_exps_end = run_exps_end.reset_index(drop=True)
    # Makes sure that the run expectancy is 0 if the play ends with 3 outs
    events["END_RUN_EXP"] = np.where(events["OUTS_END"] < 3, run_exps_end.loc[:, 4], 0.0)

    run_exps_start = run_exp_by_sit.iloc[events["START_BASES_CD"] * 3 + events["OUTS_CT"]]
    run_exps_start = run_exps_start.reset_index(drop=True)
    # Completely bizarre workaround. Have 0 idea why this is necessary to use a fake np.where
    events["START_RUN_EXP"] = np.where(True, run_exps_start[4], 0)

    # Create groups of events with the same event code
    groups = events.groupby("EVENT_CD")
    for _, g in groups:
        # These are events we can ignore (like pickoffs, etc)
        if int(g["EVENT_CD"].iloc[0]) not in event_code_to_event:
            continue

        # Modify the correct event. End run exp + runs scored - start run exp
        run_expectancy_total[event_code_to_event[int(g["EVENT_CD"].iloc[0])]] += (
            g["END_RUN_EXP"].sum() + g["EVENT_RUNS_CT"].sum()
        ) - g["START_RUN_EXP"].sum()
        # Get the number of events
        run_expectancy_freq[event_code_to_event[int(g["EVENT_CD"].iloc[0])]] += g.shape[0]

        # Some events have two different things that need to be chnaged
        if int(g["EVENT_CD"].iloc[0]) in (20, 21, 22):
            run_expectancy_total["HitInPlay"] += (g["END_RUN_EXP"].sum() + g["EVENT_RUNS_CT"].sum()) - g[
                "START_RUN_EXP"
            ].sum()
            run_expectancy_freq["HitInPlay"] += g.shape[0]
        if event_code_to_event[int(g["EVENT_CD"].iloc[0])] in ("1B", "2B", "3B", "Out"):
            run_expectancy_total["BIP"] += (g["END_RUN_EXP"].sum() + g["EVENT_RUNS_CT"].sum()) - g[
                "START_RUN_EXP"
            ].sum()
            run_expectancy_freq["BIP"] += g.shape[0]

    # Calculate the average run expectancy for each event
    for event in run_expectancy_total:
        run_expectancy_avg[event] = run_expectancy_total[event] / run_expectancy_freq[event]

    # Rescale run expectancies with respect to outs being 0 runs added
    for event in run_expectancy_total:
        run_expectancy_avg[event] -= run_expectancy_avg["Out"]

    # Calculate average OBP and wOBA to get the wOBA scale
    obp_numerator: int = (  # type: ignore
        per_600_pa["1B"]
        + per_600_pa["2B"]
        + per_600_pa["3B"]
        + per_600_pa["HR"]
        + per_600_pa["HBP"]
        + per_600_pa["IBB"]
        + per_600_pa["UBB"]
    )

    woba_numerator: float = (  # type: ignore
        run_expectancy_avg["1B"] * per_600_pa["1B"]
        + run_expectancy_avg["2B"] * per_600_pa["2B"]
        + run_expectancy_avg["3B"] * per_600_pa["3B"]
        + run_expectancy_avg["HR"] * per_600_pa["HR"]
        + run_expectancy_avg["UBB"] * per_600_pa["UBB"]
        + run_expectancy_avg["HBP"] * per_600_pa["HBP"]
    )

    # Adjust the run expectancy for each event by the wOBA scale
    for event in run_expectancy_total:
        run_expectancy_avg[event] *= obp_numerator / woba_numerator

    # This will be set outside of this function
    run_expectancy_avg["year"] = 0
    # Calculate a bunch of information that are useful for other calculations
    run_expectancy_avg["woba_scale"] = obp_numerator / woba_numerator
    run_expectancy_avg["avg_woba"] = woba_numerator * run_expectancy_avg["woba_scale"] / 600
    run_expectancy_avg["lg_runs_pa"] = per_600_pa["R"] / 600  # type: ignore
    pa_scale = events["PA"].sum() / 600

    # Calculates the average league ERA
    # 4 = earned, 6 = team unearned but earned to the pitcher
    run_expectancy_avg["lg_era"] = (
        9
        * (
            events["BAT_DEST_ID"].isin([4, 6]).astype(int).sum()
            + events["RUN1_DEST_ID"].isin([4, 6]).astype(int).sum()
            + events["RUN2_DEST_ID"].isin([4, 6]).astype(int).sum()
            + events["RUN3_DEST_ID"].isin([4, 6]).astype(int).sum()
        )
        / (per_600_pa["IP"] * pa_scale)
    )
    run_expectancy_avg["fip_constant"] = (
        run_expectancy_avg["lg_era"]
        - (
            13 * per_600_pa["HR"]
            + 3 * (per_600_pa["UBB"] + per_600_pa["IBB"] + per_600_pa["HBP"])
            - 2 * per_600_pa["K"]
        )
        / per_600_pa["IP"]
    )
    # League average HR/FB%
    run_expectancy_avg["lg_hr_fb"] = per_600_pa["HR"] * pa_scale / (events["FB"].sum() + events["PU"].sum())
    return run_expectancy_avg


def calc_weights(years_list=None):
    data_dir = Path("~/.baseballquery").expanduser()

    linear_weights_dir = data_dir
    linear_weights_dir.mkdir(parents=True, exist_ok=True)

    years = utils.get_years()

    if years_list:
        years = [year for year in years if year in years_list]

    weights_pd_list = []
    for year in tqdm(years, desc="Years", position=0, leave=True):
        events = utils.get_year_events(year)
        weights = calc_linear_weights(events)  # type: ignore
        weights["year"] = year
        weights_pd = pd.DataFrame(weights)
        weights_pd_list.append(weights_pd)

    weights_pd = pd.concat(weights_pd_list, ignore_index=True)
    weights_pd.set_index("year", inplace=True)
    if (linear_weights_dir / "linear_weights.csv").exists():
        weights_original = pd.read_csv(linear_weights_dir / "linear_weights.csv")
        weights_original.set_index("year", inplace=True)

        for row in weights_pd.index:
            if row in weights_original.index:
                weights_original.drop(row, inplace=True)
        weights_pd = pd.concat([weights_original, weights_pd])

    weights_pd.sort_index(inplace=True)
    _ = weights_pd.to_csv(linear_weights_dir / f"linear_weights.csv")
