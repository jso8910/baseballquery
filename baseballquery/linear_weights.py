from tqdm import tqdm
import pandas as pd  # type: ignore
from pathlib import Path
import h5py  # type: ignore
import numpy as np


def calc_average_stats(events: pd.DataFrame):
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
    for stat in tqdm(
        totals.keys(), position=1, desc="Calculating league average", leave=False
    ):
        if stat == "IP":
            totals[stat] = events["EVENT_OUTS_CT"].sum() / 3  # type: ignore
            continue
        totals[stat] = events[stat].sum()  # type: ignore

    per_600_pa: dict[str, float | str] = totals.copy()  # type: ignore
    scaling = 600 / totals["PA"]
    for field in per_600_pa:
        per_600_pa[field] = totals[field] * scaling

    per_600_pa_pd = pd.DataFrame([per_600_pa])
    return per_600_pa_pd


def calc_linear_weights(events: pd.DataFrame):
    per_600_pa = calc_average_stats(events)
    run_exp_by_sit = [
        # ["Outs", "Runner state", "RUNS", "COUNT", "AVG"],
        [n % 3, n // 3, 0, 0, 0.0]
        for n in range(24)
    ]

    events["END_BASES_CD"] = events["END_BASES_CD"].astype(int)  # type: ignore
    events["OUTS_TEMP"] = events["EVENT_OUTS_CT"].astype(int) + events["OUTS_CT"].astype(int)  # type: ignore
    groups = events[events["OUTS_TEMP"] < 3].groupby(["END_BASES_CD", "OUTS_TEMP"])  # type: ignore
    for _, g in groups:  # type: ignore
        run_exp_by_sit[g["END_BASES_CD"].iloc[0] * 3 + g["OUTS_TEMP"].iloc[0]][2] += g["FATE_RUNS_CT"].sum()  # type: ignore
        run_exp_by_sit[g["END_BASES_CD"].iloc[0] * 3 + g["OUTS_TEMP"].iloc[0]][3] += g["FATE_RUNS_CT"].count()  # type: ignore

    for idx in range(len(run_exp_by_sit)):
        run_exp_by_sit[idx][4] = run_exp_by_sit[idx][2] / run_exp_by_sit[idx][3]

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

    events["START_BASES_CD"] = events["START_BASES_CD"].astype(int)  # type: ignore
    events["END_BASES_CD"] = events["END_BASES_CD"].astype(int)  # type: ignore
    events["OUTS_CT"] = events["OUTS_CT"].astype(int)  # type: ignore
    run_exp_by_sit = np.array(run_exp_by_sit, dtype=float)
    run_exp_by_sit = np.vstack([run_exp_by_sit, np.array([3, 0, 0, 0, 0.0])])
    run_exp_by_sit = pd.DataFrame(run_exp_by_sit)

    run_exps_end = run_exp_by_sit.iloc[events["END_BASES_CD"] * 3 + events["OUTS_TEMP"]]  # type: ignore
    run_exps_end = run_exps_end.reset_index(drop=True)  # type: ignore
    events["END_RUN_EXP"] = np.where(events["OUTS_TEMP"] < 3, run_exps_end.loc[:, 4], 0.0)  # type: ignore

    run_exps_start = run_exp_by_sit.iloc[events["START_BASES_CD"] * 3 + events["OUTS_CT"]]  # type: ignore
    run_exps_start = run_exps_start.reset_index(drop=True)  # type: ignore
    # Completely bizarre workaround. Have 0 idea why this is necessary to use a fake np.where
    events["START_RUN_EXP"] = np.where(True, run_exps_start[4], 0)  # type: ignore

    groups = events.groupby("EVENT_CD")  # type: ignore
    for _, g in groups:
        if int(g["EVENT_CD"].iloc[0]) not in event_code_to_event:  # type: ignore
            continue

        run_expectancy_total[event_code_to_event[int(g["EVENT_CD"].iloc[0])]] += (  # type: ignore
            g["END_RUN_EXP"].sum() + g["EVENT_RUNS_CT"].sum()  # type: ignore
        ) - g[
            "START_RUN_EXP"
        ].sum()  # type: ignore
        if int(g["EVENT_CD"].iloc[0]) in (20, 21, 22):  # type: ignore
            run_expectancy_total["HitInPlay"] += (
                g["END_RUN_EXP"].sum() + g["EVENT_RUNS_CT"].sum()  # type: ignore
            ) - g[
                "START_RUN_EXP"
            ].sum()  # type: ignore
            run_expectancy_freq["HitInPlay"] += g.shape[0]
        run_expectancy_freq[event_code_to_event[int(g["EVENT_CD"].iloc[0])]] += g.shape[0]  # type: ignore
        if event_code_to_event[int(g["EVENT_CD"].iloc[0])] in ("1B", "2B", "3B", "Out"):  # type: ignore
            run_expectancy_total["BIP"] += (
                g["END_RUN_EXP"].sum() + g["EVENT_RUNS_CT"].sum()  # type: ignore
            ) - g[
                "START_RUN_EXP"
            ].sum()  # type: ignore
            run_expectancy_freq["BIP"] += g.shape[0]

    run_exps_start = run_exp_by_sit.iloc[events["START_BASES_CD"] * 3 + events["OUTS_CT"]]  # type: ignore
    run_exps_start = run_exps_start.reset_index(drop=True)  # type: ignore

    for event in run_expectancy_total:
        run_expectancy_avg[event] = (
            run_expectancy_total[event] / run_expectancy_freq[event]
        )

    for event in run_expectancy_total:
        run_expectancy_avg[event] -= run_expectancy_avg["Out"]

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

    for event in run_expectancy_total:
        run_expectancy_avg[event] *= obp_numerator / woba_numerator

    run_expectancy_avg["year"] = 0
    run_expectancy_avg["woba_scale"] = obp_numerator / woba_numerator
    run_expectancy_avg["avg_woba"] = (
        woba_numerator * run_expectancy_avg["woba_scale"] / 600
    )
    run_expectancy_avg["lg_runs_pa"] = per_600_pa["R"] / 600
    pa_scale = events["PA"].sum() / 600  # type: ignore
    run_expectancy_avg["lg_era"] = (
        9
        * (
            events["BAT_DEST_ID"].isin([4, 6]).astype(int).sum()  # type: ignore
            + events["RUN1_DEST_ID"].isin([4, 6]).astype(int).sum()  # type: ignore
            + events["RUN2_DEST_ID"].isin([4, 6]).astype(int).sum()  # type: ignore
            + events["RUN3_DEST_ID"].isin([4, 6]).astype(int).sum()  # type: ignore
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
    run_expectancy_avg["lg_hr_fb"] = per_600_pa["HR"] * pa_scale / (events["FB"].sum() + events["PU"].sum())  # type: ignore
    return run_expectancy_avg


def calc_all_weights():
    cwd = Path(__file__).parent
    chadwick_file = cwd / "chadwick.hdf5"

    linear_weights_dir = cwd
    linear_weights_dir.mkdir(parents=True, exist_ok=True)

    with h5py.File(chadwick_file) as f:
        years: list[str] = list(f.keys())

    weights_pd_list = []
    for year in tqdm(years, desc="Years", position=0, leave=True):
        events = pd.read_hdf(chadwick_file, year)  # type: ignore
        weights = calc_linear_weights(events)  # type: ignore
        weights["year"] = int(year[-4:])
        weights_pd = pd.DataFrame(weights)
        weights_pd_list.append(weights_pd)  # type: ignore

    weights_pd = pd.concat(weights_pd_list, ignore_index=True)  # type: ignore
    weights_pd.set_index("year", inplace=True)  # type: ignore
    _ = weights_pd.to_csv(linear_weights_dir / f"linear_weights.csv")  # type: ignore
