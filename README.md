# Baseball Query

Essentially, this is a Stathead replacement. You can query stats in detail by either using pre-created splits or curating your own.

## Getting started
Before you use this, you need to [install Chadwick](https://github.com/chadwickbureau/chadwick/blob/master/INSTALL). Then, this package can be installed from PyPi.
```zsh
pip install baseballquery
```

Then, run these commands and you're good to go!

```py
import baseballquery
baseballquery.update_data()
```

`update_data()` downloads a bunch of data to the ~/.baseballquery directory (on Windows, the folder name will be the same, just in your user home directory). If you want to delete the data, delete this directory.

By default, all data from 1912 onwards is downloaded. If you want to download fewer years into the past (for example, only from 1990 onwards), run this before running `update_data()`

```py
baseballquery.set_first_data_year(1990)    # Now, only years from 1990 to the current year are downloaded
```

Don't set this to a value that's after the current year. Nothing will be downloaded if you do this.

Any time you want to add new games from the current season or any new released Retrosheet data, rerun `update_data()`.

When you install this package and update the datafor the first time, it will download many GB of data from Retrosheet. Eventually, it will be deleted, but you will get a total of about 2.7 GB of data in the form of a bunch of a sqlite3 database. This whole process (including calculating linear weights) can take 30-40 minutes, so start running this in the background once you install it before you use it. If you are installing live season data, this will take about 3-4 minutes at most (depending on your network connection especially). 

Then, you initiate a stats split object to get stats.

```py
import baseballquery
splits = baseballquery.BattingStatSplits(2020, 2024)
# or, if you want pitching stats
splits = baseballquery.PitchingStatSplits(2020, 2024)
# Then apply all the splits you want. All available under the two classes in stat_splits.py
# Or, you can manually remove events from splits.events
# Finally, to get the stats dataframe
splits.calculate_stats()
print(splits.stats)
```

If you don't know what a column in the `events` object means, view the documentation for either [cwevent](https://chadwick.readthedocs.io/en/latest/cwevent.html) (it's most likely to be this one) or [cwgame](https://chadwick.readthedocs.io/en/latest/cwgame.html). If it is in neither and it is not a self explanatory field, it is likely something which is used internally by baseballquery for updating downloaded information. If you're still not sure what something is, just create an issue and I'll let you know.

Not implemented:
- Park factors for x+ stats
- Full game stats (saves, holds, shutouts, etc.) for pitchers. This one is probably important
- With splits, ERA is pretty much nonsense. Just because, even if a pitcher leaves the game, they are credited with an earned run if a runner they left on base scores. Even if they aren't eligible for the split.
    - In general, it's not really possible to coherently calculate ERA for splits. For example: if two hits come against righties then a lefty hits a homer, scoring 3 runs, is the number of earned runs against righties 0? or 1? or 2? It's not really possible to say. So, if you set any significant splits which eliminate PAs (basically anything other than set_split and set_subdivision), ignore ERA.

## Live season data
This package supports live season data. One of the biggest issues with Retrosheet is that their data only comes out a month or so after the season ends, meaning any analysis you do with the Retrosheet file format cannot be done on the current season. Well, that problem is no more after hours of painstaking work to convert the MLB StatsAPI (the API which feeds MLB GameDay) data into Chadwick CWEvent (and cwgame) data (a CSV file which the Retrosheet data is converted into). I have a list of differences between this approximation of Retrosheet and the actual Retrosheet data. The differences are quite minor. Even beyond these differences, it likely isn't perfect. However, other than my lack of confidence in the attributing of responsible pitchers (and thus earned runs) being completely correct (though, if there are any errors, it will just be a few earned runs across the entire league per season), I am confident that all significant fields work practically perfectly. Below is my list of differences between my processing and the Chadwick processing when testing in the 2024 season.

### Category 1: bugs/limitations
- Some game-level info from cwgame is missing. The fields PARK_ID, FIELD_PARK_CD, PRECIP_PARK_CD, and SKY_PARK_CD do not work (the first field is the Retrosheet ID for a stadium, the latter 3 fields are weather-related fields).
- Player IDs for players that debuted during the current year do not work fully (because the Chadwick persons register doesn't have their Retrosheet IDs yet). I attempt to approximate them but, because it is an approximation, using Retrosheet IDs you find online for recently debuted players will not work. The letter "x" has been added to the end of all Retrosheet IDs which were approximated in this way so they're clear in the database. One important thing to know is this: if two players debut in a season which share the first four letters of their last names and the first letter of their first names (according to the `name_first` field from the [Chadwick persons register](https://github.com/chadwickbureau/register)), they will have the same ID. If this becomes a genuine issue for you, open a Github issue. But, I don't see a good solution for it at this time.
- On some plays, MLB doesn't have ball tracking data which Retrosheet does. Or, as a result of them using different data sources, `BATTEDBALL_CD` will sometimes differ.
- On some plays, MLB lacks the proper fielding credits to determine whether a batter reached on error. This generally occurs on plays where a dropped throw or a throwing error results in the batter being safe at first and then advancing to second. In this case, the MLB API puts an error credit for the fielder with the error under the advance from first to second, with no fielder credits being listed on the advance from home to first. This means there is no way to differentiate between a batter reaching safety (eg on a fielder's choice) before reaching second on an error and a batter reaching first and advancing to second on the same error. So, on rare occasions, some plays will have `BAT_SAFE_ERR_FL` set to `False` when it should be `True`.
- Chadwick is not entirely consistent with whether `EVENT_CD` should be set to 2 (generic out) or 18 (error) when the batter reached safely on error. So, occassionally `EVENT_CD` will erroneously be 2 instead of 18 (never the other way around). To the best of my knowledge, nothing in the MLB StatsAPI differentiates these two circumstances.
- On plays with 2 or more outs, occassionally the conversion will incorrectly have `EVENT_CD` as 2 rather than 19 (fielder's choice). I have my best approximation for how to fix this (and I have not tested whether this bug still exists with my numerous changes in conversion logic throughout the testing process) but it may still occasionally occur.
- It's difficult to tell the difference between a fielder's choice and a reached on error on sacrifice plays. So the `BAT_SAFE_ERR_FL` will rarely be wrong (and `EVENT_CD` will be 2 or 18 instead of 19, as I generally assume an error)
- If a switch hitter is the result hitter (`RESP_BAT_ID`) but is not the hitter who finished the plate appearance (so if there's a strikeout after a player leaves the game with 2 strikes), their handedness (`RESP_BAT_HAND_CD`) cannot be deduced correctly. So, I assume they are batting lefty.
- Similarly, with pitchers, if they do not pitch at least one complete plate appearance (whether the starter exits the game on the first PA of the game or there are two pitching changes in one PA) but they still show up in the data because some running play occurred while they were pitching (like a wild pitch, a runner stealing a base, etc), their handedness (`RESP_PIT_HAND_CD`) will likely be incorrect.

### Category 2: deliberate implementation differences
- There are some instances where Chadwick takes `STRKES_CT` and `BALLS_CT` from what I would consider to be a pitch too early. This happens on certain pitching plays, where a stolen base, caught stealing, etc. happens after a pitcher step off rather than a pitch to home plate. For example, if the count is 2-2 and the runner takes off towards second before a pitch is thrown and steals second, my code will record `STRIKES_CT` and `BALLS_CT` as 2 and 2 respectively (a 2-2 count). However, Chadwick will record the count as 1-2 or 2-1 or whatever the count was on the pitch BEFORE the running event. I personally disagree with this implementation that Chadwick does and so, it being of little consequence, decided not to make my code even more messy by replicating it.
- When a DH is pinch hit for, Chadwick considers them immediately to have a position code of 10 (for DH) rather than 11 (for PH). I think it is better to leave them as 11 until the next inning when the MLB StatsAPI officially lists their position and they are officially the DH.
- Chadwick differentiates between normal unearned runs and runs that are unearned because the runner is a ghost runner by changing the `RUNx_DEST_ID` to be 7 instead of 5. I have not done this. There is no functional difference to this for calculating eg ERs.

When the new Retrosheet comes out and this package is updated, all live season data tables will be deleted and redownloaded.

### Some data
Here is some data on the accuracy of the reconstruction for batting stats. This being for the year 2024. The left column is each statistic, the right column is the number of players who were exactly correct (to the decimal place), out of 651 players. The one caveat with this is that the linear weights used with the reconstruction of the data are the original ones from the 2024 Retrosheet data, though it should be clear that did not make a large difference.

```
G             651
PA            651
AB            651
H             651
1B            651
2B            651
3B            651
HR            651
UBB           651
IBB           651
HBP           651
SF            651
SH            651
K             651
DP            649
TP            651
SB            651
CS            650
ROE           235
FC            626
R             651
RBI           651
GB            651
LD            651
FB            646
PU            649
AVG           651
OBP           651
SLG           651
OPS           651
ISO           651
BABIP         646
BB%           651
K%            651
K/BB          646
wOBA          651
wRAA          651
wRC           651
wRC+          651
GB%           644
LD%           644
FB%           642
PU%           642
```

By far the worst is ROE where the mean of the difference between the real data and the reconstructed data is -1.5 (so the reconstructed data has 1.5 fewer ROEs per player than the real data). There are zero players with too many ROEs.

Now for pitchers. Firstly, Grant Holmes and Grant Holman sadly got merged into one pitcher in my reconstruction. Honestly this is the biggest issue, but with live updating throughout the season, it's very hard to avoid. So, for these comparisons, I deleted both from both the StatsAPI reconstruction and the original data. This is out of 853 total pitchers.

```
G             853
GS            853
IP            853
TBF           853
AB            853
H             853
R             853
ER            852
UER           852
1B            853
2B            853
3B            853
HR            853
UBB           853
IBB           853
HBP           853
DP            851
TP            853
WP            853
BK            853
K             853
P             853
GB            853
LD            853
FB            848
PU            851
SH            853
SF            853
ERA           850
FIP           852
xFIP          849
WHIP          853
ERA-          850
FIP-          852
xFIP-         849
BABIP         851
BB%           853
K%            853
K-BB%         853
K/BB          822
BB/9          852
K/9           851
wOBA          853
HR/FB%        832
```

This was quite surprising. I was expecting it to be very bad, especially with earned runs. All the issues with K/BB are just because NaN == NaN returns False. There is a mere one too many earned runs allocated over the course of a season, given to the unlucky Ryan Fernandez of the St Louis Cardinals. The other two "errors" in ERA are with pitchers who have ERAs of NaN.

## Retrosheet acknowledgement

     The information used here was obtained free of
     charge from and is copyrighted by Retrosheet.  Interested
     parties may contact Retrosheet at "www.retrosheet.org".

Retrosheet makes no guarantees of accuracy for the information 
that is supplied. Much effort is expended to make our website 
as correct as possible, but Retrosheet shall not be held 
responsible for any consequences arising from the use of the 
material presented here. All information is subject to corrections 
as additional data are received. We are grateful to anyone who
discovers discrepancies and we appreciate learning of the details.
