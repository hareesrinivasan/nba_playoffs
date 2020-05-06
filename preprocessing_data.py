import pandas as pd
from sklearn.model_selection import train_test_split


def join_games_splits():
    """
    Function that merges the Home Road Splits with All_Games
    """
    games = pd.read_csv("data/All_Games.csv", parse_dates=["Date"])
    splits = pd.read_csv("data/Home_Visitor_Splits.csv")

    # Remove duplicate rows from splits (mistake in scraping thats too expensive to fix)
    # Restrict games to 1984
    splits = splits.drop_duplicates()
    games = games.loc[games.YR >= 1984].reset_index(drop=True)

    home_stats = splits.loc[splits.split_value == "Home", [col for col in splits
                                                           if col != "split_value"]]\
                       .rename(columns={"team": "Home"})
    visitor_stats = splits.loc[splits.split_value == "Visitor", [col for col in
                                                                 splits if col != "split_value"]]\
                          .rename(columns={"team": "Visitor"})

    home_stats.columns = [f"{col}_home" if col not in ["Home", "yr"] else col
                          for col in home_stats.columns]
    visitor_stats.columns = [f"{col}_visitor" if col not in ["Visitor", "yr"]
                             else col for col in visitor_stats.columns]

    joint_df = pd.merge(games, visitor_stats, left_on=["Visitor", "YR"],
                        right_on=["Visitor", "yr"], how="left")
    joint_df = pd.merge(joint_df, home_stats, left_on=["Home", "YR"],
                        right_on=["Home", "yr"], how="right")
    return joint_df


def join_playoff_standings():
    """
    Function that merges NBA_Playoffs with NBA_Standings
    """
    playoffs = pd.read_csv("data/NBA_Playoffs.csv")
    standings = pd.read_csv("data/NBA_Standings.csv")

    merged_df = pd.merge(playoffs, standings, left_on=["Winner", "YR"],
                         right_on=["Team", "YR"], how="left")
    merged_df = pd.merge(merged_df, standings, left_on=["Loser", "YR"],
                         right_on=["Team", "YR"], how="left")
    merged_df.columns = [c.replace("_x", "_winner").replace("_y", "_loser") for
                         c in merged_df.columns]

    return merged_df


def join_datasets():
    """
    Function that merges offensive and defensive stats with All_Games
    """
    games = pd.read_csv("data/All_Games.csv", parse_dates=["Date"])
    off_stats = pd.read_csv("Offensive_Stats.csv")
    def_stats = pd.read_csv("Defensive_Stats.csv")
    def_stats.columns = [f"Opp_{c}" if c != "YR" else c for c in
                         def_stats.columns]

    # Sets boolean column for whether or not the home team won
    games.loc[games["Home_Pts"] > games["Visitor_Pts"], "Home_Win"] = 1
    games.loc[games["Home_Pts"] < games["Visitor_Pts"], "Home_Win"] = 0

    # Merges offensive stats
    joint_df = pd.merge(games, off_stats, left_on=["Visitor", "YR"],
                        right_on=["Team", "YR"], how="left")
    joint_df = pd.merge(joint_df, off_stats, left_on=["Home", "YR"],
                        right_on=["Team", "YR"], how="inner")
    joint_df.columns = [c.replace("_x", "_visitor").replace("_y", "_home") for c
                        in joint_df.columns]

    # Merges defensive stats
    joint_df = pd.merge(joint_df, def_stats, left_on=["Visitor", "YR"],
                        right_on=["Opp_Team", "YR"], how="left")
    joint_df = pd.merge(joint_df, def_stats, left_on=["Home", "YR"],
                        right_on=["Opp_Team", "YR"], how="left")
    joint_df.columns = [c.replace("_x", "_visitor").replace("_y", "_home") for c
                        in joint_df.columns]

    return joint_df


def calc_totals(joint_df):
    """
    Calculates 3P, 2P, and FT totals as attempts * percent
    Only used for non-split stats
    """
    # Visitor offensive totals
    joint_df["3P_off_visitor"] = joint_df["3PA_visitor"] * joint_df["3P%_visitor"]
    joint_df["2P_off_visitor"] = joint_df["2PA_visitor"] * joint_df["2P%_visitor"]
    joint_df["FT_off_visitor"] = joint_df["FTA_visitor"] * joint_df["FT%_visitor"]

    # Home offensive totals
    joint_df["3P_off_home"] = joint_df["3PA_home"] * joint_df["3P%_home"]
    joint_df["2P_off_home"] = joint_df["2PA_home"] * joint_df["2P%_home"]
    joint_df["FT_off_home"] = joint_df["FTA_home"] * joint_df["FT%_home"]

    # Visitor defensive totals
    joint_df["3P_def_visitor"] = joint_df["Opp_3PA_visitor"] * joint_df["Opp_3P%_visitor"]
    joint_df["2P_def_visitor"] = joint_df["Opp_2PA_visitor"] * joint_df["Opp_2P%_visitor"]
    joint_df["FT_def_visitor"] = joint_df["Opp_FTA_visitor"] * joint_df["Opp_FT%_visitor"]

    # Home defensive totals
    joint_df["3P_def_home"] = joint_df["Opp_3PA_home"] * joint_df["Opp_3P%_home"]
    joint_df["2P_def_home"] = joint_df["Opp_2PA_home"] * joint_df["Opp_2P%_home"]
    joint_df["FT_def_home"] = joint_df["Opp_FTA_home"] * joint_df["Opp_FT%_home"]

    return joint_df


def split_train_test_sets(joint_df, test_size=0.2):
    joint_df = joint_df.loc[joint_df["Playoffs"] == 0]
    X = joint_df.loc[:, ["3P_off_visitor", "2P_off_visitor", "FT_off_visitor",
                         "3P_off_home", "2P_off_home", "FT_off_home",
                         "3P_def_visitor", "2P_def_visitor", "FT_def_visitor",
                         "3P_def_home", "2P_def_home", "FT_def_home"]]

    y = joint_df.loc[:, ["Home_Win"]]

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=test_size)

    return X_train, X_val, y_train, y_val


