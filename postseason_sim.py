from preprocessing_data import *
import statsmodels.api as sm
import numpy as np


class SimSeries:
    def __init__(self, team1, team2, year, num_games=7, game_iters=15000,
                 series_iters=15000):
        self.year = year
        self.team1 = team1  # the higher seed
        self.team2 = team2  # the lower seed
        self.year = year  # the year
        self.num_games = num_games
        self.game_stats = self._retrieve_game_data()
        self.stats_year = self._retrieve_splits_data()
        self.game_iters = game_iters  # the number of times to simulate each game
        self.series_iters = series_iters  # the number of times to simulate each series
        self.team1_wins = 0
        self.team2_wins = 0
        self.winner = ""
        self.winner_pct = ""
        self.games = ""


    def execute(self):
        self._sim_series()
        series_results = self.result_df.mode()
        self.winner = series_results.winner.item()
        self.winner_pct = (self.result_df.loc[self.result_df.winner == self.winner].shape[0] /
                           self.result_df.shape[0])

        self.result_counts = self.result_df.copy()
        self.result_counts.loc[:, "count"] = 1
        self.result_counts = self.result_counts.groupby(["winner", "games"])["count"].count().reset_index().sort_values("count", ascending=False)
        self.most_common_result = self.result_counts.head(1)
        self.games = self.most_common_result.games.item()

    def _retrieve_game_data(self):
        all_year_stats = join_games_splits()
        games_year = all_year_stats.loc[all_year_stats.YR == self.year]
        return games_year

    def _retrieve_splits_data(self):
        splits_data = pd.read_csv("data/Home_Visitor_Splits.csv")
        splits_data = splits_data.drop_duplicates()
        stats_year = splits_data.loc[(splits_data.yr == self.year) & ((splits_data.team == self.team1) | (splits_data.team == self.team2))]
        return stats_year

    def _retrieve_stats(self, team, place):
        """
        This function retrieves the data for the given team and year and returns 6 stats:
        the team's 3 pointers scored
        the teams's 2 pointers scored
        the teams free throws scored
        the team's 3 pointers allowd
        the team's 2 pointers allowed
        the teams free throws allowed
        returns a dictionary with the above items
        """
        stat_dict = {}
        team_stats = self.stats_year.loc[(self.stats_year.team == team) & (self.stats_year.split_value == place)]
        stat_dict["fg3"] = team_stats.fg3.item()
        stat_dict["fg2"] = team_stats.fg2.item()
        stat_dict["ft"] = team_stats.ft.item()
        stat_dict["opp_fg3"] = team_stats.opp_fg3.item()
        stat_dict["opp_fg2"] = team_stats.opp_fg2.item()
        stat_dict["opp_ft"] = team_stats.opp_ft.item()

        return stat_dict

    def _calc_weight(self):
        """
        Calculates the weights, off_weight and def_weight, for the given season and sets them.
        Function is a setter
        """
        visitor_score_vs_ppg = self.game_stats.loc[
            self.game_stats.Playoffs == 0, ["Visitor_Pts", "pts_visitor",
                                            "opp_pts_home"]]
        home_score_vs_ppg = self.game_stats.loc[
            self.game_stats.Playoffs == 0, ["Home_Pts", "pts_home", "opp_pts_visitor"]]

        visitor_score_vs_ppg.columns = ["Score", "PTS", "Opp_PTS"]
        home_score_vs_ppg.columns = ["Score", "PTS", "Opp_PTS"]

        combined_score_vs_ppg = pd.concat(
            [visitor_score_vs_ppg, home_score_vs_ppg])

        X = combined_score_vs_ppg.loc[:, ["PTS", "Opp_PTS"]].to_numpy()
        Y = combined_score_vs_ppg.loc[:, "Score"].to_numpy()
        Y = Y.reshape(-1, 1)

        model = sm.OLS(Y, X)
        results = model.fit()

        self.off_weight = results.params[0]
        self.def_weight = results.params[1]

    def _sim_score(self, off_stats, def_stats):
        off_score = (3 * np.random.poisson(off_stats["fg3"])) + \
                    (2 * np.random.poisson(off_stats["fg2"])) + \
                    np.random.poisson(off_stats["ft"])
        def_score = (3 * np.random.poisson(def_stats["opp_fg3"])) + \
                    (2 * np.random.poisson(def_stats["opp_fg2"])) + \
                     np.random.poisson(def_stats["opp_ft"])
        weighted_score = (self.off_weight * off_score) + (self.def_weight * def_score)
        return weighted_score

    def _sim_game(self, home, visitor):
        home_stats = self._retrieve_stats(home, "Home")
        visitor_stats = self._retrieve_stats(visitor, "Visitor")
        self._calc_weight()

        home_wins = 0
        visitor_wins = 0

        for _ in range(self.game_iters):
            home_score = self._sim_score(off_stats=home_stats, def_stats=visitor_stats)
            visitor_score = self._sim_score(off_stats=visitor_stats, def_stats=home_stats)

            if home_score > visitor_score:
                home_wins += 1

            elif visitor_score > home_score:
                visitor_wins += 1

            elif home_score == visitor_score:
                # if a tie, choose a random winner
                coin_flip = np.random.binomial(1, 0.5)
                if coin_flip == 0:
                    home_wins += 1

                elif coin_flip == 1:
                    visitor_wins += 1

        home_win_pct = home_wins / (home_wins + visitor_wins)
        return home_win_pct

    def _sim_series(self):

        self.result_df = pd.DataFrame()
        team1_home_win_pct = self._sim_game(home=self.team1, visitor=self.team2)
        team1_visitor_win_pct = 1 - self._sim_game(home=self.team2,
                                                   visitor=self.team1)

        for _ in range(self.series_iters):
            team1_game_wins = 0
            team2_game_wins = 0
            games = 1

            while True:
                if games in [1, 2, 5, 7]:
                    team1_win = np.random.binomial(1, team1_home_win_pct)

                elif games in [3, 4, 6]:
                    team1_win = np.random.binomial(1, team1_visitor_win_pct)

                if team1_win == 1:
                    team1_game_wins += 1

                elif team1_win == 0:
                    team2_game_wins += 1

                if (team1_game_wins == 4 or team2_game_wins == 4) and self.num_games == 7:
                    break

                elif (team1_game_wins == 3 or team2_game_wins == 3) and self.num_games == 5:
                    break

                games += 1

            iter_winner = self.team1 if team1_game_wins > team2_game_wins else self.team2
            self.result_df = self.result_df.append({"winner": iter_winner, "games": games}, ignore_index=True)


class SimSeriesAll:
    def __init__(self, sim_round="all"):
        self.sim_round = sim_round
        self.series_df = join_playoff_standings()
        self.winner_accuracy = 0
        self.games_accuracy = 0

    def execute(self):
        self._select_round()
        self._sim_series_historic()

        self.winner_accuracy = sum(self.series_df.Correct_Winner) / self.series_df.shape[0]
        self.games_accuracy = sum(self.series_df.Correct_Games) / self.series_df.shape[0]

    def _select_round(self):
        if self.sim_round != "all":
            self.series_df = self.series_df.loc[self.series_df.Round.str.contains(self.sim_round)].reset_index(drop=True)

    def _sim_series_historic(self):
        for idx in self.series_df.index:

            series = self.series_df.iloc[idx]
            seed_winner = series.seed_winner
            seed_loser = series.seed_loser
            yr = series.YR

            if (seed_winner < seed_loser) and (series.Round != "Finals"):
                team1 = series.Winner
                team2 = series.Loser

            elif (seed_winner> seed_loser) and (series.Round != "Finals"):
                team1 = series.Loser
                team2 = series.Winner

            elif (series.Round == "Finals") and (series.Pct_winner > series.Pct_loser):
                team1 = series.Winner
                team2 = series.Loser

            elif (series.Round == "Finals") and (series.Pct_winner < series.Pct_loser):
                team1 = series.Loser
                team2 = series.Winner

            elif (series.Round == "Finals") and (series.Pct_winner == series.Pct_loser):
                # Special cases for 1990, 1998, 2001
                if (yr == 1990) or (yr == 2001):
                    team1 = series.Winner
                    team2 = series.Loser

                elif yr == 1998:
                    team1 = series.Loser
                    team2 = series.Winner

            if (yr < 2003) and ("First Round" in series.Round):
                num_games = 5

            else:
                num_games = 7

            sim = SimSeries(team1=team1, team2=team2, year=yr,
                            num_games=num_games)
            sim.execute()

            if sim.winner == series.Winner:
                self.winner_accuracy += 1
                self.series_df.loc[self.series_df.index == idx, "Correct_Winner"] = 1

            if sim.games == series.Games:
                self.games_accuracy += 1
                self.series_df.loc[self.series_df.index == idx, "Correct_Games"] = 1

            self.series_df.loc[self.series_df.index == idx, "Predicted_Winner"] = sim.winner
            self.series_df.loc[self.series_df.index == idx, "Predicted_Winner_Pct"] = sim.winner_pct

            if idx % 10 == 0:
                print(f"Done {idx}")
