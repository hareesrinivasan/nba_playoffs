from preprocessing_data import join_games_splits, join_playoff_standings
import statsmodels.api as sm
import numpy as np
import pandas as pd
import pyspark

class SimSeriesAll:
    def __init__(self, series_iters=15000, game_iters=15000, sim_round="all"):
        self.series_iters = series_iters
        self.game_iters = game_iters
        self.sim_round = sim_round
        self.series_df = join_playoff_standings()
        self.game_stats = join_games_splits()
        self.splits_data = pd.read_csv("data/Home_Visitor_Splits.csv")
        self.splits_data = self.splits_data.drop_duplicates()
        self.winner_accuracy = 0

    def execute(self):
        self._select_round()
        self._compute_season_weights()
        self._create_rdd()
        self._sim_all_series()

        # self.winner_accuracy = sum(self.series_df.Correct_Winner) / self.series_df.shape[0]
        # self.games_accuracy = sum(self.series_df.Correct_Games) / self.series_df.shape[0]

    def _select_round(self):
        if self.sim_round != "all":
            self.series_df = self.series_df.loc[self.series_df.Round.str.contains(self.sim_round)].reset_index(drop=True)

    def _compute_season_weights(self):
        """
        Calculates the off and def weights for a given season
        Returns a dictionary: key - year, value - (off_weight, def_weight)
        """
        self.weights_dict = {}
        for yr in self.series_df.YR.unique():
            visitor_score_vs_ppg = self.game_stats.loc[
                (self.game_stats.Playoffs == 0) & (self.game_stats.YR == yr),
                ["Visitor_Pts", "pts_visitor", "opp_pts_home"]]
            home_score_vs_ppg = self.game_stats.loc[
                (self.game_stats.Playoffs == 0) & (self.game_stats.YR == yr),
                ["Home_Pts", "pts_home", "opp_pts_visitor"]]

            visitor_score_vs_ppg.columns = ["Score", "PTS", "Opp_PTS"]
            home_score_vs_ppg.columns = ["Score", "PTS", "Opp_PTS"]

            combined_score_vs_ppg = pd.concat(
                [visitor_score_vs_ppg, home_score_vs_ppg])

            X = combined_score_vs_ppg.loc[:, ["PTS", "Opp_PTS"]].to_numpy()
            Y = combined_score_vs_ppg.loc[:, "Score"].to_numpy()
            Y = Y.reshape(-1, 1)

            model = sm.OLS(Y, X)
            results = model.fit()

            off_weight = results.params[0]
            def_weight = results.params[1]

            self.weights_dict[yr] = (off_weight, def_weight)

    def _create_rdd(self):
        self.rdd_as_list = []
        for idx in self.series_df.index:

            series = self.series_df.iloc[idx]
            seed_winner = series.seed_winner
            seed_loser = series.seed_loser
            yr = series.YR

            if (seed_winner < seed_loser) and (series.Round != "Finals"):
                team1 = series.Winner
                team2 = series.Loser

            elif (seed_winner > seed_loser) and (series.Round != "Finals"):
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

            split_stats = self._retrieve_data(yr, team1, team2)
            off_weight = self.weights_dict[yr][0]
            def_weight = self.weights_dict[yr][1]

            for series_id in range(self.series_iters):
                for game_id in range(self.game_iters):
                    for game in range(1, num_games + 1):
                        if game in [1, 2, 5, 7]:
                            self.rdd_as_list.append([yr, team1, team2, series_id,
                                                     game_id, game, split_stats,
                                                     off_weight, def_weight])
                        elif game in [3, 4, 6]:
                            self.rdd_as_list.append([yr, team2, team1, series_id,
                                                     game_id, game, split_stats,
                                                     off_weight, def_weight])

    def _retrieve_data(self, year, team1, team2):
        two_team_splits = (self.splits_data.loc[(self.splits_data.yr == year) &
                                      ((self.splits_data.team == team1) |
                                       (self.splits_data.team == team2))])
        return two_team_splits

    def _sim_all_series(self):
        sc = pyspark.SparkContext('local[*]')
        results_rdd = sc.parallelize(self.rdd_as_list)
        # t = self.rdd_as_list[0]
        # print(self._single_game_sim(t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7], t[8]))

        results_rdd = results_rdd.map(lambda x: self._single_game_sim(x[0], x[1], x[2],
                                                                   x[3], x[4], x[5],
                                                                   x[6], x[7], x[8]))
        results_rdd.saveAsTextFile("output")
        # print(results_rdd.take(1))
        sc.stop()

    def _single_game_sim(self, yr, home, visitor, series_id, game_id, game, split_stats, off_weight, def_weight):
        home_stats = split_stats.loc[(split_stats.split_value == "Home") & (split_stats.team == home)]
        visitor_stats = split_stats.loc[(split_stats.split_value == "Visitor") & (split_stats.team == visitor)]

        home_score = self._sim_score(home_stats, visitor_stats, off_weight, def_weight)
        visitor_score = self._sim_score(visitor_stats, home_stats, off_weight, def_weight)

        if home_score > visitor_score:
            return [yr, home, visitor, series_id, game_id, game, split_stats,
                    off_weight, def_weight, home_score, visitor_score,
                    home, visitor]

        elif visitor_score > home_score:
            return [yr, home, visitor, series_id, game_id, game, split_stats,
                    off_weight, def_weight, home_score, visitor_score,
                    visitor, home]

        elif home_score == visitor_score:
            # If tied, pick random winner
            coin_flip = np.random.binomial(1, 0.5)
            if coin_flip == 0:
                return [yr, home, visitor, series_id, game_id, game,
                        split_stats, off_weight, def_weight, home_score,
                        visitor_score, home, visitor]

            elif coin_flip == 1:
                return [yr, home, visitor, series_id, game_id, game,
                        split_stats,
                        off_weight, def_weight, home_score, visitor_score,
                        visitor, home]

    def _sim_score(self, off_stats, def_stats, off_weight, def_weight):
        off_score = (3 * np.random.poisson(off_stats.fg3)) + \
                    (2 * np.random.poisson(off_stats.fg2)) + \
                    np.random.poisson(off_stats.ft)
        def_score = (3 * np.random.poisson(def_stats.opp_fg3)) + \
                    (2 * np.random.poisson(def_stats.opp_fg2)) + \
                     np.random.poisson(def_stats.opp_ft)
        weighted_score = (off_weight * off_score) + (def_weight * def_score)
        return weighted_score.item()


if __name__=="__main__":
    c = SimSeriesAll(game_iters=15000, series_iters=15000)
    c.execute()
    # d = c.raw_results
    print("Done")
