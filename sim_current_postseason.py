from preprocessing_data import *
from postseason_sim import SimSeries

class SimPlayoffs:
    def __init__(self, standings, year):
        self.standings = standings
        self.year = year

    def execute(self):
        self._find_seeds()
        self._first_round()
        self._second_round()
        self._championship_round()
        self._finals()

    def _find_seeds(self):
        west_standings = self.standings.loc[self.standings.Conference == "West"]
        east_standings = self.standings.loc[self.standings.Conference == "East"]

        self.west_seeds = {seed: west_standings.loc[west_standings.seed == seed, "Team"].item() for seed in range(1, 9)}
        self.east_seeds = {seed: east_standings.loc[east_standings.seed == seed, "Team"].item() for seed in range(1, 9)}

    def _first_round(self):
        east1_8 = SimSeries(self.east_seeds[1], self.east_seeds[8], self.year)
        east1_8.execute()
        self.east1_8_winner = east1_8.winner

        east2_7 = SimSeries(self.east_seeds[2], self.east_seeds[7], self.year)
        east2_7.execute()
        self.east2_7_winner = east2_7.winner

        east3_6 = SimSeries(self.east_seeds[3], self.east_seeds[6], self.year)
        east3_6.execute()
        self.east3_6_winner = east3_6.winner

        east4_5 = SimSeries(self.east_seeds[4], self.east_seeds[5], self.year)
        east4_5.execute()
        self.east4_5_winner = east4_5.winner

        west1_8 = SimSeries(self.west_seeds[1], self.west_seeds[8], self.year)
        west1_8.execute()
        self.west1_8_winner = west1_8.winner

        west2_7 = SimSeries(self.west_seeds[2], self.west_seeds[7], self.year)
        west2_7.execute()
        self.west2_7_winner = west2_7.winner

        west3_6 = SimSeries(self.west_seeds[3], self.west_seeds[6], self.year)
        west3_6.execute()
        self.west3_6_winner = west3_6.winner

        west4_5 = SimSeries(self.west_seeds[4], self.west_seeds[5], self.year)
        west4_5.execute()
        self.west4_5_winner = west4_5.winner

        print(self.east1_8_winner, self.east2_7_winner, self.east3_6_winner,
              self.east4_5_winner, self.west1_8_winner, self.west2_7_winner,
              self.west3_6_winner, self.west4_5_winner)

    def _second_round(self):
        east1_8_4_5 = SimSeries(self.east1_8_winner, self.east4_5_winner, self.year)
        east1_8_4_5.execute()
        self.east_finalist1 = east1_8_4_5.winner

        east2_7_3_6 = SimSeries(self.east2_7_winner, self.east3_6_winner, self.year)
        east2_7_3_6.execute()
        self.east_finalist2 = east2_7_3_6.winner

        west1_8_4_5 = SimSeries(self.west1_8_winner, self.west4_5_winner, self.year)
        west1_8_4_5.execute()
        self.west_finalist1 = west1_8_4_5.winner

        west2_7_3_6 = SimSeries(self.west2_7_winner, self.west3_6_winner, self.year)
        west2_7_3_6.execute()
        self.west_finalist2 = west2_7_3_6.winner

        print(self.east_finalist1, self.east_finalist2, self.west_finalist1, self.west_finalist2)

    def _championship_round(self):
        east = SimSeries(self.east_finalist1, self.east_finalist2, self.year)
        east.execute()
        self.east_champ = east.winner

        west = SimSeries(self.west_finalist1, self.west_finalist2, self.year)
        west.execute()
        self.west_champ = west.winner

        print(self.east_champ, self.west_champ)

    def _finals(self):
        finals = SimSeries(self.east_champ, self.west_champ, self.year)
        finals.execute()
        self.nba_champion = finals.winner
        print(self.nba_champion)