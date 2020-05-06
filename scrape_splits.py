import pandas as pd
import requests
from bs4 import BeautifulSoup
from queue import Queue
from threading import Thread
import time


abbrev_queue = Queue()
split_queue = Queue()
num_threads = 4


def scrape_single_abbrev(q):
    while True:
        yr = q.get()
        print(f"Starting {yr}")

        url = f"https://www.basketball-reference.com/leagues/NBA_{yr}.html"

        response = requests.get(url)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")

        team_tags = soup.find_all("th", {"class": "left", "scope": "row",
                                         "data-stat": "team_name"})

        abbrev_df = pd.DataFrame(columns=["Team", "YR", "Abbrev"])
        for tag in team_tags:
            append_dict = {}
            a_tag = tag.findChildren("a")[0]
            link_parts = a_tag.attrs["href"].split("/")
            append_dict["Team"] = a_tag.text
            append_dict["Abbrev"] = link_parts[2]
            append_dict["YR"] = yr

            assert str(yr) == link_parts[3][:4]

            abbrev_df = abbrev_df.append(append_dict, ignore_index=True)

        abbrev_df.to_csv("Team_Abbreviations.csv", mode="a", index=False, header=False)

        print(f"Done {yr}")

        time.sleep(1)

        q.task_done()


def scrape_all_abbrev():
    for i in range(num_threads):
        worker = Thread(target=scrape_single_abbrev, args=(abbrev_queue,))
        worker.setDaemon(True)
        worker.start()

    for yr in range(1984, 2021):
        abbrev_queue.put(yr)

    print("***Starting Abbreviation Queue")
    abbrev_queue.join()
    print("***Done Abbreviation Queue")


def scrape_single_team_splits(q):
    while True:
        team, yr, abbrev = q.get()
        print(f"Starting {abbrev}, {yr}")

        url = f"https://www.basketball-reference.com/teams/{abbrev}/{yr}/splits/"

        response = requests.get(url)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")

        split_df = pd.DataFrame(columns=["split_value", "g", "wins", "losses",
                                         "fg", "fga", "fg3", "fg3a", "ft",
                                         "fta", "orb", "trb", "ast", "stl",
                                         "blk", "tov", "pf", "pts", "opp_fg",
                                         "opp_fga", "opp_fg3", "opp_fg3a",
                                         "opp_ft", "opp_fta", "opp_orb",
                                         "opp_trb", "opp_ast", "opp_stl",
                                         "opp_blk", "opp_tov", "opp_pf",
                                         "opp_pts"])

        # home_split_tag = soup.findAll("tr")[4].findChildren("td")
        # road_split_tag = soup.findAll("tr")[5].findChildren("td")

        for tags in soup.findAll("tr")[4:6]:
            tag = tags.findChildren("td")
            append_dict = {}
            for t in tag:
                col = t.attrs["data-stat"]
                val = t.text
                append_dict[col] = val

            # Replace "Road" with "Visitor" for consistency
            if append_dict["split_value"] == "Road":
                append_dict["split_value"] = "Visitor"
            split_df = split_df.append(append_dict, ignore_index=True)

        split_df.loc[:, "fg2"] = split_df["fg"].astype(float) - split_df["fg3"].astype(float)
        split_df.loc[:, "opp_fg2"] = split_df["opp_fg"].astype(float) - split_df["opp_fg3"].astype(float)

        split_df["team"] = team
        split_df["yr"] = yr
        split_df["abbrev"] = abbrev


        split_df.to_csv("Home_Visitor_Splits.csv", mode="a", index=False, header=False)
        print(f"Done {abbrev}, {yr}")


        q.task_done()


def scrape_all_splits():
    for i in range(num_threads):
        worker = Thread(target=scrape_single_team_splits, args=(split_queue,))
        worker.setDaemon(True)
        worker.start()

    abbrev_df = pd.read_csv("Team_Abbreviations.csv")
    for idx in range(abbrev_df.shape[0]):
        row = abbrev_df.iloc[idx]

        team = row["Team"]
        yr = row["YR"]
        abbrev = row["Abbrev"]
        split_queue.put((team, yr, abbrev))

    print("***Starting Split Queue")
    split_queue.join()
    print("***Done Split Queue")


def main():
    # abbrev_df = pd.DataFrame(columns=["Team", "YR", "Abbrev"])
    # abbrev_df.to_csv("Team_Abbreviations.csv", index=False)
    # scrape_all_abbrev()

    split_df = pd.DataFrame(columns=["split_value", "g", "wins", "losses",
                                         "fg", "fga", "fg3", "fg3a", "ft",
                                         "fta", "orb", "trb", "ast", "stl",
                                         "blk", "tov", "pf", "pts", "opp_fg",
                                         "opp_fga", "opp_fg3", "opp_fg3a",
                                         "opp_ft", "opp_fta", "opp_orb",
                                         "opp_trb", "opp_ast", "opp_stl",
                                         "opp_blk", "opp_tov", "opp_pf",
                                         "opp_pts", "fg2", "opp_fg2", "team",
                                         "yr", "abbrev"])
    split_df.to_csv("Home_Visitor_Splits.csv", index=False)
    scrape_all_splits()


if __name__ == "__main__":
    main()