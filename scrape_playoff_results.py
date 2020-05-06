import pandas as pd
import requests
from bs4 import BeautifulSoup
from queue import Queue
from threading import Thread
import time


standings_queue = Queue()
playoff_queue = Queue()
num_threads = 10


def scrape_single_standings(q):
    while True:
        yr = q.get()

        print(f"Starting {yr}")

        url = f"https://www.landofbasketball.com/yearbyyear/{yr-1}_{yr}_standings.htm"

        response = requests.get(url)
        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        west = pd.read_html(str(soup.findAll("table")[0]))[0]
        west.columns = list(west.iloc[0])
        west = west.iloc[1:]
        west = west.loc[:, ["Team", "W", "L", "Pct", "GB"]]
        west.loc[:, "Conference"] = "West"
        west.loc[:, "Seed"] = west.index
        west.loc[:, "YR"] = yr

        east = pd.read_html(str(soup.findAll("table")[1]))[0]
        east.columns = list(east.iloc[0])
        east = east.iloc[1:]
        east = east.loc[:, ["Team", "W", "L", "Pct", "GB"]]
        east.loc[:, "Conference"] = "East"
        east.loc[:, "Seed"] = east.index
        east.loc[:, "YR"] = yr

        combined_standings = pd.concat([west, east])
        combined_standings.to_csv("NBA_Standings.csv", mode="a", index=False, header=False)

        print(f"Done {yr}")
        q.task_done()


def scrape_all_standings():
    for i in range(num_threads):
        worker = Thread(target=scrape_single_standings, args=(standings_queue,))
        worker.setDaemon(True)
        worker.start()

    for yr in range(1984, 2021):
        standings_queue.put(yr)

    print("***Starting Standings Queue")
    standings_queue.join()
    print("***Done Standings Queue")


def scrape_single_playoffs(q):
    while True:
        yr = q.get()

        print(f"Starting {yr}")

        url = f"https://www.basketball-reference.com/leagues/NBA_{yr}.html"

        response = requests.get(url)
        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        playoffs_section = list(soup.find("div", {"id": "all_all_playoffs"}).children)[5]
        playoff_soup = BeautifulSoup(playoffs_section, "html.parser")

        playoff_df = pd.read_html(str(playoff_soup.findAll("table")[0]))[0]
        playoff_df.columns = ["Round", "Result", "del", "del", "del", "del"]
        playoff_df = playoff_df.loc[playoff_df["Result"].str.contains("over") == True, ["Round", "Result"]]

        playoff_df.Result = playoff_df.Result.str.split(" over ")
        playoff_df[["Winner", "Loser"]] = pd.DataFrame(
            playoff_df.Result.values.tolist(), index=playoff_df.index)
        playoff_df.loc[:, "Games"] = playoff_df.Loser.str[-2].astype(int) + playoff_df.Loser.str[-4].astype(int)
        playoff_df.loc[:, "YR"] = yr
        playoff_df = playoff_df.drop("Result", axis=1)
        playoff_df.loc[:, "Loser"] = playoff_df.loc[:, "Loser"].str.replace(r"\(.*\)", "").str.strip()

        playoff_df.to_csv("NBA_Playoffs.csv", mode="a", index=False, header=False)

        print(f"Done {yr}")
        q.task_done()


def scrape_all_playoffs():
    for i in range(num_threads):
        worker = Thread(target=scrape_single_playoffs, args=(playoff_queue,))
        worker.setDaemon(True)
        worker.start()

    for yr in range(1984, 2020):
        playoff_queue.put(yr)

    print("***Starting Playoff Queue")
    playoff_queue.join()
    print("***Done Playoff Queue")


def main():
    # standings = pd.DataFrame(columns=["Team", "W", "L", "Pct", "GB", "Conference", "YR", "seed"])
    # standings.to_csv("NBA_Standings.csv", index=False)
    # scrape_all_standings()

    playoff_df = pd.DataFrame(columns=["Round", "Winner", "Loser", "Games"])
    playoff_df.to_csv("NBA_Playoffs.csv", index=False)
    scrape_all_playoffs()


if __name__ == "__main__":
    main()








