import pandas as pd
import requests
from bs4 import BeautifulSoup
from queue import Queue
from threading import Thread


season_queue = Queue()
num_threads = 10


def months_in_season(yr):
    url = f"https://www.basketball-reference.com/leagues/NBA_{yr}_games-february.html"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    months_tag = soup.find("div", {"class": "filter"})
    months_in_season = months_tag.text.strip().lower().split("\n\n")

    return months_in_season


def scrape_one_season_games(q, i):
    while True:
        yr = q.get()

        print(f"Starting {yr} {i+1}")
        months = months_in_season(yr)

        df_season = pd.DataFrame(columns=["Date", "Visitor", "Visitor_Pts", "Home", "Home_Pts", "OT"])

        for m in months:
            url = f"https://www.basketball-reference.com/leagues/NBA_{yr}_games-{m}.html"
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            df_month = pd.read_html(str(soup.find_all("table")[0]))[0]

            if "Start (ET)" in df_month.columns:
                df_month = df_month.drop("Start (ET)", axis=1)

            # Some columns are empty and have hyperlinks, hence Del1, Del2, etc
            df_month.columns = ["Date", "Visitor", "Visitor_Pts", "Home",
                                "Home_Pts", "Del1", "Del_OT", "Del2",
                                "Del3"]

            # "OT" is 1 for overtime games, 0 for games that end in regulation
            df_month.loc[df_month["Del_OT"] == "OT", "OT"] = 1
            df_month.loc[df_month["Del_OT"].isna(), "OT"] = 0
            df_month = df_month.drop(["Del1", "Del2", "Del3", "Del_OT"],
                                     axis=1)
            df_season = df_season.append(df_month)

        # Playoffs=1 for playoff games, 0 for regular season games
        df_season = df_season.reset_index(drop=True)

        # 2020 has no playoffs as of yet
        if yr != 2020:

            # In 1980, the playoffs started in April
            if yr == 1980:
                df_season.loc[:, "Date"] = pd.to_datetime(df_season["Date"])
                df_season.loc[:, "Playoffs"] = 0
                df_season.loc[(df_season.Date.dt.month) == 4 & (df_season.Date.dt.month == 5), "Playoffs"] = 1

            else:
                playoffs_idx = df_season.loc[df_season["Date"] == "Playoffs"].index.item()
                playoffs_start_idx = playoffs_idx + 1

                # The row with "Playoffs" for everything needs to be deleted
                df_season = df_season.loc[df_season.Date != "Playoffs"]

                df_season.loc[df_season.index < playoffs_start_idx, "Playoffs"] = 0
                df_season.loc[df_season.index >= playoffs_start_idx, "Playoffs"] = 1

        else:
            # The row with "Playoffs" for everything needs to be deleted
            df_season = df_season.loc[df_season.Date != "Playoffs"]
            df_season.loc[:, "Playoffs"] = 0

        df_season.loc[:, "YR"] = yr

        df_season.to_csv("All_Games.csv", mode="a", header=False, index=False)


        print(f"Done {yr}")
        q.task_done()


def scrape_all_games():
    for i in range(num_threads):
        worker = Thread(target=scrape_one_season_games, args=(season_queue, i))
        worker.setDaemon(True)
        worker.start()

    for yr in range(1980, 2021):
        season_queue.put(yr)

    print("***Starting Process\n")
    season_queue.join()
    print("***Done")


def main():
    all_games = pd.DataFrame(columns=['Date', 'Visitor', 'Visitor_Pts', 'Home', 'Home_Pts', 'OT', "Playoffs", "YR"])
    all_games.to_csv("All_Games.csv", index=False)

    scrape_all_games()


if __name__=="__main__":
    main()





