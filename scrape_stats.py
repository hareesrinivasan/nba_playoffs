import pandas as pd
import requests
from bs4 import BeautifulSoup
from queue import Queue
from threading import Thread


off_stat_queue = Queue()
def_stat_queue = Queue()
num_threads = 4

def one_season_stats(q, tag):
    """
    Scrapes stats for every team from each season and appends stats to the
    output csv.
    """
    while True:
        yr = q.get()
        print(f"Starting {yr}")

        if tag == "all_team-stats-per_poss":
            table_loc = 20

        elif tag == "all_opponent-stats-per_poss":
            table_loc = 16

        response = requests.get(f"https://www.basketball-reference.com/leagues/NBA_{yr}.html")
        html = response.content
        outer_soup = BeautifulSoup(html, "html.parser")

        # For some reason, the data frame needed cannot be scraped from the
        # first beautiful soup object, so another soup object is created
        outer_tag = outer_soup.find_all('div', {'id': tag})[0]
        inner_html = str(list(outer_tag.descendants)[table_loc])
        inner_soup = BeautifulSoup(inner_html, "html.parser")

        stats_yr = pd.read_html(str(inner_soup.find_all("table")[0]))[0]
        stats_yr.loc[:, 'YR'] = int(yr)
        stats_yr['Team'] = stats_yr['Team'].str.replace("*", "")

        if tag == "all_team-stats-per_poss":
            stats_yr.to_csv("Offensive_Stats.csv", mode="a", header=False, index=False)

        elif tag == "all_opponent-stats-per_poss":
            stats_yr.to_csv("Defensive_Stats.csv", mode="a", header=False, index=False)

        print(f"Done {yr}")

        q.task_done()


def scrape_off_stats():
    """
    Creates worker queue to call function to scrape each season individually.
    """
    for i in range(num_threads):
        tag = "all_team-stats-per_poss"
        worker = Thread(target=one_season_stats, args=(off_stat_queue, tag))
        worker.setDaemon(True)
        worker.start()

    for yr in range(1980, 2021):
        off_stat_queue.put(yr)

    print("***Starting Offensive Stats\n")
    off_stat_queue.join()
    print("***Done Offensive Stats")


def scrape_def_stats():
    for i in range(num_threads):
        tag = "all_opponent-stats-per_poss"
        worker = Thread(target=one_season_stats, args=(def_stat_queue, tag))
        worker.setDaemon(True)
        worker.start()

    for yr in range(1980, 2021):
        def_stat_queue.put(yr)

    print("***Starting Defensive Stats\n")
    def_stat_queue.join()
    print("***Done Defensive Stats")


def main():
    off_stats = pd.DataFrame(
        columns=['Rk', 'Team', 'G', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA',
                 '3P%', '2P', '2PA', '2P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB',
                 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'YR'])
    off_stats.to_csv("Offensive_Stats.csv", index=False)

    def_stats = pd.DataFrame(
        columns=['Rk', 'Team', 'G', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA',
                 '3P%', '2P', '2PA', '2P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB',
                 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'YR'])
    def_stats.to_csv("Defensive_Stats.csv", index=False)

    scrape_off_stats()
    scrape_def_stats()


if __name__=="__main__":
    main()

