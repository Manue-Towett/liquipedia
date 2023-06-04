import json
import os
import random
import re
import threading
from datetime import date
from queue import Queue

import pandas as pd
import requests
from bs4 import BeautifulSoup
from utils import ProxyHandler, Logger

BASE_URL = "https://liquipedia.net"

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json; charset=UTF-8",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
}


class APScraper:
    """Scraper for https://liquipedia.net/valorant/Portal:Statistics"""
    requests.packages.urllib3.disable_warnings()

    def __init__(self) -> None:
        super().__init__()

        settings_file = open("./settings/settings.json", "r")
        settings = json.load(settings_file)
        settings_file.close()

        self.thread_num = settings["thread_num"]
        _output_dir = settings["output_file_path"]
        self.output_path = f"{_output_dir}/active_players_{date.today()}.xlsx"

        if not os.path.exists(_output_dir):
            os.makedirs(_output_dir)

        self.active_players, self.crawled = [], []
        self.queue  = Queue()

        self.logger = Logger("APScraper")
        self.logger.info("==== Active Players Scraper Started ====")

    def extract_active_players_rows(self, soup:BeautifulSoup,name: str) -> None:
        """
        Extracts active players slugs from the Active Players table

        :param soup: beautifulsoup object of the html response
        :param name: the name of the organization from which active players
        are to be scraped
        """
        for row in soup.find_all("tr", {"class": "Player"}):
            url_span_tag = row.find("span", {"class": "inline-player"})
            row_data, row_dict = [], {"Organization": name}

            row_data = row.find("td", {"class":"ID"})

            row_dict["ID"] = row_data.get_text(strip=True)

            row_dict["player_url"] = BASE_URL + row_data.a["href"]

            self.active_players.append(row_dict)

    def find_top_twenty(self) -> list[dict]:
        """
        Finds the current top 20 organizations from liquipedia
        """
        url = "https://liquipedia.net/valorant/Portal:Statistics"

        while True:
            try:
                response = requests.get(url, headers=HEADERS)

                if response.status_code == 200:
                    break

            except:pass

        soup = BeautifulSoup(response.text, "html.parser")

        for table in soup.select("div.divTable"):

            if re.search("organization", str(table).lower()):
                top_20_table = table

        top_orgs = self.extract_top_organizations(top_20_table)

        return top_orgs

    def extract_top_organizations(self, table: BeautifulSoup) -> list[dict]:
        """
        Extract table slugs from top 20 organizations in liquipedia
        
        :param table: a beautifulsoup object representing the top 20
        organizations table
        """
        top_organizations, headings = [], []

        for heading in table.select("div.divHeaderRow div"):
            
            if heading.text.strip():
                headings.append(heading.get_text(strip=True))

        for row in table.select("div.divRow"):
            row_dict, row_data = {}, []

            for row_cell in row.select("div.divCell"):

                if row_cell.text.strip():

                    if row_cell.find("span"):
                        row_dict["active_url"] = BASE_URL + row_cell.a["href"]

                    row_data.append(row_cell.get_text(strip=True))

            for heading, value in zip(headings, row_data):
                row_dict[heading] = value

            top_organizations.append(row_dict)

        return top_organizations

    def fetch_active_players(self, url: str, name: str) -> None:
        """
        Fetches the active players page from a given organization url

        :param url: the url to given organization on liquipedia
        :param name: the name of the given organization
        """
        
        while True:
            proxy = {"https": f"http://{random.choice(self.proxies)}"}

            try:
                response = requests.get(url, verify=False, 
                                        proxies=proxy, timeout=15)

                soup = BeautifulSoup(response.text, "html.parser")
                tables = soup.select("table")

                if response.status_code == 200 and len(tables):
                    break

            except:pass

        self.logger.info(f"Tables found >>> {name}: {len(tables)}")

        for table in tables:

            if re.search("active squad", str(table).lower()):
                active_table = table

                self.extract_active_players_rows(active_table, name)

    def create_thread_jobs(self, links: list, names: list) -> None:
        """
        Create scraping jobs for threads
        
        :param links: a list of links to be put on the queue
        :param names: a list of top 20 organizations to be put on the queue
        """
        [self.queue.put((link, name, links)) for link, name in zip(links, names)]
        self.queue.join()

    def work(self) -> None:
        """calls the active players fetching function with threads"""

        while True:
            link, name, links = self.queue.get()
            self.fetch_active_players(link, name)

            links.remove(link)
            self.crawled.append(link)

            self.logger.info(
                f"Queue: {len(links)} || Crawled: {len(self.crawled)}"
            )

            self.queue.task_done()

    def append_to_excel(self) -> None:
        """Saves data to excel"""
        self.logger.info("Finished scraping. Saving to excel...")

        df = pd.DataFrame.from_dict(self.active_players)
        active_players = df[["Organization", "ID", "player_url"]]
        active_players.to_excel(self.output_path, index=False)

        self.logger.info("Records saved!")

    def run(self) -> None:
        """Entry point to the scraper"""
        urls, names = [], []

        proxy_handler = ProxyHandler()
        proxy_handler.get_proxies()
        
        self.proxies = proxy_handler.proxies

        for organization in self.find_top_twenty():
            names.append(organization["Organization"])

            urls.append(organization["active_url"])

        [threading.Thread(target=self.work, daemon=True).start()
         for _ in range(self.thread_num)]

        self.create_thread_jobs(urls, names)

        self.append_to_excel()


if __name__ == "__main__":
    scraper = APScraper()
    scraper.run()