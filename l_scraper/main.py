import json
import os
import random
import threading
from datetime import date
from queue import Queue

import pandas as pd
import requests
from bs4 import BeautifulSoup
from utils import CSVHandler, ImageHandler, Logger, ProxyHandler

IGNORE_HEADING_LIST = [
    "mouse settings", "hardware", "crosshair settings", "last updated"
]

COLUMN_HEADERS = [
    "ID", "Name:", "Romanized Name:","Nationality:", "Born:", "Status:",
    "Years Active (Player):", "Team:","Approx. Total Winnings:", 
    "Profile URL", "faceit",	"twitter",	"twitch",	"youtube",
    "Mouse",	"eDPI",	"DPI",	"Polling Rate", "Sensitivity",	"Zoom",	
    "Raw Input", "Curvature", "Circumference",	"Mouse Setup", "Raw.",	
    "Mousepad",	"Monitor", "Refresh rate",	"In-game resolution",
    "Keyboard", "Headset",	"Color",	"Outlines", "Center Dot", "MoveErr",
    "FiringErr", "Fade",	"Inner Lines", "Alternate IDs:", "instagram",
    "steam",	"Main Agents:",	"esea", "facebook",	"Role:", "Scaling",	
    "tiktok", "reddit",	"bilibili",	"vk",	"Pointer Speed",
    "Outer Lines",	"esl",	"5ewin"]

class LiquipediaScraper:
    requests.packages.urllib3.disable_warnings()

    def __init__(self) -> None:
        settings_file = open("./settings/settings.json", "r")
        settings = json.load(settings_file)
        settings_file.close()

        self.thread_num = settings["thread_num"]
        self._input_file_path = settings["input_file_path"]
        _output_dir = settings["output_file_path"]
        self.output_path = f"{_output_dir}/scraped_data_{date.today()}.xlsx"
        self.images_path = settings["image_dir"]

        if not os.path.exists(_output_dir):
            os.makedirs(_output_dir)

        self.profiles, self.history, self.achievements = [], [], []
        self.queue, self.images_queue = Queue(), Queue()
        self.images, self.crawled = [], []

        self.logger = Logger(__class__.__name__)

        self.logger.info("==== Liquipedia scraper started ====")

    def sort_tables(self, soup:BeautifulSoup, w_tables:list, name:str) -> None:
        """
        sorts the tables into history, achievements and settings.

        :param soup: a BeautifulSoup object of html page returned from the server
        :param w_tables: empty list to which tables of class "wikitable" are 
        appended
        :param name: the name of the player
        """
        for table in soup.select("table"):
            try:
                _class = table.attrs["class"]

                if len(_class) == 1 and _class[0] == "wikitable":
                    w_tables.append(table)
                elif "wikitable-striped" in _class:
                    self.extract_achievements(table, name)
                
            except:
                pass
        
        try:
            history_rows = soup.select("table")[0].select("tr")
            self.extract_history(history_rows, name)

        except:
            self.logger.info(f"History table for {name} not found!!!")
    
    def extract_bio(self, soup:BeautifulSoup, data_dict:dict) -> str:
        """
        Extracts a given players general bio including name

        :param soup: a BeautifulSoup object of html page returned from the server
        :param data_dict: a dictionary to store player's bio
        
        :return name: a string representing name of a player
        """
        keys, values = [], []

        try:
            name = soup.find("h1", {"id":"firstHeading"}).get_text(strip=True)
            data_dict["ID"] = name

        except:
            self.logger.info("Could not find player's bio. Retrying...")
            return

        bio_list = soup.select("div.infobox-cell-2")

        for (item, index) in zip(bio_list, range(len(bio_list))):
            _text = item.get_text(strip=True)

            if index % 2 == 0:
                keys.append(_text.replace("\xa0", " "))
            else:
                values.append(_text.replace("\xa0", " "))

        for (key, value) in (zip(keys, values)):
            data_dict[key] = value
        
        self.logger.info(f"Extracting player slugs for >>> {name}")

        return name

    def extract_external_links(self, soup:BeautifulSoup, data_dict:dict) -> None:
        """
        Extracts social media links from the player profile
        
        :param soup: a BeautifulSoup object of html page returned from the server
        :param data_dict: a dictionary to store player's social media links
        """
        try:
            external_tags = soup.find("div", {"class":"infobox-center infobox-icons"})
            
            for link in external_tags.select("a.external"):
                href = link.attrs["href"]
                site_name = link.find("i")["class"][1].replace("lp-", "")
                data_dict[site_name] = href

        except:pass

    def extract_history(self, history_rows:BeautifulSoup, name:str) -> None:
        """
        Extracts player's history from the history table

        :param history_rows: table rows from the history table
        :param name: player's name
        """
        
        for row in history_rows:
            timeframe = row.find("td", {"class":"th-mono"}).get_text(strip=True)
            _from, _to = timeframe.split("—")
            team = row.find("a")["title"]
            history = {
                "ID": name, "From":_from, "To": _to, "Team": team
            }
            self.history.append(history)
    
    def extract_settings(self, s_tables:BeautifulSoup, data_dict:dict) -> None:
        """
        Extracts data from the wikitables containing player settings

        :param s_tables: a list containing tables(BeautifulSoup objects) with 
        setting data
        :param data_dict: dictionary to store player's information
        """
        headings, values = [], []

        for table in s_tables:

            for heading in table.select("th"):
                header_text = heading.get_text(strip=True)
                is_title = False

                for heading in IGNORE_HEADING_LIST:
                    if heading in header_text.lower():
                        is_title = True
                        break

                if not is_title:
                    headings.append(header_text)

            for value in table.select("td"):
                value_text = value.get_text(strip=True)

                if value_text:
                    values.append(value_text)

            for (key, value) in (zip(headings, values)):
                data_dict[key] = value

        self.profiles.append(data_dict) 

    def extract_achievements(self, table:BeautifulSoup, name:str) -> None:
        """
        Extracts player  achievement slugs from the achievements table
        
        :param table: a beautifulsoup object containing the achivements slugs
        :param name: players name
        """
        headings, rows = [], []

        for heading in table.select("th"):
            header_text = heading.get_text(separator=" ")

            if not "complete list" in header_text.lower():
                headings.append(header_text)

        headings.insert(-1, "Team 2")

        for row in table.select("tbody tr"):
            data = []

            for value in row.select("td"):
                value_text = value.get_text(separator=" ", strip=True)

                if value_text not in data:
                    data.append(value_text.replace("\xa0", ""))

            if len(data):
                team_2 = row.find("td",{"class":"results-team-icon"})
                data[-2] = team_2.find("img")["alt"]
                rows.append(data)

        for row in rows:
            row_dict = {"ID":name}

            for (heading, value) in (zip(headings, row)):
                row_dict[heading] = value

            if len(row_dict):
                self.achievements.append(row_dict)

    def request_page(self, link:str) -> BeautifulSoup:
        """
        Fetches a player profile from a given url
        
        :param link: the link to the player's profile on Liquipedia
        """
        while True:
            try:
                proxy = {"https":f"http://{random.choice(self.proxies)}"}
                response = requests.get(link, proxies=proxy, timeout=10)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                else:continue

                return soup

            except:pass  

    def extract_slugs(self, link:str, data_dict:dict) -> tuple:
        """Calls the functions to extract profiles, history and achivements"""
        wikitables, name =  [], ""

        while not name:
            soup = self.request_page(link)
            name = self.extract_bio(soup, data_dict)

        data_dict["Profile URL"] = link

        self.extract_external_links(soup, data_dict)
        self.sort_tables(soup, wikitables, name)
        self.extract_settings(wikitables, data_dict)

        return soup, name


    def work(self) -> None:
        """Fetches a link from the queue and scrapes the player profile"""
        while True:
            link, data_dict, profiles = self.queue.get()
            soup, name = self.extract_slugs(link, data_dict)

            self.create_image_jobs(soup, name)

            profiles.remove(link)
            self.crawled.append(link)

            self.logger.info(
                f"Queue: {len(profiles)} | Crawled: {len(self.crawled)} | "
                f"Downloaded images: {len(self.images)}")

            self.queue.task_done()

    def create_image_jobs(self, soup:BeautifulSoup, name:str) -> None:
        """Create jobs for image scraping threads"""
        self.images_queue.put((soup, name))
        self.images_queue.join()
    
    def create_thread_jobs(self) -> None:
        """Create scraping jobs for threads"""
        df = pd.read_excel(
            self._input_file_path, sheet_name="List of Profiles")
        links = df["Link"].to_list()

        [self.queue.put((link, dict(), links)) for link in links]
        self.queue.join()
    
    def run(self) -> None:
        """Entry point to the scraper"""
        proxy_handler = ProxyHandler()
        proxy_handler.get_proxies()
        
        self.proxies = proxy_handler.proxies

        image_handler = ImageHandler(self.images, self.images_queue, 
                                     self.images_path, self.proxies)

        for _ in range(self.thread_num):
            threading.Thread(target=self.work, daemon=True).start()

            threading.Thread(
                target=image_handler.work, daemon=True).start()

        self.create_thread_jobs()

        csv_handler = CSVHandler(COLUMN_HEADERS, 
                                 self.profiles, self.history, 
                                 self.achievements, self.output_path)
        
        csv_handler.save_to_excel()


if __name__ == "__main__":
    scraper = LiquipediaScraper()
    scraper.run()