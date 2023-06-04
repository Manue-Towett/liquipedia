import os, sys, json, requests, threading, logging, random, shutil
import urllib.request
import pandas as pd
from bs4 import BeautifulSoup
from queue import Queue
from datetime import date

IGNORE_HEADING_LIST = [
    "mouse settings", "hardware", "crosshair settings", "last updated"
]

COLUMN_HEADERS = ["ID", "Name:", "Romanized Name:",
                  "Nationality:", "Born:", "Status:",
                  "Years Active (Player):", "Team:",
                  "Approx. Total Winnings:", "Profile URL",
                  "faceit",	"twitter",	"twitch",	"youtube",
                  "Mouse",	"eDPI",	"DPI",	"Polling Rate",
                  "Sensitivity",	"Zoom",	"Raw Input",
                  "Curvature", "Circumference",	"Mouse Setup",
                  "Raw.",	"Mousepad",	"Monitor",
                  "Refresh rate",	"In-game resolution",
                  "Keyboard", "Headset",	"Color",	"Outlines",
                  "Center Dot",	"MoveErr",	"FiringErr",
                  "Fade",	"Inner Lines", "Alternate IDs:",
                  "instagram",	"steam",	"Main Agents:",	"esea",
                  "facebook",	"Role:",	"Scaling",	"tiktok",
                  "reddit",	"bilibili",	"vk",	"Pointer Speed",
                  "Outer Lines",	"esl",	"5ewin"
                  ]

class LiquipediaScraper:
    if not os.path.exists("./logs/"):os.makedirs("./logs/")
    requests.packages.urllib3.disable_warnings()
    logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        handlers=[logging.FileHandler(
                  f"./logs/liquipedia_scraper_log_{date.today()}.log"), 
        logging.StreamHandler(sys.stdout)])
    logging.info(" ========= Liquipedia Scraper Started =========")


    def __init__(self) -> None:
        settings_file = open("./settings/settings.json", "r")
        settings = json.load(settings_file)
        settings_file.close()
        self.thread_num = settings["thread_num"]
        self._input_file_path = settings["input_file_path"]
        _output_dir = settings["output_file_path"]
        if not os.path.exists(_output_dir):os.makedirs(_output_dir)
        self.images_path = settings["image_dir"]
        self.profiles, self.history, self.achievements = [], [], []
        self.output_path = f"{_output_dir}/scraped_data_{date.today()}.xlsx"
        self.queue, self.images_queue, self.crawled = Queue(), Queue(), []
        self.proxies, self.images = [], []
    

    def sort_tables(
        self, soup:BeautifulSoup, wikitables:list, name:str) -> None:
        """Sorts the table into history, settings and achievements tables"""
        tables = soup.select("table")
        for table in tables:
            try:
                _class = table.attrs["class"]
                if len(_class) == 1 and _class[0] == "wikitable":
                    wikitables.append(table)
                elif "wikitable-striped" in _class:
                    self.extract_achievements(table, name)
            except:
                player_history_rows = tables[0].select("tr")
                self.extract_player_history(player_history_rows, name)


    def extract_player_info(
        self, soup:BeautifulSoup, data_dict:dict) -> str:
        """Extracts player's bio"""
        keys, values = [], []
        try:
            name = soup.find(
                "h1", {"id":"firstHeading"}).get_text(strip=True)
        except:return
        data_dict["ID"] = name
        player_info_list = soup.select("div.infobox-cell-2")
        for (item, index) in (
            zip(player_info_list, range(len(player_info_list)))):
            _text = item.get_text(strip=True)
            if index % 2 == 0:keys.append(_text.replace("\xa0", " "))
            else:values.append(_text.replace("\xa0", " "))
        for (key, value) in (zip(keys, values)):data_dict[key] = value
        return name


    def extract_external_links(
        self, soup:BeautifulSoup, data_dict:dict) -> None:
        """Extracts social media links from the player profile"""
        try:
            external_tags = soup.find(
                "div", {"class":"infobox-center infobox-icons"})
            for link in external_tags.select("a.external"):
                href = link.attrs["href"]
                site_name = link.find("i")["class"][1].replace("lp-", "")
                data_dict[site_name] = href
        except:pass


    def extract_player_history(
        self, history_rows:BeautifulSoup, name:str) -> None:
        """Extracts player history table"""
        for row in history_rows:
            timeframe = row.find(
                "td", {"class":"th-mono"}).get_text(strip=True)
            _from, _to = timeframe.split("—")
            team = row.find("a")["title"]
            history = {
                "ID": name, "From":_from, "To": _to, "Team": team
            }
            self.history.append(history)


    def extract_settings(
        self, setting_tables:BeautifulSoup, data_dict:dict) -> None:
        """Extracts data from the wikitables with settings"""
        headings, values = [], []
        for table in setting_tables:
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
        """Extracts player  achievement slugs"""
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

    




    
    def check_image_exists(self, filepath: str) -> bool:
        """Checks if an image has already been downloaded"""
        if os.path.isfile(filepath):return True
        else:return False
    

    def work_image(self) -> None:
        """Gets a page from the queue and scans for image url"""
        if not os.path.exists(self.images_path):
            os.makedirs(self.images_path)
        while True:
            soup, name = self.images_queue.get()
            file_path = f"./images/{name}.png"
            if not self.check_image_exists(file_path):
                self.download_image(soup, file_path)
            self.images_queue.task_done()

    
    def download_image(self, soup:BeautifulSoup, file_path:str) -> None:
        """Downloads an image from a given url and saves to given file path"""
        images = soup.select("div", {"class":"infobox-image lightmode"})
        for image in images:
            try:
                a_tag = image.find("a", {"class":"image"})
                image_url = a_tag.img["src"]
                break
            except:pass
        while True:
            try:
                url = f"https://liquipedia.net{image_url}"
                # print(url)
                # urllib.request.urlretrieve(url, file_path)
                response = requests.get(url, timeout=10, stream=True)
                response.raw.decode_content = True
                with open(file_path, "wb") as file:
                    shutil.copyfileobj(response.raw, file)
                if os.stat(file_path).st_size > 500:
                    self.images.append(image_url)
                    return
            except Exception as e:pass

    def request_page(self, link:str) -> BeautifulSoup:
        """Fetches a player profile from a given url"""
        for _ in range(5):
            try:
                ip_port = random.choice(self.proxies)
                proxy = {"https":f"http://{ip_port}"}
                response = requests.get(
                    link, proxies=proxy, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(
                        response.text, "html.parser")
                else:continue
                return soup
            except Exception as e:pass

    
    def create_thread_jobs(self) -> None:
        """Create scraping jobs for threads"""
        df = pd.read_excel(
            self._input_file_path, sheet_name="List of Profiles")
        links = df["Link"].to_list()
        [self.queue.put((link, dict(), links)) for link in links]
        self.queue.join()

    
    def create_image_jobs(self, soup:BeautifulSoup, name:str) -> None:
        """Create jobs for image scraping threads"""
        self.images_queue.put((soup, name))
        self.images_queue.join()

    
    def get_proxies(self) -> None:
        """Fetches proxies from https://free-proxy-list.net/"""
        logging.info("Fetching proxies...")
        proxies = set()
        while len(self.proxies) < 10:
            try:
                response = requests.get('https://free-proxy-list.net/')
                proxies_table = BeautifulSoup(response.text, "html.parser")
                if response.status_code != 200:continue
                table_rows = proxies_table.select("tbody tr")[:299]
                if not len(table_rows):continue
                ports = ["3128", "3124", "80", "8080"]
                for row in table_rows:
                    for port in ports:   
                        proxy = ":".join(
                            [row.select('td')[0].text.strip(), port])            
                        proxies.add(proxy)
            except:continue
            self.filter_working_proxies(list(proxies))
        logging.info(f"Working proxies: {len(self.proxies)}. "
                      "Proceeding to scrape profiles...")


    
    def filter_working_proxies(self, proxies:list) -> None:
        """Creates threads to check if a proxy is working"""
        threads = []
        [threads.append(threading.Thread(
            target=self.work_ip, args =(proxies,), daemon=True
            )) for _ in range(2000)]
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]
        self.proxies = list(set(self.proxies))
        

    def work_ip(self, proxies:list) -> None:
        """Checks if a free proxy is working"""
        while len(proxies):
            try:
                ip_port = random.choice(proxies)
                url = "https://liquipedia.net/"
                proxy = {"https":f"http://{ip_port}"}
                proxies.remove(ip_port)
                response = requests.get(
                    url, proxies=proxy, verify=False, timeout=10)
                if response.status_code == 200:
                    self.proxies.append(f"{ip_port}")
                logging.info(
                    f"Proxies found: {len(self.proxies)}")
            except:pass
    

    def extract_slugs(self, link:str, data_dict:dict) -> tuple:
        """Calls the functions to extract profiles, history and achivements"""
        wikitables, name =  [], ""
        while not name:
            soup = self.request_page(link)
            name = self.extract_player_info(soup, data_dict)
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
            logging.info(
                f"Queue: {len(profiles)} || Crawled: {len(self.crawled)} | "
                f"Downloaded images: {len(self.images)}")
            self.queue.task_done()

    
    def convert_to_dataframe(self) -> pd.DataFrame:
        """Converts dictionary to dataframe"""
        profiles_df = pd.DataFrame.from_dict(self.profiles)
        column_names = profiles_df.columns.values.tolist()
        profiles_df = profiles_df[
            [column for column in COLUMN_HEADERS if column in column_names]
        ]
        history_df = pd.DataFrame.from_dict(self.history)
        achievements_df = pd.DataFrame.from_dict(self.achievements)
        return profiles_df, history_df, achievements_df

    
    def save_to_excel(self) -> None:
        """Saves data to excel file"""
        logging.info(f"Done scraping. Saving to >> {self.output_path}")
        profiles, history, achievements = self.convert_to_dataframe()
        profiles.to_excel(
            self.output_path, sheet_name="profiles", index=False)
        with pd.ExcelWriter(self.output_path, mode="a") as writer:  
            history.to_excel(writer, sheet_name="history", index=False)
            achievements.to_excel(
                writer, sheet_name="achievements", index=False)
        logging.info("Records saved!")

    
    def run(self) -> None:
        """Entry point to the scraper"""
        self.get_proxies()
        [threading.Thread(target=self.work, daemon=True).start() 
        for _ in range(self.thread_num)]
        [threading.Thread(target=self.work_image, daemon=True).start() 
        for _ in range(2)]
        self.create_thread_jobs()
        self.save_to_excel()
        


if __name__ == "__main__":
    scraper = LiquipediaScraper()
    scraper.run()