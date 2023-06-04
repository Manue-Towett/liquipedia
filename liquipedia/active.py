import requests, re, os, sys, logging, threading, random, json
import pandas as pd
from bs4 import BeautifulSoup
from queue import Queue
from datetime import date

BASE_URL = "https://liquipedia.net"

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json; charset=UTF-8",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
}

class APScraper:
    if not os.path.exists("./logs/"):os.makedirs("./logs/")
    requests.packages.urllib3.disable_warnings()
    logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        handlers=[logging.FileHandler(f"./logs/liquipedia_active_players_scraper_log_{date.today()}.log"), 
        logging.StreamHandler(sys.stdout)])
    logging.info(" ======== Active Players Scraper Bot Started ========")


    def __init__(self) -> None:
        settings_file = open("./settings/settings.json", "r")
        settings = json.load(settings_file)
        settings_file.close()
        self.thread_num = settings["thread_num"]
        _output_dir = settings["output_file_path"]
        if not os.path.exists(_output_dir):os.makedirs(_output_dir)
        self.active_players = []
        self.output_path = f"{_output_dir}/active_players_{date.today()}.xlsx"
        self.queue, self.crawled = Queue(), []
        self.proxies = []

    def extract_active_players_headers(self, soup:BeautifulSoup) -> list:
        """Extracts active players heading slugs"""
        table_headers = []
        headings = soup.find("tr", {"class":"HeaderRow"})
        for heading in headings.select("th"):
            if heading.text.strip():
                table_headers.append(heading.get_text(strip=True))
        return table_headers


    def extract_active_players_rows(
        self, soup:BeautifulSoup, table_headers:list, rows:list, name:str) -> None:
        """Extracts active players slugs"""
        for row in soup.find_all("tr", {"class":"Player"}):
            url_span_tag = row.find("span", {"class":"inline-player"})
            row_data, row_dict = row.select("td"), {"Organization":name}
            [row_data.remove(cell) for cell in row.select("td") 
            if not cell.text.strip()]
            for heading, value in zip(table_headers, row_data):
                row_dict[heading] = value.get_text(strip=True)
            row_dict["player_url"] = BASE_URL + url_span_tag.a["href"]
            rows.append(row_dict)


    def find_top_twenty(self) -> list[dict]:
        """Finds the current top 20 organizations"""
        url = "https://liquipedia.net/valorant/Portal:Statistics"
        while True:
            try:
                response = requests.get(url, headers=HEADERS)
                break
            except:pass
        soup = BeautifulSoup(response.text, "html.parser")
        for table in soup.select("div.divTable"):
            if re.search("organization", str(table).lower()):
                top_20_table = table
        top_orgs = self.extract_top_organizations(top_20_table)
        return top_orgs


    def extract_top_organizations(self, table:BeautifulSoup) -> list[dict]:
        """Extract slugs from top 20 organizations"""
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
        

    def fetch_active_players(self, url:str, name:str) -> None:
        """Fetches the active players page from a given url"""
        active_players = []
        while True:
            proxy = {"https":f"http://{random.choice(self.proxies)}"}
            try:
                response = requests.get(
                    url, headers=HEADERS, verify=False, proxies=proxy, timeout=2)
                if response.status_code == 200:break
            except:pass
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.select("table")
        for table in tables:
            if re.search("active squad", str(table).lower()):
                active_table = table
                headings = self.extract_active_players_headers(active_table)
                self.extract_active_players_rows(
                    active_table, headings, active_players, name)
        return active_players
    

    def create_thread_jobs(self, links:list, names:list) -> None:
        """Create scraping jobs for threads"""
        [self.queue.put((link, name, links)) for link, name in zip(links, names)]
        self.queue.join()

    
    def get_proxies(self) -> None:
        """Fetches proxies from https://free-proxy-list.net/"""
        logging.info("Fetching proxies...")
        proxies = set()
        while True:
            try:
                response = requests.get('https://free-proxy-list.net/')
                proxies_table = BeautifulSoup(response.text, "html.parser")
                if response.status_code != 200:continue
                table_rows = proxies_table.select("tbody tr")[:299]
                if not len(table_rows):continue
                ports = ["3128", "3124", "80", "8080"]
                for row in table_rows:
                    for port in ports:   
                        proxy = ":".join([row.select('td')[0].text.strip(), port])            
                        proxies.add(proxy)
            except:continue
            self.filter_working_proxies(list(proxies))
            break
        logging.info(f"Working proxies: {len(self.proxies)}. Proceeding to scrape active players...")


    def filter_working_proxies(self, proxies:list) -> None:
        """Creates threads to check if a proxy is working"""
        threads = []
        [threads.append(threading.Thread(target=self.work_ip, args =(proxies,), daemon=True))
        for _ in range(2000)]
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
                response = requests.get(url, proxies=proxy, verify=False, timeout=10)
                if response.status_code == 200:self.proxies.append(f"{ip_port}")
                logging.info(f"Proxies found: {len(self.proxies)}")
            except:pass

    
    def work(self) -> None:
        """calls the active players fetching function with threads"""
        while True:
            link, name, links = self.queue.get()
            active_players = self.fetch_active_players(link, name)
            self.active_players.extend(active_players)
            links.remove(link)
            self.crawled.append(link)
            logging.info(f"Queue: {len(links)} || Crawled: {len(self.crawled)}")
            self.queue.task_done()

    
    def append_to_excel(self) -> None:
        """Saves data to excel"""
        logging.info("Finished scraping. Saving to excel...")
        active_players_df = pd.DataFrame.from_dict(self.active_players)
        active_players = active_players_df[["Organization", "ID", "player_url"]]
        active_players.to_excel(self.output_path, index=False)
        logging.info("Records saved!")

    
    def run(self) -> None:
        """Entry point to the scraper"""
        organization_urls, organization_names = [], []
        self.get_proxies()
        for organization in self.find_top_twenty():
            organization_names.append(organization["Organization"])
            organization_urls.append(organization["active_url"])
        [threading.Thread(target=self.work, daemon=True).start() 
        for _ in range(self.thread_num)]
        self.create_thread_jobs(organization_urls, organization_names)
        self.append_to_excel()
        

if __name__ == "__main__":
    scraper = APScraper()
    scraper.run()