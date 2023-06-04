import threading
from queue import Queue

import requests
from bs4 import BeautifulSoup

from .logger import Logger


class ProxyHandler:
    def __init__(self) -> None:
        self.ports = ["3128", "3124", "80", "8080"]
        self.proxies = []
        
        self.proxy_queue = Queue()
        
        self.logger = Logger("ProxyHandler")

        self.create_ip_workers()

    def get_proxies(self) -> None:
        """Fetches proxies from https://free-proxy-list.net/"""
        self.logger.info("Fetching proxies...")

        proxies = set()

        while len(self.proxies) < 10:
            try:
                response = requests.get('https://free-proxy-list.net/')
                proxies_table = BeautifulSoup(response.text, "html.parser")

                if response.status_code != 200:
                    continue

                table_rows = proxies_table.select("tbody tr")[:299]

                if not len(table_rows):
                    continue

                for row in table_rows:
                    for port in self.ports:   
                        proxy = ":".join(
                            [row.select('td')[0].text.strip(), port])            
                        proxies.add(proxy)

            except:continue

            self.logger.info("Filtering working proxies...")
            self.create_ip_jobs(list(proxies))
            
        self.logger.info(f"Working proxies: {len(self.proxies)}. "
                          "Proceeding to scrape profiles...")
    
    def create_ip_workers(self) -> None:
        """Creates threads to check if a proxy is working"""
        for _ in range(2000):
            thread = threading.Thread(target=self.work_ip, daemon=True)
            thread.start()
        

    def work_ip(self) -> None:
        """Checks if a free proxy is working"""
        while True:
            ip_port, proxies = self.proxy_queue.get()
            try:
                url = "https://liquipedia.net/"
                proxy = {"https":f"http://{ip_port}"}
                proxies.remove(ip_port)

                response = requests.get(
                    url, proxies=proxy, verify=False, timeout=10)

                if response.status_code == 200:
                    self.proxies.append(f"{ip_port}")

                self.logger.info(f"Proxies found: {len(self.proxies)}")

            except:pass

            self.proxy_queue.task_done()
    
    def create_ip_jobs(self, proxies:list) -> None:
        """Create ip thread jobs"""
        [self.proxy_queue.put((proxy, proxies)) for proxy in proxies]
        self.proxy_queue.join()

        self.proxies = list(set(self.proxies))
