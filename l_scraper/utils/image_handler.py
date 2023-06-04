import os
import random
import shutil
from queue import Queue

import requests
from bs4 import BeautifulSoup

from .logger import Logger


class ImageHandler:
    def __init__(self, images:list, queue:Queue, dir:str, proxies:list) -> None:
        """
        Scrapes images from liquipedia and stores them locally

        :param images: a list to store image urls
        :param queue: a queue where image thread jobs are stored for processing
        :param dir: the directory where images will be stored
        :param proxies: list of proxies
        """

        self.images = images
        self.images_path = dir
        self.images_queue = queue
        self.proxies = proxies

        if not os.path.exists(self.images_path):
            os.makedirs(self.images_path)

        self.logger = Logger("ImageHandler")

    def check_image_exists(self, filepath: str) -> bool:
        """Checks if an image has already been downloaded"""
        if os.path.isfile(filepath) and os.stat(filepath).st_size > 500:
            return True
        else:
            return False
    
    def extract_image_url(self, soup:BeautifulSoup, file_path:str) -> None:
        """
        Extracts an image url from html response from the server

        :param soup: a beautifulsoup object of html response from the server
        :param file_path: relative path to the image in the local directory
        """
        images = soup.select("div", {"class":"infobox-image lightmode"})

        for image in images:
            try:
                a_tag = image.find("a", {"class":"image"})
                image_url = a_tag.img["src"]
                
                self.download_image(image_url, file_path)

                break

            except:pass
    
    def download_image(self, image_url:str, dir:str) -> None:
        """
        Downloads an image from a given url and saves to given file path

        :param image_url: relative path to the image on the server
        :param dir: the relative path to an image on local directory
        """
        
        while True:
            response = self.fetch_image(image_url)
            try:
                with open(dir, "wb") as file:
                    shutil.copyfileobj(response.raw, file)

                if os.stat(dir).st_size > 500:
                    self.images.append(image_url)

                    break

            except:
                self.logger.warn("Could not download image. Retrying...")
    
    def fetch_image(self, image_url:str) -> None:
        """
        Fetches an image page from the server and returns the response if status
        equal to 200

        :param image_url: relative path to the image in the server
        """
        while True:
            try:
                proxy = {"https":f"http://{random.choice(self.proxies)}"}
                url = f"https://liquipedia.net{image_url}"

                response = requests.get(url, proxies=proxy, timeout=30, stream=True)
                response.raw.decode_content = True

                if response.status_code == 200:
                    return response

            except:pass
    
    def work(self) -> None:
        """Gets a page from the queue and scans for image url"""
        while True:
            soup, name = self.images_queue.get()
            file_path = f"./images/{name}.png"

            if not self.check_image_exists(file_path):
                self.extract_image_url(soup, file_path)

            self.images_queue.task_done()