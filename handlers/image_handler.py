import os, shutil, requests
from bs4 import BeautifulSoup

class ImageHandler:
    
    def download_image(self, soup:BeautifulSoup, name:str) -> None:
        """Downloads images from a given url"""
        if not os.path.exists(self.images_path):os.makedirs(self.images_path)
        images = soup.select("div", {"class":"infobox-image lightmode"})
        image_found, downloaded_images = False, os.listdir(self.images_path)
        for image in images:
            try:
                a_tag = image.find("a", {"class":"image"})
                image_url = a_tag.img["src"]
                image_found = True
                break
            except:pass
        if image_found:
            file_name = name + ".png"
            if not file_name in downloaded_images:
                # soup = self.request_page(f"https://liquipedia.net{image_url}")
                # if soup:
                #     image_tag = soup.find("div", {"class":"fullMedia"})
                #     image_url = image_tag.a["href"]
                # urllib.request.urlretrieve(f"https://liquipedia.net{image_url}", self.images_path+file_name)
                response = self.request_image_page(image_url)
                response.raw.decode_content = True
                with open(self.images_path+file_name, "wb+") as file:
                    shutil.copyfileobj(response.raw, file)

    
    def request_image_page(self, image_url:str) -> requests:
        """Requests a given page containing an image"""
        for _ in range(5):
            proxy = {"https":f"http://{random.choice(self.proxies)}"}
            try:
                url = f"https://liquipedia.net{image_url}"
                response = requests.get(
                    url, proxies=proxy, verify=False, stream=True)
                if response.status_code == 200:return response
            except:pass