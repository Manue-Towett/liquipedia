import os, shutil, requests

url = "https://liquipedia.net/commons/images/thumb/d/d6/FPX_ardiis_at_the_VCT_Stage_2_Masters_Copenhagen.jpg/600px-FPX_ardiis_at_the_VCT_Stage_2_Masters_Copenhagen.jpg"
response = requests.get(url, stream=True)
print(response.status_code)
response.raw.decode_content = True

with open("new.png", "wb") as file:
    shutil.copyfileobj(response.raw, file)

# print(os.stat("./images/Marved.png").st_size < 500)



    
    # def request_image_page(self, image_url:str) -> requests:
    #     """Requests a given page containing an image"""
    #     for _ in range(5):
    #         proxy = {"https":f"http://{random.choice(self.proxies)}"}
    #         try:
    #             url = f"https://liquipedia.net{image_url}"
    #             response = requests.get(
    #                 url, proxies=proxy, verify=False, stream=True
    #             )
    #             if response.status_code == 200:return response
    #         except:pass