from playwright.sync_api import sync_playwright
import time
from scrapy import Selector
import requests
import json
import csv
import os

from DB_Queries import DataBase


def Search_ON_Gsmarena(smartphones):

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        page.goto("https://www.gsmarena.com/", timeout=90000)

        for phone in smartphones:
            # Click on the search box to focus it (if needed)
            page.click('//form[@id="topsearch"]/input')
            
            # Clear the input field
            page.fill('//form[@id="topsearch"]/input', '')
            

            phone = phone.lower().strip()
            # Simulate typing with delay
            for char in phone:
                page.keyboard.type(char)
                time.sleep(0.1)  # Simulate typing delay

            time.sleep(8)  # Wait for any changes or results

            elements = page.query_selector_all("//form[@id='topsearch']//div[@class='phone-results']//a")
          
                    
            for element in elements:
                link = element.get_attribute("href")
                Name = element.inner_text()
                Name = Name.lower().strip()
                    
                ######  Exact match ######
                if phone == Name:
                  
                    break
                 
                   

            print(f"Phone: {Name}")

            if link:
                link = f"https://www.gsmarena.com/{link}" 

                ####### Get info #####
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
                }
                
                Resp = requests.get(link, headers=headers)

                # Check if the request was successful
                if Resp.status_code == 200:
                    data = Resp.text  # or response.text for raw content
        
                    response = Selector(text=data)

                    HeadingDict = {}   
                    for spec in response.xpath("//div[@id='specs-list']//table"):
                        heading = spec.xpath(".//th[1]/text()").get()  
                        titleDict = {}
                        for row in spec.xpath(".//td[@class='ttl']"):
                            title = row.xpath(".//text()").extract()
                            title = " ".join(title)
                            info = row.xpath(".//following-sibling::td[@class='nfo']//text()").extract()
                            info = " ".join(info)
                            titleDict[title.strip()] = info.strip()

                            HeadingDict[heading.strip()] = titleDict

                    with open("smartphones.csv", mode='a', newline='', encoding='utf-8-sig') as file:
                        writer = csv.writer(file)
                        writer.writerow([phone,Name,json.dumps(HeadingDict)])
                else:
                    print(f"Request failed with status code {Resp.status_code}")

               
            
        time.sleep(20)
        browser.close()      
       
                 


if __name__ == '__main__':
    start_time = time.time()
    

    ##### Remove Old file ####
    try:
       os.remove("smartphones.csv")
    except:
        pass

    # ##### Get Categories and Extract Product Info #####
    DB_Object = DataBase("Get Phones Name for Searching on Gsmarena")
    DB_Object.clean_old_logs("Log -")
    smart_phones = DB_Object.get_smartphones()
    DB_Object.close_DB_connection()


    if smart_phones:
        Search_ON_Gsmarena(smart_phones)

    

    # ###### Send Data to Database from Csv ##### file #####
    Object = DataBase("Send Data to gsmarena Table")
    
    Object.insert_gsmarena()
    
    Object.update_gsmarena_id_productTable()


    Object.close_DB_connection()     
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time / 60:.2f} minutes")
