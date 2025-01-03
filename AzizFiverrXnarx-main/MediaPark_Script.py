from selenium_driverless import webdriver
from selenium_driverless.types.by import By
import asyncio
from scrapy import Selector
import time
import json
import math
import re
import csv
import os
import logging
from datetime import datetime
from DB_Queries import DataBase
from Dataclean import process_product_name

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scrape.log'),
        logging.StreamHandler()
    ]
)

current_date = datetime.now().strftime("%Y-%m-%d")
FileName = f"MediaPark - {current_date}today.csv"

logging.info("NEW LOG! \n\n")


async def main(categories):
    logging.info("Starting scraping process")
    chrome_options = webdriver.ChromeOptions()
    
    async with webdriver.Chrome(options=chrome_options) as driver:
        await driver.maximize_window()

        for category in categories:
            category_url = category['Link']
            category_Name = category['Category']
            store_Name = category['Store']
            logging.info(f"Processing category: {category_Name}")
            
            try:
                category_url = category_url.replace("\n","").replace("ï»¿","").strip()
                logging.info(f"category url: {category_url}")
                await driver.get(category_url, timeout=60, wait_load=True)
                await driver.sleep(3)
                
                logging.info(f"Starting pagination loop for {category_Name}")
                Total_pages = 1
                page = 1
                while True:
                    logging.info(f"Processing page {page} of {Total_pages}")
                    
                    Chect_Trig = None  #### This is for check if Java Object found or Not
                    ###### Java Script Object #######
                    try:
                        
                        json_object = await driver.find_element(By.XPATH,"//script[contains(text(),'products') and contains(text(),'bread_crumbs')]",timeout=5)
                        if json_object:
                            json_Object_Text = await json_object.get_attribute("innerHTML")
            
                            json_Object_Text = json_Object_Text.replace("self.__next_f.push(","")
                            
                            json_Object_Text = json_Object_Text[:-1]

                            Json_list = json.loads(json_Object_Text)
                            get_data(Json_list[1],category_Name,store_Name)
                            
                            Chect_Trig = True
                            logging.info("JS")
                        else:
                            Chect_Trig = False    
                            
                    except:
                        Chect_Trig = False
                   
                    
                    html_content = await driver.page_source
                    selector = Selector(text=html_content)



                        ######  HTML structure #######
                        
                    if not Chect_Trig:
                        ##### Scroll Page Load Images #####
                       
                        count_products = selector.xpath("//a[contains(@class,'product-cart')]")
                        if count_products:
                           count_products = len(count_products)
                           scroll_No = math.ceil(int(count_products)/5)
                        else:
                            scroll_No =  3
                        for i in range(scroll_No):  # Adjust the range for the number of scrolls you need
                            await driver.execute_script("window.scrollBy(0, 600);")  # Scroll down by 500 pixels
                            time.sleep(1)  # Wait for a second (or adjust the wait time as needed)
                        time.sleep(2)
                        ##### Getting Data ######
                        for product in selector.xpath("//a[contains(@class,'product-cart')]"):
    
                            Name = process_product_name(product.xpath(".//p/text()").get(),category_Name)
                            Slug = product.xpath(".//@href").get() 
                            Image = product.xpath(".//img/@src").get()
                            Price = product.xpath(".//b/text()").get()
                            if not "https://mediapark.uz" in Slug:
                                Slug = f"https://mediapark.uz{Slug}"
                            logging.info(f"processing HTML product name: {Name}")
                            
                            write_to_file_data([Name,Slug,Image,Price,category_Name,store_Name],FileName)
                        logging.info("HTML")

                   
                        
                    if page == 1: 
                        first_url = await driver.current_url
                        # Try to get total pages from pagination element first
                        # total_pages_element = selector.xpath("//div[contains(@class, 'pagination')]//a[last()-1]/text()").get()
                        # if total_pages_element and total_pages_element.isdigit():
                        #     Total_pages = int(total_pages_element)
                        #     print(f"Total Pages from pagination: {Total_pages}")
                        # else:
                            # Fallback to calculating from product count
                        count_products = selector.xpath("//p[contains(text(),'товары')]/ancestor::div[1]/p[1]/text()").get()
                        # count_products = selector.xpath("//p[contains(text(),'Mahsulotlar')]/ancestor::div[1]/p[1]/text()").get()
                        if count_products:
                            logging.info(f"Total Pages calculated: {count_products}")    
                            Total_pages = 30

                    ##### Pagination ######
                    page += 1
                    if Total_pages >= page and Total_pages > 1:
                        await driver.get(f"{first_url}?page={page}", timeout=60 ,wait_load=True)
                    else:
                        break
            except Exception as e:
                logging.error(f"Error processing category {category_Name}: {str(e)}")
                pass   

def get_data(text, Category, store_Name):
    logging.info(f"Processing JSON data for category: {Category}")
    pattern = r'"products":(.*?),\s*"bread_crumbs"'


    # Use re.search to find the match
    match = re.search(pattern, text, re.DOTALL)

    # Check if a match is found
    if match:
        # Extract the content between "products": and ,"bread_crumbs"
        products_content = match.group(1)
        output = json.loads(products_content)
        # print(output[0])
        for lst in output:

            Name = lst['name']['ru']
            Name = process_product_name(Name,Category)
            logging.info(f"processing JS object name: {Name}")
            Slug = lst['slug']['ru']
            Slug_link = f"https://mediapark.uz/products/view/{Slug}"
            try:
                Image = lst['mobile_photos'][0]
                if not Image:
                    logging.info(f"default to second image")
                    Image = lst['mobile_photos'][1]
                if not Image:
                    logging.info(f"default to third image")
                    Image = lst['mobile_photos'][2]
            except:
                Image = ""
            if not Image:
                print(f"XX: {lst['mobile_photos']}")
           
            Price = lst['actual_price']
            write_to_file_data([Name,Slug_link,Image,Price,Category,store_Name],FileName)          




def write_to_file_data(rw, filename):
    try:
        file_exists = os.path.isfile(filename)
        if not file_exists:
            logging.info(f"Creating new CSV file: {filename}")
        with open(filename, 'a', encoding='utf-8-sig', newline="") as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(['Name','Link','Image','Price','Category','Store'])
            writer.writerow(rw)
    except Exception as e:
        logging.error(f"Error writing to file: {str(e)}")

if __name__ == '__main__':
    start_time = time.time()
    logging.info("Script started")

    DB_Object = DataBase("Get MediaPark Categories")
    logging.info("Cleaning old logs")
    DB_Object.clean_old_logs("scrape-")
    
    logging.info("Fetching categories from database")
    rows = DB_Object.get_Categories("mediapark")
    DB_Object.close_DB_connection()

    if rows:
        logging.info(f"Found {len(rows)} categories")

        TempDictList = []
        for row in rows:
            category_url, category_name, store_name = row

            if category_name.lower() == "Smartfonlar".lower():
                if  ";" in category_url:
                    for url in category_url.split(";"):
                        TempDictList.append({"Link":url,"Category":category_name,"Store":store_name})

                else:
                    TempDictList.append({"Link":category_url,"Category":category_name,"Store":store_name})
            
        asyncio.run(main(TempDictList))

    logging.info("Starting database upload process")
    Object = DataBase("Send Data to DataBase")
    Object.clean_old_logs("MediaPark -")

    Products = Object.read_csv(f"MediaPark - {current_date}.csv")

    products_data = []
    if len(Products) > 0:
        logging.info(f"Processing {len(Products)} products for database upload")
        existing_images = Object.fetch_all_image_names()
        exists_products_ID, exists_products_Name = Object.fetch_all_product_names_Ids()

        for index, product in Products.iterrows():
            products_data.append(Object.prepare_product_data(product, existing_images,exists_products_ID, exists_products_Name))
  
    if products_data:
        logging.info(f"Inserting {len(products_data)} products into database")
        Object.insert_products_batch(products_data)

    Object.close_DB_connection()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Script completed. Elapsed time: {elapsed_time / 60:.2f} minutes")
