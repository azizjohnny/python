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
from datetime import datetime
from DB_Queries import DataBase

current_date = datetime.now().strftime("%Y-%m-%d")
FileName = f"MediaPark - {current_date}.csv"


async def main(categories):

  
    chrome_options = webdriver.ChromeOptions()
    
   

    async with webdriver.Chrome(options=chrome_options) as driver:
        await driver.maximize_window()

      
        for category in categories:
            category_url = category['Link']
            category_Name = category['Category']
            store_Name = category['Store']
            
                 
            try:
                category_url = category_url.replace("\n","").replace("ï»¿","").strip()
            
                await driver.get(category_url, timeout=60 ,wait_load=True)
                await driver.sleep(3)
                
                Total_pages = 1
                page = 1
                while True:
                
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
                            print("Java")
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
                            
                            write_to_file_data([Name,Slug,Image,Price,category_Name,store_Name],FileName)
                        print("HTML")

                   
                        
                    if page == 1: 
                        first_url = await driver.current_url
                        count_products = selector.xpath("//p[contains(text(),'товары')]/ancestor::div[1]/p[1]/text()").get()
                        
                        if count_products:
                            Total_pages = math.ceil(int(count_products)/25)
                            print(f"Totat Pages {Total_pages}")    

                    ##### Pagination ######
                    page += 1
                    if Total_pages >= page and Total_pages > 1:
                    
                        await driver.get(f"{first_url}?page={page}", timeout=60 ,wait_load=True)
                    else:
                        break    
            except:
                pass   

def get_data(text,Category,store_Name):
  
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
            Slug = lst['slug']['ru']
            try:
                Image = lst['mobile_photos'][0]
            except:
                Image = ""
           
            Price = lst['actual_price']
            write_to_file_data([Name,f"https://mediapark.uz/products/view/{Slug}",Image,Price,Category,store_Name],FileName)          

def process_product_name(product_name, category_name):
    product_name_lower = product_name.lower()
    
    if category_name == 'Smartfonlar':
        if 'gb' in product_name_lower:
            product_name = product_name_lower.split('gb')[0].strip()
        elif 'гб' in product_name_lower:
            product_name = product_name_lower.split('гб')[0].strip()
        elif '/' in product_name_lower:
            product_name_parts = product_name_lower.split('/')
            product_name = product_name_parts[0] + '/' + product_name_parts[1].split(' ')[0]

    # Remove all words containing at least one Cyrillic character
    product_name = re.sub(r'[\w]*[а-яА-ЯЁё]+[\w]*', '', product_name).strip()

    # Trim leading and trailing spaces
    product_name = product_name.strip()

    # List of colors to remove
    colors_to_remove = ['red', 'blue', 'green', 'yellow', 'black', 'white', 'silver', 'gold', 'purple', 'pink', 'orange', 'grey','dark grey']

    # Remove words that represent colors and the words after them
    for color in colors_to_remove:
        product_name = re.sub(r'\b' + re.escape(color) + r'\b', '', product_name, flags=re.IGNORECASE).strip()

    # Capitalize the first letter of the product name
    if product_name:
        product_name = product_name[0].upper() + product_name[1:]

    return product_name



def write_to_file_data(rw,filename):

        file_exists = os.path.isfile(filename)    
        # Open the file in append mode with the appropriate settings
        with open(filename, 'a', encoding='utf-8-sig', newline="") as file:
            
            writer = csv.writer(file)  
            if not file_exists:
                writer.writerow(['Name','Link','Image','Price','Category','Store'])
            writer.writerow(rw)            

if __name__ == '__main__':
    start_time = time.time()

    
    # ##### Get Categories and Extract Product Info #####
    DB_Object = DataBase("Get MediaPark Categories")
    DB_Object.clean_old_logs("Log -")
    rows = DB_Object.get_Categories("mediapark")
    DB_Object.close_DB_connection()

    if rows:

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


    

    ###### Send Data to Database from Csv ##### file #####
    Object = DataBase("Send Data to DataBase")
    Object.clean_old_logs("MediaPark -")

    Products = Object.read_csv(f"MediaPark - {current_date}.csv")

    products_data = []
    if len(Products) > 0:
       
        print(f"Total Unique Products {len(Products)}")
        

        existing_images = Object.fetch_all_image_names()
        exists_products_ID, exists_products_Name = Object.fetch_all_product_names_Ids()

        for index, product in Products.iterrows():
            products_data.append(Object.prepare_product_data(product, existing_images,exists_products_ID, exists_products_Name))
  
    if products_data:
       Object.insert_products_batch(products_data)


    Object.close_DB_connection()     
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time / 60:.2f} minutes")
