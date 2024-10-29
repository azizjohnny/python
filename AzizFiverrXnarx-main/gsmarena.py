from playwright.sync_api import sync_playwright
import time
from scrapy import Selector
import requests
import json
import csv
import os
import pandas as pd
from DB_Queries import DataBase

# Import the Translator
from deep_translator import GoogleTranslator

def Search_ON_Gsmarena(smartphones):

    # Initialize the translator
    dest_language = 'uz'  # Uzbek language code
    translator = GoogleTranslator(source='auto', target=dest_language)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        page.goto("https://www.gsmarena.com/", timeout=90000)

        data_list = []  # Store data temporarily

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
                    
            link = None
            Name = None
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

                    # Remove "Price" from the data
                    if 'Misc' in HeadingDict:
                        if 'Price' in HeadingDict['Misc']:
                            del HeadingDict['Misc']['Price']

                    # Save the original data
                    data_list.append([phone, Name, json.dumps(HeadingDict, ensure_ascii=False)])

        time.sleep(20)
        browser.close()

        # Save the data to an Excel file
        df = pd.DataFrame(data_list, columns=['Phone', 'Name', 'Specifications'])
        df.to_excel('smartphones_original.xlsx', index=False)
        print("Original data saved to smartphones_original.xlsx")
         
        return data_list

def translate_and_save(data_list, translator):
    translated_data_list = []
    
    def translate_dict(data_dict, translator):
        translated_dict = {}
        for key, value in data_dict.items():
            if isinstance(value, dict):
                value_translated = translate_dict(value, translator)
            else:
                try:
                    value_translated = translator.translate(value)
                except Exception as e:
                    print(f"Error translating value '{value}': {e}")
                    value_translated = value  # Use the original text if translation fails
            try:
                key_translated = translator.translate(key)
            except Exception as e:
                print(f"Error translating key '{key}': {e}")
                key_translated = key  # Use the original key if translation fails
            translated_dict[key_translated] = value_translated
        return translated_dict

    for row in data_list:
        phone, name, specs = row
        HeadingDict = json.loads(specs)

        # Translate the HeadingDict
        translated_heading_dict = translate_dict(HeadingDict, translator)

        translated_data_list.append([phone, name, json.dumps(translated_heading_dict, ensure_ascii=False)])
    
    # Save translated data to another Excel file
    df_translated = pd.DataFrame(translated_data_list, columns=['Phone', 'Name', 'Specifications'])
    df_translated.to_csv('smartphones.csv', index=False)
    print("Translated data saved to smartphones.csv")

    return translated_data_list

if __name__ == '__main__':
    start_time = time.time()

    ##### Remove Old files #####
    try:
       os.remove("smartphones_original.xlsx")
       os.remove("smartphones.csv")
    except:
        pass

    # ##### Get Categories and Extract Product Info #####
    DB_Object = DataBase("Get Phones Name for Searching on Gsmarena")
    DB_Object.clean_old_logs("Log -")
    smart_phones = DB_Object.get_smartphones()
    DB_Object.close_DB_connection()

    if smart_phones:
        original_data = Search_ON_Gsmarena(smart_phones)

        # Initialize the translator
        translator = GoogleTranslator(source='auto', target='uz')

        # Translate and save the data
        translated_data = translate_and_save(original_data, translator)

        # ###### Send Translated Data to Database from Excel file #####
        Object = DataBase("Send Data to gsmarena Table")
        Object.insert_gsmarena()
        Object.update_gsmarena_id_productTable()
        Object.close_DB_connection()

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time / 60:.2f} minutes")


