from scrapy.crawler import CrawlerProcess
import scrapy
import urllib.parse
from scrapy import Selector
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose,TakeFirst
from DB_Queries import DataBase
from datetime import datetime
import re
import time
from Dataclean import process_product_name

current_date = datetime.now().strftime("%Y-%m-%d")
FileName = f"Three - {current_date}.csv"

def remove_currency(value):
    if value:
        return value.replace('сўм', '').replace(' ', '')
    return value


def clean_price(value):
    if value:
        return value.replace('.', '')
    return value

def prepend_txt(value,txt):
    if "Price" in txt: 
        return f"{value.strip()}" if value else None
    else:
        return f"{txt}{value.strip()}" if value else None
def Get_Categories(store):
        ##### Get Categories and Extract Product Info #####
    DB_Object = DataBase(f"Get {store} Categories")
    DB_Object.clean_old_logs("Log -")
    rows = DB_Object.get_Categories(store)
    DB_Object.close_DB_connection()

    
    TempDictList = []
    if rows:

        for row in rows:
            category_url, category_name, store_name = row

            if  ";" in category_url:
                for url in category_url.split(";"):
                    TempDictList.append({"Link":url,"Category":category_name,"Store":store_name})

            else:
                  TempDictList.append({"Link":category_url,"Category":category_name,"Store":store_name})
        return TempDictList
   

class ProductItem(scrapy.Item):
    Name = scrapy.Field()
    Link = scrapy.Field()
    Image = scrapy.Field()
    Price = scrapy.Field()
    Category = scrapy.Field()
    Store = scrapy.Field()

class ProductLoader(ItemLoader):
    default_item_class = ProductItem
    default_output_processor = TakeFirst()

class ElmakonSpider(scrapy.Spider):
    name = "elmakon"
   
  
    endPoint = '/?result_ids=pagination_contents&is_ajax=1'

    
    def start_requests(self):
        categories = Get_Categories("elmakon")

        for category in categories:
            category_url = category['Link']
            category_Name = category['Category']
            store_Name = category['Store']
            Page_No = 1


            if category_Name.lower() == "Smartfonlar".lower():
           
                if category_url[-1] != '/':
                    category_url = f"{category_url}/page-"
                else:
                    category_url = f"{category_url}page-"

                yield scrapy.Request(url=self.build_url(category_url,Page_No), callback=self.parse,meta={'Category':category_Name,'Store':store_Name,'Page_No':Page_No,'Category_Url':category_url})
            

    def build_url(self,category,page):
        
        return f"{category}{page}{self.endPoint}"

    def parse(self, response):

        category_Name = response.meta['Category']
        store_Name = response.meta['Store']
        Page_No = response.meta['Page_No']
        category_url = response.meta['Category_Url']

        try:
            data = response.json()
            html_content = data.get('html', {}).get('pagination_contents', '')
            selector = Selector(text=html_content)

            for product in selector.xpath("//div[contains(@class,'ut2-gl__item')]"):
                    loader = ProductLoader(item=ProductItem(), selector=product)
                   
                    loader.add_xpath('Name', ".//a[@class='product-title']/@title", 
                        MapCompose(lambda v: process_product_name(v, category_Name)))
                    loader.add_xpath('Link', ".//a[@class='product-title']/@href")
                    loader.add_xpath('Image', ".//div[@class='ut2-gl__image']//img/@srcset")
                    loader.add_xpath('Price', ".//span[contains(@id,'sec_discounted_price')]/text()",MapCompose(lambda v: prepend_txt(v,"Price"), clean_price))
                    loader.add_value('Category',category_Name)
                    loader.add_value('Store',store_Name)
                  
                    yield loader.load_item()
           
            # Prepare for the next page
            if html_content:
                Page_No += 1
                yield scrapy.Request(url=self.build_url(category_url,Page_No), callback=self.parse,meta={'Category':category_Name,'Store':store_Name,'Page_No':Page_No,'Category_Url':category_url})

        except Exception as e:
            print(f"Failed to parse response: {e}")


class IdeaSpider(scrapy.Spider):
    name = "idea"
    base_url = 'https://api.idea.uz/api/v2/products?category_id='
    
    def start_requests(self):

        categories = Get_Categories("idea")

        for category in categories:
            category_url = category['Link']
            category_Name = category['Category']
            store_Name = category['Store']
            Page_No = 1
            
            if category_Name.lower() == "Smartfonlar".lower():
            
                category_id = category_url.replace("https://idea.uz/category/","")
                category_id = category_id.split("-")[0]                
                yield scrapy.Request(url=self.build_url(category_id,Page_No), callback=self.parse,meta={'Category':category_Name,'Store':store_Name,'Page_No':Page_No,'category_id':category_id})
            

    def build_url(self,category_id,Page_No):
       
        return f"{self.base_url}{category_id}&page={Page_No}"

    def parse(self, response):

        category_Name = response.meta['Category']
        store_Name = response.meta['Store']
        Page_No = response.meta['Page_No']
        category_id = response.meta['category_id']
        try:
            data = response.json()
            products = data['data']
            for product in products:
                loader = ProductLoader(item=ProductItem(), selector=product)
                loader.add_value('Name',product['name'],
                    MapCompose(lambda v: process_product_name(v, category_Name)))
                loader.add_value('Link',product['url'])
                loader.add_value('Image',product['gallery'][0]['original'])
                loader.add_value('Price',product['current_price'])
                loader.add_value('Category',category_Name)
                loader.add_value('Store',store_Name)
                
                yield loader.load_item()
                
                
            
            # Prepare for the next page
            Page_No += 1
            Total_Page = data['meta']['last_page']
            if Total_Page >= Page_No:
               yield scrapy.Request(url=self.build_url(category_id,Page_No), callback=self.parse,meta={'Category':category_Name,'Store':store_Name,'Page_No':Page_No,'category_id':category_id})
        except Exception as e:
            print(f"Failed to parse response: {e}")


class TexnomartSpider(scrapy.Spider):
    name = "texnomart"
    
    Texnomart_base_url = 'https://gateway.texnomart.uz/api/common/v1/search/filters?category_all='
   
   
    
    
    def start_requests(self):
        categories = Get_Categories("texnomart")

        for category in categories:
            category_url = category['Link']
            category_Name = category['Category']
            store_Name = category['Store']
            Page_No = 1
            

            ##########  for All categories #######
            # if "https://texnomart" in category_url:

            ##### For SmartPhone only #####
            if "https://texnomart" in category_url and category_Name.lower() == "Smartfonlar".lower():
                
                category_id = category_url.replace("https://texnomart.uz/ru/katalog/","").replace("/","").strip()
                yield scrapy.Request(url=self.build_url(category_id,Page_No), callback=self.parse,meta={'Category':category_Name,'Store':store_Name,'category_id':category_id})
            
    def build_url(self,category_slug,Page_No):
          
        return  f"{self.Texnomart_base_url}{category_slug}&sort=-popular&page={Page_No}"
            

    def parse(self, response):

        category_Name = response.meta['Category']
        store_Name = response.meta['Store']
        category_id = response.meta['category_id']

        try:
            data = response.json()
            products = data['data']['products']
            for product in products:
                loader = ProductLoader(item=ProductItem(), selector=product)
                loader.add_value('Name',product['name'],
                    MapCompose(lambda v: process_product_name(v, category_Name)))
                loader.add_value('Link',str(product['id']),MapCompose(lambda v:prepend_txt(v,"https://texnomart.uz/ru/product/detail/")))
                loader.add_value('Image',product['image'])
                loader.add_value('Price',product['f_sale_price'],MapCompose(lambda v: remove_currency(v)))
                loader.add_value('Category',category_Name)
                loader.add_value('Store',store_Name)

                yield  loader.load_item()
                
             
            
            # Prepare for the next page
           
            Total_Page = data['data']['pagination']['total_page']
            Current_Page = data['data']['pagination']['current_page']
   
            if Current_Page < Total_Page:
               yield scrapy.Request(url=self.build_url(category_id,Current_Page + 1), callback=self.parse,meta={'Category':category_Name,'Store':store_Name,'category_id':category_id})
   
        except Exception as e:
            print(f"Failed to parse response: {e}")

if __name__ == "__main__":
    
    start_time = time.time()
    process = CrawlerProcess(settings={
        'FEED_FORMAT': 'csv',
        'FEED_URI': f'{FileName}',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_APPEND': True,
        'ROBOTSTXT_OBEY': False,
        'REQUEST_FINGERPRINTER_IMPLEMENTATION' : '2.7',
        'TWISTED_REACTOR' : 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    })
    process.crawl(ElmakonSpider)
    process.crawl(IdeaSpider)
    process.crawl(TexnomartSpider)
    process.start()


        ##### Send Data to Database from Csv ##### file #####
    Object = DataBase("Send Data to DataBase")
    Object.clean_old_logs("Three -")

    Products = Object.read_csv(f"Three - {current_date}.csv")


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


    
    
   