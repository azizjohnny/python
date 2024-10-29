import mysql.connector
import requests
import logging
import pandas as pd
import re
import os
import csv
from datetime import datetime, timedelta
current_date = datetime.now().strftime("%Y-%m-%d")

current_date = datetime.now().strftime("%Y-%m-%d")

class DataBase:
    def __init__(self,Goal):
        self.setup_logging()
   
        try:
            # Connect to MySQL
            self.connection = mysql.connector.connect(
                host = '194.31.52.65',
                database = 'xnarx',
                user = 'root',
                password = 'Cool2002!'
            )
            self.cursor = self.connection.cursor()
            logging.info(f"Connected to the database successfully: {Goal}")
    
        except mysql.connector.Error as error:
            logging.error(f"Error while connecting to MySQL:\n  {error}")

    def setup_logging(self):
        current_date = datetime.now().strftime("%Y-%m-%d")
        log_filename = f'Log -{current_date}.log'
        logging.basicConfig(filename=log_filename, level=logging.DEBUG,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # Add custom filtering for mysql.connector, asyncio, websockets.client
        self.filter_modules_logging(['mysql.connector', 'asyncio', 'websockets.client'])
    

    def filter_modules_logging(self, module_names):
        for module_name in module_names:
            module_logger = logging.getLogger(module_name)
            module_logger.setLevel(logging.WARNING)
            module_logger.addHandler(logging.NullHandler()) 
  
  
    def read_csv(self,file_path):
        if os.path.exists(file_path):
    
            df = pd.read_csv(file_path)
            
            df.drop_duplicates(subset=['Name', 'Store'], keep='first', inplace=True)
            
            return df
        else:
            return ""

    def clean_old_logs(self,Start_with ):
       
        log_dir = os.getcwd()
        current_date = datetime.now().date() 
        
    
        for filename in os.listdir(log_dir):
            try:
                if filename.startswith(f"{Start_with}") and filename.endswith(".log") or filename.endswith(".csv"):
                    
                 
                    
                    date_match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
                    if date_match:
                        file_date_str = date_match.group(0)
                       
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                 
                    difference = current_date - file_date
                    

                    if difference.days > 3:
                        os.remove(filename)
                        logging.info(f"Deleted old log file: {filename}")
            except:
                pass                


    def fetch_all_image_names(self):
        sql = "SELECT name FROM image"
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return {row[0] for row in result}
    
    def fetch_all_product_names_Ids(self):
        sql = "SELECT id,product_name FROM product"
        self.cursor.execute(sql)
        products = self.cursor.fetchall()
        id_to_position = {index: product[0]  for index, product in enumerate(products)}
        position_to_name = {product[1].lower().strip():index  for index, product in enumerate(products)}
        return id_to_position, position_to_name


        # return {row[0] for row in result}

    def save_image_to_db(self, image_url, product_image, image_data):
        sql = "INSERT INTO image (url, image_name, image_blob) VALUES (%s, %s, %s)"
        self.cursor.execute(sql, (image_url, product_image, image_data))
        self.connection.commit()
        print(f"Image {product_image} inserted successfully!")

    def prepare_product_data(self, product, existing_images,exists_products_ID,exists_products_Name):
        product_name = str(product['Name'])
        product_link = str(product['Link'])
        image_url = str(product['Image'])
        price = str(product['Price'])
        category_name = str(product['Category'])
        store_name = str(product['Store'])
        product_image = product_name.replace("/", "").replace(" ", "_").strip()
        if product_image:
           product_image = f"{product_image}.jpg" 
        else:
            product_image = None   

        if price:
            try:
                price_string_cleaned = re.sub(r'\D', '', price)
                price = float(price_string_cleaned)
            except:
                price = 0
        else:
            price = 0


        ##### This is for Avoid duplicates Entries #######
        check_product_name = product_name
        position = exists_products_Name.get(product_name.lower().strip(), -1)
        
        # print(exists_products_ID)
        
        if position != -1:  
           check_product_name = None 

        current_datetime = datetime.now()
        # formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        formatted_datetime = current_datetime.strftime('%Y-%m-%d 00:00:00.000000')

        image_data = None
        if product_image not in existing_images and ("http:/" in image_url or "https:/" in image_url):
            if " " in image_url:
                image_url = image_url.split(" ")[0]
            try:
                response = requests.get(image_url)
                if response.status_code == 200:
                    image_data = response.content

                
                image_sql = """
                    INSERT IGNORE INTO image (data,name)
                    VALUES (%s, %s)
                """
                print(product_image)

                image_info = (image_data,product_image)
                self.cursor.execute(image_sql, image_info)
                self.connection.commit()
                print("Images Save!")    
            
            
            except Exception as e:
                print(f"Error downloading image {image_url}: {e}")

        return (category_name, product_image, product_name, formatted_datetime, product_link, price, store_name, image_url,check_product_name)

    def insert_products_batch(self, products_data):
        product_sql = """
            INSERT INTO product (category_name, product_image, product_name)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE category_name=VALUES(category_name), product_image=VALUES(product_image), product_name=VALUES(product_name);
        """
   

        price_sql = """
            INSERT IGNORE INTO pricehistory (date, product_link, price, store_name, product_id)
            VALUES (%s, %s, %s, %s, %s)
        """
       
        ##### Product Info ######
        
        product_data = [(pd[0], pd[1], pd[2]) for pd in products_data if pd[8] and pd[2]]

        self.cursor.executemany(product_sql, product_data)
        self.connection.commit()
            # print("Products Save!")
        print("Products Save!")
      
        
        price_data = []
        exists_products_ID, exists_products_Name = self.fetch_all_product_names_Ids()
        for pd in products_data:
        
            position = exists_products_Name.get(pd[2].lower().strip(), -1)
            if position != -1:
                prd_id = exists_products_ID[position] 
                price_data.append((pd[3],pd[4],pd[5],pd[6],prd_id))
            
    
        self.cursor.executemany(price_sql, price_data)
        self.connection.commit()
        
        print("Price Save!") 
        
    def get_Categories(self,store_name):
        
        try:

            # SQL query to retrieve data from the category table
            query = """
                    SELECT category_link, category_name, store_name
                    FROM category
                    WHERE LOWER(store_name) = LOWER(%s)
                    """
            

            # Execute the query
            self.cursor.execute(query, (store_name,))

            # Fetch all rows
            rows = self.cursor.fetchall()
            return rows

          
        except mysql.connector.Error as error:
            print(f"Error querying data: {error}")
            return False
  
     
    def get_smartphones(self):

        try:

            # SQL query to retrieve data from the category table
            query = """
                    SELECT product_name
                    FROM product
                    WHERE gsmarena_id IS NULL
                    """
            # Execute the query
            self.cursor.execute(query)    
             # Fetch all rows
            rows = self.cursor.fetchall()
            clean_smartphones = [self.clean_smartphone_names(row[0]) for row in rows]
           
            clean_smartphones = list(set(clean_smartphones))
            return clean_smartphones
            # final_smartphones = [["Product Name"]] + [[phone] for phone in clean_smartphones]

            # with open("smartphones.csv", mode='w', newline='') as file:
            #     writer = csv.writer(file)
                
            #     # Writing all rows at once
            #     writer.writerows(final_smartphones)

        except mysql.connector.Error as error:
            print(f"Error querying data: {error}")

    def insert_gsmarena(self):
        try:
            with open('smartphones.csv', 'r') as file:
                reader = csv.DictReader(file, fieldnames=['search', 'name','info'])
                data_to_insert = [(row['search'], row['name'],row['info'])  for row in reader if row['name'] != 'more device results']

            self.cursor.executemany("INSERT IGNORE  INTO gsmarena (search,name,details) VALUES (%s, %s,%s)", data_to_insert)

            # Commit the transaction and close the connection
            self.connection.commit()
        except:
            pass    
               
    def update_gsmarena_id_productTable(self):

       
        self.cursor.execute("""
            SELECT p.product_name, g.id,LENGTH(g.search) AS search_length
            FROM product p
            LEFT JOIN gsmarena g
            ON p.product_name LIKE CONCAT('%',g.search, '%')
            WHERE g.search IS NOT NULL
            ORDER BY search_length ASC
        """)
        matches = self.cursor.fetchall()

        # Step 1: Prepare the data for bulk update
        update_data = [[row[1],row[0]] for row in matches]


        ###### Step 2: Update the product table in bulk using executemany
        self.cursor.executemany("""
            UPDATE product
            SET gsmarena_id = %s
            WHERE product_name = %s
        """, update_data)

        self.connection.commit()           

    def clean_smartphone_names(self,input_string):

      
        ####### Step 1: Remove division-like numbers (e.g., "5/6")
        cleaned_string = re.sub(r'\b\d+/\d+\b', '', input_string)
        
        ###### Step 2: Use regex to remove a number at the end of the string preceded by a space 
        cleaned_string = re.sub(r'\s\d+$', '', cleaned_string).strip()

        ###### Step 3: Remove ()
        cleaned_string = re.sub(r"\(.*", "", cleaned_string).strip()
        
        ###### Step 4: Remove everything after +
        cleaned_string = re.sub(r"\+.*", "+", cleaned_string).strip()
        
        ###### Step 4: Remove everything after " eu"
        cleaned_string = re.sub(r" eu.*", "+", cleaned_string).strip()

    
        ##### Step 6 : Remove 4G or 5G at End
        # Replace the pattern with an empty string
        cleaned_string = re.sub(r"(4G|5G)\s*$", "", cleaned_string, flags=re.IGNORECASE).strip()
  
        return cleaned_string
    
    def close_DB_connection(self):
        self.connection.close()

# # # Main script 
###### Testing #######
# Object = DataBase("Test")
# Products = Object.read_csv(f"Three - {current_date}.csv")

# if len(Products) > 0:
#     # try:
#         print(f"Total Unique Products {len(Products)}")
#         products_data = []

#         existing_images = Object.fetch_all_image_names()
#         exists_products_ID, exists_products_Name = Object.fetch_all_product_names_Ids()

#         for index, product in Products.iterrows():
#             products_data.append(Object.prepare_product_data(product, existing_images,exists_products_ID,exists_products_Name))
          
#         Object.insert_products_batch(products_data)
#     # except:
#     #     pass     

# Object.close_DB_connection()
