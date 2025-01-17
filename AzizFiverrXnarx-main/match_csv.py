import sqlite3
import mysql.connector
from DB_Queries import DataBase
from datetime import datetime


"""
1. Iphone RAM mentioned and not mentioned - 1500, 14949 / 14999, 14950 - MediaPark
2. Realme 12+ 8/256 Gb vs Realme 12 +
3. Rmx3363 Realme Gt Master Edition 8+256 - - 5996993 Gb, 207
"""

# Function to create CSV file with Original and New ID columns
def create_sqlite_db():
    import csv
    import os
    
    # Check if file exists
    file_exists = os.path.exists('changes.csv')
    
    # Open in append mode if exists, write mode if new
    mode = 'a' if file_exists else 'w'
    csvfile = open('changes.csv', mode, newline='')
    writer = csv.writer(csvfile)
    
    # Only write header if new file
    if not file_exists:
        writer.writerow(['Original', 'New'])
        
    return csvfile, writer

# Function to check if Original ID exists and add New ID if not already present
def check_and_add_original(writer, original_id, new_id):
    import csv
    
    # Read existing entries to check for Original ID
    with open('changes.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if int(row['Original']) == original_id:
                print(f"Original ID: {original_id} already exists.")
                return
    
    # If Original ID not found, append new row
    writer.writerow([original_id, new_id])
    print(f"Added Original ID: {original_id} with New ID: {new_id}")

# Function to update MySQL database for all entries in changes.db
def update_mysql_product_ids(db_instance, sqlite_cursor):
    try:
        # Fetch all Original and New IDs from the changes table
        sqlite_cursor.execute("SELECT Original, New FROM changes")
        id_pairs = sqlite_cursor.fetchall()

        if not id_pairs:
            print("No ID pairs found in changes.db to update.")
            return

        for original_id, new_id in id_pairs:
            # Update statement
            update_sql = "UPDATE pricehistory SET product_id = %s WHERE product_id = %s"
            try:
                db_instance.cursor.execute(update_sql, (new_id, original_id))
                db_instance.connection.commit()
                print(f"Updated product_id from {original_id} to {new_id} in pricehistory table.")
            except mysql.connector.Error as error:
                if error.errno == 1062:
                    print(f"Duplicate entry error when updating product_id from {original_id} to {new_id}: {error}")
                    try:
                        # Extract conflict entry from error message
                        conflict_entry = error.msg.split("Duplicate entry '")[1].split("' for key")[0]
                        
                        # Parsing logic
                        # Step 1: Split on the first hyphen
                        product_id, rest = conflict_entry.split('-', 1)
                        
                        # Step 2: Find the last hyphen in rest
                        last_hyphen_index = rest.rfind('-')
                        if last_hyphen_index == -1:
                            print(f"Could not find the last hyphen in conflict entry: {conflict_entry}")
                            continue
                        
                        # Step 3: Extract date and store_name
                        date = rest[:last_hyphen_index]
                        store_name = rest[last_hyphen_index + 1:]
                        
                        # Delete the conflicting record
                        delete_sql = "DELETE FROM pricehistory WHERE product_id = %s AND date = %s AND store_name = %s"
                        db_instance.cursor.execute(delete_sql, (product_id, date, store_name))
                        db_instance.connection.commit()
                        print(f"Deleted conflicting record with product_id {product_id}, date {date}, store_name {store_name}")
                        
                        # Retry the update
                        db_instance.cursor.execute(update_sql, (new_id, original_id))
                        db_instance.connection.commit()
                        print(f"Retried and updated product_id from {original_id} to {new_id} in pricehistory table.")
                    except Exception as parse_error:
                        print(f"Error parsing conflict details: {parse_error}")
                        print("Skipping this update due to parsing error.")
                else:
                    print(f"Error updating product_id from {original_id} to {new_id}: {error}")
                    print(f"Please ensure that 'product_id' column in 'pricehistory' table can store the value {new_id}.")
                    continue  # Continue with the next ID pair
    except Exception as error:
        print(f"Error updating MySQL: {error}")

# Function to display all ID pairs in changes.db
def display_changes(sqlite_cursor):
    sqlite_cursor.execute("SELECT Original, New FROM changes")
    id_pairs = sqlite_cursor.fetchall()
    if id_pairs:
        print("ID pairs in changes.db:")
        for original_id, new_id in id_pairs:
            print(f"Original ID: {original_id}, New ID: {new_id}")
    else:
        print("No ID pairs found in changes.db.")

# Main script
def match():
    # Connect to SQLite database
    sqlite_conn, sqlite_cursor = create_sqlite_db()

    # Optional: Get user input to add a new ID pair
    add_new_pair = input("Do you want to add a new Original and New ID pair? (yes/no): ").strip().lower()
    if add_new_pair == 'yes':
        original_id = input("Enter Original ID: ").strip()
        new_id = input("Enter New ID: ").strip()
        # Convert IDs to integers
        try:
            original_id = int(original_id)
            new_id = int(new_id)
            if original_id == new_id:
                print("Original ID and New ID are the same. Please enter different IDs.")
                return
        except ValueError:
            print("Invalid ID entered. IDs must be integers.")
            return
        # Check and add to SQLite
        check_and_add_original(sqlite_cursor, original_id, new_id)
        sqlite_conn.commit()

    # Display all ID pairs in changes.db
    display_changes(sqlite_cursor)

    # Connect to MySQL database using DataBase class
    db_instance = DataBase("MySQL Connection for Updates")

    # Update MySQL database for all entries in changes.db
    update_mysql_product_ids(db_instance, sqlite_cursor)

    # Close connections
    sqlite_conn.close()
    db_instance.close_DB_connection()
