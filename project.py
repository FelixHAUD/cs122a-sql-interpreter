import sys
import csv
import os
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime

# Database Configuration
# NOTE: Please update these credentials as needed for your local environment
DB_CONFIG = {
    'user': '***',         # Replace with your MySQL username
    'password': '***', # Replace with your MySQL password
    'host': 'localhost',
    'database': 'cs122a_project', # Default database name
    'raise_on_warnings': True
}

# Inferred Schema / DDL
# These are based on the problem description and function requirements.
# The order of columns in CREATE TABLE statements must match the CSV column order.

TABLES = {}

TABLES['AgentClient'] = (
    "CREATE TABLE `AgentClient` ("
    "  `uid` INT NOT NULL,"
    "  `username` VARCHAR(255) NOT NULL,"
    "  `email` VARCHAR(255) NOT NULL,"
    "  `zip` INT,"
    "  `interests` VARCHAR(255),"
    "  PRIMARY KEY (`uid`)"
    ") ENGINE=InnoDB"
)

TABLES['PaymentMethod'] = (
    "CREATE TABLE `PaymentMethod` ("
    "  `card_number` BIGINT NOT NULL,"
    "  `card_holder` VARCHAR(255) NOT NULL,"
    "  `expiration_date` DATE NOT NULL,"
    "  `cvv` INT NOT NULL,"
    "  `uid` INT NOT NULL,"
    "  PRIMARY KEY (`card_number`),"
    "  FOREIGN KEY (`uid`) REFERENCES `AgentClient` (`uid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['BaseModel'] = (
    "CREATE TABLE `BaseModel` ("
    "  `bmid` INT NOT NULL,"
    "  `description` VARCHAR(255),"
    "  PRIMARY KEY (`bmid`)"
    ") ENGINE=InnoDB"
)

TABLES['CustomizedModel'] = (
    "CREATE TABLE `CustomizedModel` ("
    "  `mid` INT NOT NULL,"
    "  `bmid` INT NOT NULL,"
    "  PRIMARY KEY (`mid`),"
    "  FOREIGN KEY (`bmid`) REFERENCES `BaseModel` (`bmid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['InternetService'] = (
    "CREATE TABLE `InternetService` ("
    "  `sid` INT NOT NULL,"
    "  `endpoint` VARCHAR(255),"
    "  `provider` VARCHAR(255),"
    "  `domain` VARCHAR(255),"
    "  PRIMARY KEY (`sid`)"
    ") ENGINE=InnoDB"
)

TABLES['Utilizes'] = (
    "CREATE TABLE `Utilizes` ("
    "  `bmid` INT NOT NULL,"
    "  `sid` INT NOT NULL,"
    "  PRIMARY KEY (`bmid`, `sid`),"
    "  FOREIGN KEY (`bmid`) REFERENCES `BaseModel` (`bmid`) ON DELETE CASCADE,"
    "  FOREIGN KEY (`sid`) REFERENCES `InternetService` (`sid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['Configuration'] = (
    "CREATE TABLE `Configuration` ("
    "  `cid` INT NOT NULL,"
    "  `label` VARCHAR(255),"
    "  `content` VARCHAR(255),"
    "  `duration` INT,"
    "  `uid` INT NOT NULL,"
    "  PRIMARY KEY (`cid`),"
    "  FOREIGN KEY (`uid`) REFERENCES `AgentClient` (`uid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

# Helper to get connection
def get_connection():
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        sys.exit(1)

# 1. Import data -> CURRENTLY WORKING: had to fix the schemas to not use old formatting and also took care of case if table doesn't exist before hand
def import_data(folder_name):
    # Connect to MySQL Server (not specific DB yet to create it)
    config_no_db = DB_CONFIG.copy()
    del config_no_db['database']
    
    try:
        cnx = mysql.connector.connect(**config_no_db)
        cursor = cnx.cursor()
        
        # Create Database if not exists
        db_name = DB_CONFIG['database']
        
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        except mysql.connector.Error:
            pass
        
        cursor.execute(f"CREATE DATABASE {db_name}")
        cnx.database = db_name
        
        # Create Tables
        # Order matters for foreign keys: AgentClient -> PaymentMethod, BaseModel -> CustomizedModel, etc.
        table_creation_order = ['AgentClient', 'PaymentMethod', 'BaseModel', 'CustomizedModel', 'InternetService', 'Utilizes', 'Configuration']
        
        for table_name in table_creation_order:
            table_description = TABLES.get(table_name)
            if table_description:
                try:
                    cursor.execute(table_description)
                except mysql.connector.Error as err:
                    print(f"Failed creating table {table_name}: {err}")
                    sys.exit(1)

        # Import CSVs
        # We assume CSV filenames match table names: e.g., AgentClient.csv
        # And columns match the DDL order.
        for table_name in table_creation_order:
            file_path = os.path.join(folder_name, f"{table_name}.csv")
            if os.path.exists(file_path):
                with open(file_path, 'r') as csvfile:
                    csv_reader = csv.reader(csvfile)
                    
                    rows = list(csv_reader)
                    if not rows:
                        continue
                        
                    num_cols = len(rows[0])
                    placeholders = ', '.join(['%s'] * num_cols)
                    query = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
                    
                    # Convert NULL to None
                    data = []
                    for row in rows:
                        cleaned_row = [None if x == 'NULL' else x for x in row]
                        data.append(cleaned_row)
                        
                    cursor.executemany(query, data)
                    cnx.commit()
            else:
                # It's possible some tables don't have data or file is missing
                pass

        cursor.close()
        cnx.close()
        print("Success") # Output Boolean as "Success" or "Fail" per instructions
    except Exception as e:
        print("Fail")
        print(e) # Debugging

# 2. Insert Agent Client -> Currently working!
def insert_agent_client(uid, username, email, card_number, card_holder, expiration_date, cvv, zip_code, interests):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        
        add_client = ("INSERT INTO AgentClient "
                      "(uid, username, email, zip, interests) "
                      "VALUES (%s, %s, %s, %s, %s)")
        client_data = (uid, username, email, zip_code, interests)
        cursor.execute(add_client, client_data)
        
        add_card = ("INSERT INTO PaymentMethod "
                    "(card_number, card_holder, expiration_date, cvv, uid) "
                    "VALUES (%s, %s, %s, %s, %s)")
        card_data = (card_number, card_holder, expiration_date, cvv, uid)
        cursor.execute(add_card, card_data)
        
        cnx.commit()
        cursor.close()
        cnx.close()
        print("Success")
    except mysql.connector.Error as err:
        print("Fail")
        print(err)

# 3. Add a customized model -> CURRENTLY WORKING
def add_customized_model(mid, bmid):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        
        query = "INSERT INTO CustomizedModel (mid, bmid) VALUES (%s, %s)"
        cursor.execute(query, (mid, bmid))
        
        cnx.commit()
        cursor.close()
        cnx.close()
        print("Success")
    except mysql.connector.Error as err:
        print("Fail")

# 4. Delete a base model
def delete_base_model(bmid):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        
        query = "DELETE FROM BaseModel WHERE bmid = %s"
        cursor.execute(query, (bmid,))
        
        if cursor.rowcount > 0:
            cnx.commit()
            print("Success")
        else:
            print("Success") # Assuming idempotent or handled
            
        cursor.close()
        cnx.close()
    except mysql.connector.Error as err:
        print("Fail")

# 5. List internet service -> THIS IS CURRENTLY WORKING
def list_internet_service(bmid):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        
        query = ("SELECT i.sid, i.endpoint, i.provider "
                 "FROM InternetService i "
                 "JOIN Utilizes u ON i.sid = u.sid "
                 "WHERE u.bmid = %s "
                 "ORDER BY i.provider ASC")
        
        cursor.execute(query, (bmid,))
        
        rows = cursor.fetchall()
        for row in rows:
            print(','.join(map(str, row)))
            
        cursor.close()
        cnx.close()
    except mysql.connector.Error as err:
        print("Fail") # Or just empty output? Instructions say "Output: Table..."

# 6. Count customized model
def count_customized_model(bmid):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        
        query = ("SELECT b.bmid, b.description, COUNT(c.mid) "
                 "FROM BaseModel b "
                 "LEFT JOIN CustomizedModel c ON b.bmid = c.bmid "
                 "WHERE b.bmid = %s "
                 "GROUP BY b.bmid, b.description "
                 "ORDER BY b.bmid ASC")
        
        cursor.execute(query, (bmid,))
        
        rows = cursor.fetchall()
        for row in rows:
            print(','.join(map(str, row)))
            
        cursor.close()
        cnx.close()
    except mysql.connector.Error as err:
        pass

# 7. Find Top-N longest duration configuration
def top_n_duration_config(uid, n):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        
        query = ("SELECT uid, cid, label, content, duration "
                 "FROM Configuration "
                 "WHERE uid = %s "
                 "ORDER BY duration DESC "
                 "LIMIT %s")
        
        cursor.execute(query, (uid, int(n)))
        
        rows = cursor.fetchall()
        for row in rows:
            print(','.join(map(str, row)))
            
        cursor.close()
        cnx.close()
    except mysql.connector.Error as err:
        pass

# 8. Keyword search
def list_base_model_keyword(keyword):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
                
        search_term = f"%{keyword}%"
        
        query = ("SELECT DISTINCT b.bmid, i.sid, i.provider, i.domain "
                 "FROM BaseModel b "
                 "JOIN Utilizes u ON b.bmid = u.bmid "
                 "JOIN InternetService i ON u.sid = i.sid "
                 "WHERE i.domain LIKE %s "
                 "ORDER BY b.bmid ASC "
                 "LIMIT 5")
        
        cursor.execute(query, (search_term,))
        
        rows = cursor.fetchall()
        for row in rows:
            print(','.join(map(str, row)))
            
        cursor.close()
        cnx.close()
    except mysql.connector.Error as err:
        pass

def show_tables():
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        cursor.execute("SHOW TABLES")

        for row in cursor.fetchall():
            print(row[0])

        cursor.close()
        cnx.close()
        print("Success")
    except:
        print("Fail")
        
def debug_show_all_tables():
    try:
        cnx = get_connection()
        cursor = cnx.cursor()

        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            print(f"\n{table}:")
            
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()

            if not rows:
                print("(empty)")
            else:
                for row in rows:
                    print(','.join(map(str, row)))

        cursor.close()
        cnx.close()

        print("\nSuccess")

    except Exception as e:
        print("Fail")
        print(e)  # uncomment if debugging

def main():
    if len(sys.argv) < 2:
        return

    command = sys.argv[1]
    
    if command == 'import':
        import_data(sys.argv[2])
    elif command == 'insertAgentClient':
        # [uid] [username] [email] [card_number] [card_holder] [expiration_date] [cvv] [zip] [interests]
        # Example order: 1 “awong” “test@uci.edu” 12345 “Alice Wong” “2020-03-09” 321 92612 “finance;data analysis”
        # sys.argv: [script, command, uid, username, email, card_number, card_holder, exp_date, cvv, zip, interests]
        # Indices:    0       1       2      3        4         5            6          7        8    9      10
        insert_agent_client(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9], sys.argv[10])
    elif command == 'addCustomizedModel':
        add_customized_model(sys.argv[2], sys.argv[3])
    elif command == 'deleteBaseModel':
        delete_base_model(sys.argv[2])
    elif command == 'listInternetService':
        list_internet_service(sys.argv[2])
    elif command == 'countCustomizedModel':
        count_customized_model(sys.argv[2])
    elif command == 'topNDurationConfig':
        top_n_duration_config(sys.argv[2], sys.argv[3])
    elif command == 'listBaseModelKeyWord':
        list_base_model_keyword(sys.argv[2])
    elif command == 'showTables':
        show_tables()
        debug_show_all_tables()

if __name__ == '__main__':
    main()

