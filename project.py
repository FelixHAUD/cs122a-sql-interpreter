import sys
import csv
import os
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime

# Database Configuration
# NOTE: Please update these credentials as needed for your local environment
DB_CONFIG = {
    'user': 'root',         # Replace with your MySQL username
    'password': 'sqlPwd26!', # Replace with your MySQL password
    'host': 'localhost',
    'database': 'cs122a_project', # Default database name
    'raise_on_warnings': True
}

# Schema provided in HW2 Solutions

TABLES = {}

TABLES['Users'] = (
    "CREATE TABLE `Users` ("
    "  `uid` INT NOT NULL,"
    "  `email` TEXT NOT NULL,"
    "  `username` TEXT NOT NULL,"
    "  PRIMARY KEY (`uid`)"
    ") ENGINE=InnoDB"
)

TABLES['AgentCreator'] = (
    "CREATE TABLE `AgentCreator` ("
    "  `uid` INT NOT NULL,"
    "  `bio` TEXT,"
    "  `payout` TEXT,"
    "  PRIMARY KEY (`uid`),"
    "  FOREIGN KEY (`uid`) REFERENCES `Users`(`uid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['AgentClient'] = (
    "CREATE TABLE `AgentClient` ("
    "  `uid` INT NOT NULL,"
    "  `interests` TEXT NOT NULL,"
    "  `cardholder` TEXT NOT NULL,"
    "  `expire` DATE NOT NULL,"
    "  `cardno` INT NOT NULL,"
    "  `cvv` INT NOT NULL,"
    "  `zip` INT NOT NULL,"
    "  PRIMARY KEY (`uid`),"
    "  FOREIGN KEY (`uid`) REFERENCES `Users`(`uid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['BaseModel'] = (
    "CREATE TABLE `BaseModel` ("
    "  `bmid` INT NOT NULL,"
    "  `creator_uid` INT NOT NULL,"
    "  `description` TEXT NOT NULL,"
    "  PRIMARY KEY (`bmid`),"
    "  FOREIGN KEY (`creator_uid`) REFERENCES `AgentCreator`(`uid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['CustomizedModel'] = (
    "CREATE TABLE `CustomizedModel` ("
    "  `bmid` INT NOT NULL,"
    "  `mid` INT NOT NULL,"
    "  PRIMARY KEY (`bmid`, `mid`),"
    "  FOREIGN KEY (`bmid`) REFERENCES `BaseModel`(`bmid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['Configuration'] = (
    "CREATE TABLE `Configuration` ("
    "  `cid` INT NOT NULL,"
    "  `client_uid` INT NOT NULL,"
    "  `content` TEXT NOT NULL,"
    "  `labels` TEXT NOT NULL,"
    "  PRIMARY KEY (`cid`),"
    "  FOREIGN KEY (`client_uid`) REFERENCES `AgentClient`(`uid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['InternetService'] = (
    "CREATE TABLE `InternetService` ("
    "  `sid` INT NOT NULL,"
    "  `provider` TEXT NOT NULL,"
    "  `endpoints` TEXT NOT NULL,"
    "  PRIMARY KEY (`sid`)"
    ") ENGINE=InnoDB"
)

TABLES['LLMService'] = (
    "CREATE TABLE `LLMService` ("
    "  `sid` INT NOT NULL,"
    "  `domain` TEXT,"
    "  PRIMARY KEY (`sid`),"
    "  FOREIGN KEY (`sid`) REFERENCES `InternetService`(`sid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['DataStorage'] = (
    "CREATE TABLE `DataStorage` ("
    "  `sid` INT NOT NULL,"
    "  `type` TEXT,"
    "  PRIMARY KEY (`sid`),"
    "  FOREIGN KEY (`sid`) REFERENCES `InternetService`(`sid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['ModelServices'] = (
    "CREATE TABLE `ModelServices` ("
    "  `bmid` INT NOT NULL,"
    "  `sid` INT NOT NULL,"
    "  `version` INT NOT NULL,"
    "  PRIMARY KEY (`bmid`, `sid`),"
    "  FOREIGN KEY (`bmid`) REFERENCES `BaseModel`(`bmid`) ON DELETE CASCADE,"
    "  FOREIGN KEY (`sid`) REFERENCES `InternetService`(`sid`) ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES['ModelConfigurations'] = (
    "CREATE TABLE `ModelConfigurations` ("
    "  `bmid` INT NOT NULL,"
    "  `mid` INT NOT NULL,"
    "  `cid` INT NOT NULL,"
    "  `duration` INT NOT NULL,"
    "  PRIMARY KEY (`bmid`, `mid`, `cid`),"
    "  FOREIGN KEY (`bmid`, `mid`) REFERENCES `CustomizedModel`(`bmid`, `mid`) ON DELETE CASCADE,"
    "  FOREIGN KEY (`cid`) REFERENCES `Configuration`(`cid`) ON DELETE CASCADE"
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
    # Connect to MySQL Server (not specific DB yet so we can drop/create it)
    config_no_db = DB_CONFIG.copy()
    del config_no_db['database']

    try:
        cnx = mysql.connector.connect(**config_no_db)
        cursor = cnx.cursor()

        #create db
        db_name = DB_CONFIG['database']

        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        except mysql.connector.Error:
            pass

        cursor.execute(f"CREATE DATABASE {db_name}")
        cnx.database = db_name

        # table creation order to make sure foreign key dependencies work
        table_creation_order = [
            'Users',
            'AgentCreator',
            'AgentClient',
            'BaseModel',
            'CustomizedModel',
            'Configuration',
            'InternetService',
            'LLMService',
            'DataStorage',
            'ModelServices',
            'ModelConfigurations'
        ]

        #create all tables
        for table_name in table_creation_order:
            ddl = TABLES.get(table_name)
            if ddl:
                try:
                    cursor.execute(ddl)
                except mysql.connector.Error as err:
                    print(f"Failed creating table {table_name}: {err}")
                    sys.exit(1)

        #import csv data
        for table_name in table_creation_order:
            #print("attempting to create table: ", table_name)
            file_path = os.path.join(folder_name, f"{table_name}.csv")

            if not os.path.exists(file_path):
                continue  # some tables may not have CSVs

            with open(file_path, 'r', encoding='utf-8') as csvfile:
                csv_reader = csv.reader(csvfile)
                rows = list(csv_reader)

                if not rows:
                    continue

                header = rows[0]      # first row = column names from CSV
                data_rows = rows[1:]  # remaining rows = actual data

                if not data_rows:
                    continue

                num_cols = len(data_rows[0])
                placeholders = ', '.join(['%s'] * num_cols)
                insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"

                cleaned_rows = []
                for row in data_rows:
                    cleaned_row = [None if val == 'NULL' else val for val in row]
                    cleaned_rows.append(cleaned_row)

                cursor.executemany(insert_sql, cleaned_rows)
                cnx.commit()

        cursor.close()
        cnx.close()

        print("Success")

    except Exception as e:
        print("Fail")
        #print(e)

# 2. Insert Agent Client -> Currently working!
def insert_agent_client(uid, username, email, card_number, card_holder, expiration_date, cvv, zip_code, interests):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()

        # 1. Insert into Users
        insert_user = (
            "INSERT INTO Users (uid, email, username) "
            "VALUES (%s, %s, %s)"
        )
        cursor.execute(insert_user, (uid, email, username))

        # 2. Insert into AgentClient
        insert_client = (
            "INSERT INTO AgentClient (uid, interests, cardholder, expire, cardno, cvv, zip) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        )
        cursor.execute(insert_client, (uid, interests, card_holder, expiration_date, card_number, cvv, zip_code))

        cnx.commit()
        print("Success")

        cursor.close()
        cnx.close()

    except mysql.connector.Error as err:
        print("Fail")


# 3. Add a customized model -> CURRENTLY WORKING
def add_customized_model(mid, bmid):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        
        query = "INSERT INTO CustomizedModel (bmid, mid) VALUES (%s, %s)"
        cursor.execute(query, (bmid, mid))
        
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
        
        query = ("SELECT i.sid, i.endpoints, i.provider "
                 "FROM InternetService i "
                 "JOIN ModelServices ms ON i.sid = ms.sid "
                 "WHERE ms.bmid = %s "
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
def count_customized_model(*bmid_list):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()

        # Convert all elements to integers
        bmid_list = [int(b) for b in bmid_list]

        # SQL must support multiple bmids, so use IN (...)
        placeholders = ','.join(['%s'] * len(bmid_list))

        query = (
            "SELECT b.bmid, b.description, COUNT(c.mid) AS customizedModelCount "
            "FROM BaseModel b "
            "LEFT JOIN CustomizedModel c ON b.bmid = c.bmid "
            f"WHERE b.bmid IN ({placeholders}) "
            "GROUP BY b.bmid, b.description "
            "ORDER BY b.bmid ASC"
        )

        cursor.execute(query, bmid_list)
        rows = cursor.fetchall()

        # Output format: each row printed as CSV
        for row in rows:
            print(','.join(map(str, row)))

        cursor.close()
        cnx.close()

    except mysql.connector.Error:
        print("Fail")


# 7. Find Top-N longest duration configuration
def top_n_duration_config(uid, n):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        
        query = (
            "SELECT C.client_uid AS uid, C.cid, C.labels AS label, C.content, MC.duration "
            "FROM Configuration C "
            "JOIN ModelConfigurations MC ON C.cid = MC.cid "
            "WHERE C.client_uid = %s "
            "ORDER BY MC.duration DESC "
            "LIMIT %s"
        )
        
        cursor.execute(query, (uid, int(n)))
        
        rows = cursor.fetchall()
        for row in rows:
            print(','.join(map(str, row)))
            
        cursor.close()
        cnx.close()
    except mysql.connector.Error:
        print("Fail")

# 8. Keyword search
def list_base_model_keyword(keyword):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()

        search_term = f"%{keyword}%"

        query = (
            "SELECT DISTINCT b.bmid, i.sid, i.provider, l.domain "
            "FROM BaseModel b "
            "JOIN ModelServices ms ON b.bmid = ms.bmid "
            "JOIN InternetService i ON ms.sid = i.sid "
            "JOIN LLMService l ON i.sid = l.sid "
            "WHERE l.domain LIKE %s "
            "ORDER BY b.bmid ASC "
            "LIMIT 5"
        )

        cursor.execute(query, (search_term,))
        rows = cursor.fetchall()

        for row in rows:
            print(','.join(map(str, row)))

        cursor.close()
        cnx.close()

    except mysql.connector.Error:
        print("Fail")

def print_nl2sql_results():
    """
    Reads nl2sql_results.csv and prints it in the required table format.
    The CSV must include the columns:
        NLquery_id,
        NLquery,
        LLM_model_name,
        prompt,
        LLM_returned_SQL_id,
        LLM_returned_SQL_query,
        SQL_correct,
        ... plus any error columns
    """

    csv_path = "NL2SQL_results - Sheet1.csv"

    if not os.path.exists(csv_path):
        print("Fail")
        print("NL2SQL_results - Sheet1.csv not found.")
        return

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if not rows:
            print("Fail")
            print("CSV is empty.")
            return

        header = rows[0]
        data_rows = rows[1:]

        # Print header
        print(",".join(header))

        # Print each row
        for row in data_rows:
            print(",".join(row))

        print("Success")

    except Exception as e:
        print("Fail")
        print(e)

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

def run_sql_command(sql):
    """
    Executes an arbitrary SQL command and prints results as CSV rows.
    Use only for SELECT queries.
    """
    print("attempt to run sql command")
    try:
        cnx = get_connection()
        cursor = cnx.cursor()

        cursor.execute(sql)
        rows = cursor.fetchall()
        #print(len(rows))

        # Print results as CSV-style output
        for row in rows:
            print(','.join(map(str, row)))

        cursor.close()
        cnx.close()
        print("Success")

    except mysql.connector.Error as err:
        print("Fail")
        print("Error:", err)

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
        count_customized_model(*sys.argv[2:])
    elif command == 'topNDurationConfig':
        top_n_duration_config(sys.argv[2], sys.argv[3])
    elif command == 'listBaseModelKeyWord':
        list_base_model_keyword(sys.argv[2])
    elif command == "printNL2SQLresult":
        print_nl2sql_results()
    elif command == 'showTables':
        show_tables()
        debug_show_all_tables()
    elif command == 'runsql':
        # Everything after the command is the SQL text
        sql = " ".join(sys.argv[2:])
        run_sql_command(sql)


if __name__ == '__main__':
    main()

