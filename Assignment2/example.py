from DbConnector import DbConnector
from tabulate import tabulate
from pathlib import Path
from datetime import datetime
import os

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, query, table_name):
        try:
            self.cursor.execute(query)
            self.db_connection.commit()
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"ERROR: Failed to create table {table_name}:", e)


    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def insert_into_activity(self, activities):
        
        for activity in activities:
            user_id, transportation_mode, start_date_time, end_date_time = activity
            query = (
                "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)"
                " VALUES (%s, %s, %s, %s)"
            )
            values = (user_id, transportation_mode, start_date_time, end_date_time)
            self.cursor.execute(query, values)
            
        self.db_connection.commit()
           

    def insert_into_user(self, sorted_users):
        for user in sorted_users:
            
            id, has_labels = user

            query = "INSERT INTO User (id, has_labels) VALUES (%s, %s)"
            self.cursor.execute(query, (id, has_labels))
        self.db_connection.commit()

    def insert_data_into_sql(self):

        print("running")

        # Read the ids from labeled_ids.txt and store ids in an array;
        ids = []
        try:
            with open('dataset/dataset/labeled_ids.txt', 'r') as file:
                # Iterate over each line in the file
                for line in file:
                    # Strip white space and add to ids list
                    ids.append(line.strip())
        except FileNotFoundError:
            print("The file data.txt was not found.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

        # Now 'ids' list contains all the ids from the file
        print("Here are ids")
        print(ids)
        
        root_dir = os.getcwd()
        users = []
        activities = []
        for foldername, _, filenames in os.walk(root_dir + "/dataset/dataset/Data"):
          
            # Check if the foldername ends with "Trajectory", if not, it is a user folder
            if not foldername.endswith("Trajectory"):
                id = foldername[-3:]
                
                # TODO Come back to this shit - very not good
                # code includes folder "data", shouldn't
                if id == "ata":
                    continue
                
                # Flag and append the user with has_labels if the id is in the id's list
                if id in ids:
                    users.append((id, 1))

                # Flag and append the user with has_labels if the id is not in the id's list
                else:
                    users.append((id, 0))
                    
            for file in filenames:
                # only handle plt files
                if file.endswith("plt"):
                    user_id = foldername[-14:-11]
                    with open(foldername + '/' + file) as file:
                        # Check if the plt file contains fewer or exactly 2500 lines
                        if len(file.readlines()) <= 2500:
                            for i, line in enumerate(file):
                                # Retrieve datetime from the first valid line in the file, which is always the 6th
                                if i == 6:
                                    columns = line.strip().split(',')
                                    start_date, start_time = columns[5], columns[6]
                                    start_date_time = start_date + " " + start_time
                                # Retrieve datetime from the last valid line in the file.
                                columns_lastline = line.strip().split(',')
                                end_date, end_time = columns_lastline[5], columns_lastline[6]
                                end_date_time_str = end_date + " " + end_time
                                end_date_time = datetime.strptime(end_date_time_str, '%Y-%m-%d %H:%M:%S')
                                
                                # Append the activity to the activities list
                                activities.append((user_id, None, start_date_time, end_date_time))
                  

        # Sort the user data and then insert it into the database
        sorted_users = sorted(users, key=lambda x: x[0])
        self.insert_into_user(sorted_users)
        
        # Insert the activity data into the database
        self.insert_into_activity(activities)
        print("inserted activities starting:")
        for foldername, _, filenames in os.walk(root_dir + "/dataset/dataset/Data"):
            for file in filenames:
                # only handle txt files
                if file.endswith("txt"):
                    user_id = foldername[-3:]
                    labeled_activities = []
                    with open(foldername + '/' + file) as file:
                        for i, line in enumerate(file):
                            if i == 0:
                                continue
                            line = line.strip().split('\t')
                            start_date_time_str, end_date_time_str, transportation_mode = line[0], line[1], line[2]
                            start_date_time = datetime.strptime(start_date_time_str, '%Y/%m/%d %H:%M:%S')
                            end_date_time = datetime.strptime(end_date_time_str, '%Y/%m/%d %H:%M:%S')
                            labeled_activities.append((user_id, transportation_mode, start_date_time, end_date_time))
                    # with open(foldername + '/' + file) as file:
                    #     counter = 1
                    #     for line in file:
                    #         if counter < 6:
                    #             counter += 1
                    #         else:
                    #             line = line.split(',')
                    #             # Line is now list of data values
                    #             # Example data: 39.906631, 116.385564, 0, 492, 40097.5864583333, 2009-10-11, 14:04:30
                    #             # Latitude in decimal degrees index 0
                    #             # Longitude in decimal degrees index 1
                    #             # Ignore index 2
                    #             # Altitude in feet index 3 (-777 if not valid)
                    #             # Date - number of days (with fractional part) since 12/30/1899 index 4
                    #             # Date as a string index 5
                    #             # Time as a string index 6
                                
                                
                    #         pass
            

            
            # Note: Ensure you handle potential exceptions, e.g., if the file is not found or the data is not in the expected format.

            # for filename in filenames:
            #     print(filename)
        
        # Insert all the users into the User table in in MySQL
        #print(users)

        # Sort the user list based in id's
        
        
    
        # Example data: 39.906631, 116.385564, 0, 492, 40097.5864583333, 2009-10-11, 14:04:30
        # Latitude in decimal degrees field 1
        # Longitude in decimal degrees field 2
        # Ignore field 3
        # Altitude in feet field 4 (-777 if not valid)
        # Date - number of days (with fractional part) since 12/30/1899 field 5
        # Date as a string field 6
        # Time as a string field 7
        

        # Some of the users has labels, found in labeled_ids.txt
        return
        

def main():
    program = None
    try:
        program = ExampleProgram()

        
        # Define the queries for creating tables
        # Person Query
        person_table_query =    """CREATE TABLE IF NOT EXISTS Person (
                                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                                    name VARCHAR(30))"""
      
        # User Query
        user_table_query =      """CREATE TABLE IF NOT EXISTS User (
                                    id VARCHAR(255) NOT NULL PRIMARY KEY,
                                    has_labels BOOLEAN)"""

        # Activity Query
        activity_table_query =  """CREATE TABLE IF NOT EXISTS Activity (
                                    id INT AUTO_INCREMENT PRIMARY KEY,
                                    user_id VARCHAR(255) NOT NULL,
                                    transportation_mode VARCHAR(255),
                                    start_date_time DATETIME,
                                    end_date_time DATETIME,
                                    FOREIGN KEY (user_id) REFERENCES User(id))"""

        # Trackpoint Query
        trackpoint_table_query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                                      id INT AUTO_INCREMENT PRIMARY KEY,
                                      activity_id INT NOT NULL,
                                      lat DOUBLE,
                                      lon DOUBLE,
                                      altitude INT,
                                      date_days DOUBLE,
                                      date_time DATETIME,
                                      FOREIGN KEY (activity_id) REFERENCES Activity(id))"""
        
        # Creating the tables
        
       
        program.drop_table(table_name="TrackPoint")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="User")
        program.drop_table(table_name="Person")

        program.create_table(person_table_query, "Person")
        program.create_table(user_table_query, "User")
        program.create_table(activity_table_query, "Activity")
        program.create_table(trackpoint_table_query, "TrackPoint")


        # Insert data to the Person table
        program.insert_data(table_name="Person")

        # Fetch data from the Person table
        _ = program.fetch_data(table_name="Person")

        # Display the tables
        program.show_tables()

        program.insert_data_into_sql()
        
        # Delete the tables we created
        # Important that we do it in this order so that we don't break any foreign key constraints
        

        # Jada
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
