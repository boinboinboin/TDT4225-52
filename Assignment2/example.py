from DbConnector import DbConnector
from tabulate import tabulate
from pathlib import Path
from datetime import datetime
import os
import time

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        # self.db_connection.query('SET GLOBAL connect_timeout=28800')
        # self.db_connection.query('SET GLOBAL interactive_timeout=28800')
        # self.db_connection.query('SET GLOBAL wait_timeout=28800')
        self.cursor = self.connection.cursor
        self.counter_ignored = 0
        self.counter_trackpoints = 0
        self.counter_transportation = 0
        self.counter_transportation_ignored = 0

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
            query = "INSERT IGNORE INTO %s (name) VALUES ('%s')"
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
        query = (
            "INSERT IGNORE INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time)"
            " VALUES (%s, %s, %s, %s, %s)"
        )
        self.cursor.executemany(query, activities)
        self.db_connection.commit()
           

    def insert_into_user(self, sorted_users):

        query = "INSERT IGNORE INTO User (id, has_labels) VALUES (%s, %s)"
        self.cursor.executemany(query, sorted_users)
        self.db_connection.commit()
    
    def insert_transportation_into_activity(self, labeled_activities):
        # TODO, this can be done much quicker if fixed before inserting activities into database
        filtered_activities = []
        length = len(labeled_activities)
        for activity in labeled_activities:
            user_id, transportation_mode, start_date_time, end_date_time = activity
            # Check that the user has labels.txt
            # Commented out because its unnecessary
            # self.cursor.execute("SELECT has_labels FROM User WHERE id = %s", (user_id,))
            # has_labels = self.cursor.fetchone()[0]
            # if has_labels == 1:
            # First we obtain all the start and end dates from each activity for the user
            self.cursor.execute("SELECT start_date_time, end_date_time FROM Activity WHERE user_id = %s", (user_id,))
            dates = self.cursor.fetchall()
            for dates_db in dates:
                # We check if any of these start - end date pairs match up exactly with the labeled activity
                start_date_db, end_date_db = dates_db
                if start_date_time == start_date_db and end_date_time == end_date_db:
                    # print("found match for user: " + user_id + " " + str(start_date_time) + " " + str(end_date_time))
                    values = (transportation_mode, user_id, start_date_time)
                    filtered_activities.append(values)
                    self.counter_transportation += 1
                    break
                else:
                    self.counter_transportation_ignored += 1
                    continue
            # else:
                # print("User " + user_id + " has no labels.txt, but isn't listed as labeled in labeled_ids.txt")
        query = ("UPDATE Activity SET transportation_mode = %s WHERE user_id = %s AND start_date_time = %s")
        print("ignored " + str(self.counter_transportation_ignored) + " labeled activities")
        print("inserted " + str(self.counter_transportation) + " labeled activities")
        print(length - self.counter_transportation, "labeled activities with no date match")
        # self.cursor.executemany(query, filtered_activities)
        # self.db_connection.commit()
        
    def insert_trackpoints(self, trackpoints):
        query = (
            "INSERT IGNORE INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
        )
        # Split the trackpoints into chunks to avoid memory error, tried to change mysql settings 
        # to circumvent this but it didn't work.
        n = 500000
        splicedTrackpoints = [trackpoints[i:i + n] for i in range(0, len(trackpoints), n)]
        size = len(splicedTrackpoints)
        print("spliced trackpoints into " + str(size) + " chunks")
        
        for index, tuple in enumerate(splicedTrackpoints):
            print(size - index, "chunks left")
            self.cursor.executemany(query, tuple)
            self.db_connection.commit()

    def query1(self):
        # how many users, activities and trackpoints are there in the dataset
        query = (
            """
            SELECT 
            (SELECT COUNT(*) FROM User) AS users, 
            (SELECT COUNT(*) FROM Activity) AS activities, 
            (SELECT COUNT(*) FROM TrackPoint) AS trackpoints
            """
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
    
    def query2(self):
        query = (
            """
            SELECT avg(number_of_activities), min(number_of_activities), max(number_of_activities) 
                FROM 
                (
                    SELECT count(Activity.user_id) as number_of_activities
                    FROM User
                    LEFT JOIN Activity
                    ON (Activity.user_id = query1.id)
                    GROUP BY query1.id
                ) AS t;
            """
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        
    def query3(self):
        query = (
            """
            SELECT user_id, COUNT(*) AS activity_count 
            FROM Activity 
            GROUP BY user_id 
            ORDER BY activity_count 
            DESC LIMIT 15
            """
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        
    def query4(self):
        query = (
            """
            SELECT 
                DISTINCT user_id AS user_id
            FROM 
                Activity
            WHERE 
                transportation_mode = 'bus';
        """
                    )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        print(len(result))
        
    def query5(self):
        query = (
            """
            SELECT user_id, COUNT(DISTINCT transportation_mode) AS transportation_modes_count 
            FROM Activity 
            GROUP BY user_id 
            ORDER BY transportation_modes_count 
            DESC LIMIT 10
            """
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        
    def query6(self):
        query = (
            """
            SELECT 
            user_id,
            start_date_time,
            end_date_time,
            COUNT(*) AS occurrence_count
            FROM 
                Activity
            GROUP BY 
                user_id, start_date_time, end_date_time
            HAVING 
                COUNT(*) > 1;
        """
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        
    def query7a(self):
        query = (
            """
            SELECT COUNT(DISTINCT user_id) AS overnight_users 
            FROM Activity 
            WHERE DATE(start_date_time) = DATE(end_date_time - INTERVAL 1 DAY)
            """
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        
    def query7b(self):
        query = (
            """SELECT user_id, 
            transportation_mode, 
            TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) AS duration_minutes 
            FROM Activity 
            WHERE DATE(start_date_time) = DATE(end_date_time - INTERVAL 1 DAY) 
            ORDER BY user_id
            """
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        print(len(result))
        
    def query8(self):
        query = (
            "SELECT (SELECT COUNT(*) FROM User) AS users, (SELECT COUNT(*) FROM Activity) AS activities, (SELECT COUNT(*) FROM TrackPoint) AS trackpoints"
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        
    def query9(self):
        query = (
            "SELECT (SELECT COUNT(*) FROM User) AS users, (SELECT COUNT(*) FROM Activity) AS activities, (SELECT COUNT(*) FROM TrackPoint) AS trackpoints"
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        
    def query10(self):
        query = (
            "SELECT (SELECT COUNT(*) FROM User) AS users, (SELECT COUNT(*) FROM Activity) AS activities, (SELECT COUNT(*) FROM TrackPoint) AS trackpoints"
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        
    def query11(self):
        query = (
        """
        SELECT u.id AS user_id, COUNT(*) AS invalid_activity_count 
        FROM User AS u JOIN Activity AS a ON u.id = a.user_id LEFT JOIN (SELECT t1.activity_id, COUNT(*) AS invalid_tp_count 
        FROM TrackPoint AS t1 LEFT JOIN TrackPoint AS t2 ON t1.activity_id = t2.activity_id AND t1.id = t2.id + 1 
        WHERE TIMESTAMPDIFF(MINUTE, t2.date_time, t1.date_time) >= 5 GROUP BY t1.activity_id) AS invalid_t ON a.id = invalid_t.activity_id 
        WHERE invalid_t.invalid_tp_count IS NOT NULL GROUP BY u.id;
        """
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        print(len(result))
        
    def query12(self):
        query = (
        """
        SELECT user.id AS user_id,
               user.has_labels,
               COALESCE(most_used_mode, 'No Activities') AS most_used_transportation_mode
        FROM User user
        LEFT JOIN (
            SELECT query1.user_id, 
                   MAX(transp_count) AS max_count
            FROM (
                SELECT user_id, 
                       transportation_mode,
                       COUNT(*) AS transp_count
                FROM Activity
                WHERE transportation_mode IS NOT NULL
                GROUP BY user_id, transportation_mode
            ) AS query1
            GROUP BY query1.user_id
        ) AS max_counts
        ON user.id = max_counts.user_id
        LEFT JOIN (
            SELECT user_id, 
                   transportation_mode AS most_used_mode,
                   COUNT(*) AS transp_count
            FROM Activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
        ) AS query2
        ON user.id = query2.user_id AND query2.transp_count = max_counts.max_count
        WHERE user.has_labels = true;
        """
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        print(len(result))
        
    def insert_data_into_sql(self):

        print("Starting...")

        # Read the ids from labeled_ids.txt and store ids in an array;
        ids = []
        try:
            with open('Assignment2/dataset/dataset/labeled_ids.txt', 'r') as file:
                # Iterate over each line in the file
                for line in file:
                    # Strip white space and add to ids list
                    ids.append(line.strip())
        except FileNotFoundError:
            print("The file data.txt was not found.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

        # Now 'ids' list contains all the ids from the file
        
        root_dir = os.getcwd()
        print("getting users, activities and trackpoints from files...")
        tik = time.time()
        users = []
        activities = []
        activities_id = 0
        trackpoints = []
        for foldername, _, filenames in os.walk(root_dir + "/Assignment2/dataset/dataset/Data"):
          
            # Check if the foldername ends with "Trajectory", if not, it is a user folder
            if not foldername.endswith("Trajectory") and not foldername.endswith("Data"):
                id = foldername[-3:]
                
                # Flag and append the user with has_labels if the id is in the id's list
                if id in ids:
                    users.append((id, 1))

                # Flag and append the user with has_labels if the id is not in the id's list
                else:
                    users.append((id, 0))
            # This for loop populates the users, activities and trackpoints lists with values from the .plt files
            for file in filenames:
                # only handle plt files
                if file.endswith("plt"):
                    user_id = foldername[-14:-11]
                    with open(foldername + '/' + file) as file:
                        # Check if the plt file contains fewer or exactly 2500 lines
                        # 2506 since first 6 lines is the header
                        file = file.readlines()
                        if len(file) <= 2506:
                            for i, line in enumerate(file):
                                # ignore first 6 lines, rest is added to trackpoints array
                                if i > 6:
                                    self.counter_trackpoints += 1
                                    columns = line.strip().split(',')
                                    lat, lon, alt, date_days, date, the_time = columns[0], columns[1], columns[3], columns[4], columns[5], columns[6]
                                    date_time = date + " " + the_time
                                    # This returns an error if a date does not match the correct format,
                                    # so no corrupted data is added to the database
                                    date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
                                    trackpoints.append((activities_id, lat, lon, alt, date_days, date_time))
                                    # print("added trackpoint: " + str(activities_id) + " " + str(lat) + " " + str(lon) + " " + str(alt) + " " + str(date_days) + " " + str(date_time))
                                # The seventh line (index 6) is the first valid line in the file, and contains the start date and time of the activity
                                if i == 6:
                                    columns = line.strip().split(',')
                                    start_date, start_time = columns[5], columns[6]
                                    start_date_time = start_date + " " + start_time
                                    
                                # Last line gives the end date and time of the activity. The activity is then appended to the activities list
                                if i == len(file) - 1:
                                    columns_lastline = line.strip().split(',')
                                    end_date, end_time = columns_lastline[5], columns_lastline[6]
                                    end_date_time_str = end_date + " " + end_time
                                    end_date_time = datetime.strptime(end_date_time_str, '%Y-%m-%d %H:%M:%S')
                                    activities.append((activities_id ,user_id, None, start_date_time, end_date_time))
                                    activities_id += 1
                        else:
                            self.counter_ignored += 1
                  
        tok = time.time()
        print("got users, activities and trackpoints after: " + str(tok - tik) + " seconds.")
        print("ignored " + str(self.counter_ignored) + " files")
        print("obtained " + str(self.counter_trackpoints) + " trackpoints")
        # Sort the user data and then insert it into the database
        sorted_users = sorted(users, key=lambda x: x[0])
        print("inserting users into db...")
        tik = time.time()
        self.insert_into_user(sorted_users)
        tok = time.time()
        print("finished inserting users after: " + str(tok - tik) + " seconds.")
        
        # Insert the activity data into the database
        print("inserting activities into db...")
        tik = time.time()
        self.insert_into_activity(activities)
        tok = time.time()
        print("finished inserting activities after: " + str(tok - tik) + " seconds.")
        labeled_activities = []
        for foldername, _, filenames in os.walk(root_dir + "/Assignment2/dataset/dataset/Data"):
            for file in filenames:
                # only handle txt files
                if file.endswith("txt"):
                    user_id = foldername[-3:]
                    with open(foldername + '/' + file) as file:
                        for i, line in enumerate(file):
                            if i == 0:
                                continue
                            line = line.strip().split('\t')
                            start_date_time_str, end_date_time_str, transportation_mode = line[0], line[1], line[2]
                            start_date_time = datetime.strptime(start_date_time_str, '%Y/%m/%d %H:%M:%S')
                            end_date_time = datetime.strptime(end_date_time_str, '%Y/%m/%d %H:%M:%S')
                            labeled_activities.append((user_id, transportation_mode, start_date_time, end_date_time))
        print("inserting transportation into activities...")
        tik = time.time()
        self.insert_transportation_into_activity(labeled_activities)
        tok = time.time()
        print("finished inserting transportation after: " + str(tok - tik) + " seconds.")
        
        print("inserting trackpoints into db...")
        tik = time.time()
        self.insert_trackpoints(trackpoints)
        tok = time.time()
        print("finished inserting trackpoints after: " + str(tok - tik) + " seconds.")
        
        # Note: Ensure you handle potential exceptions, e.g., if the file is not found or the data is not in the expected format.
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
                                    id INT PRIMARY KEY,
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
        
       
        # program.drop_table(table_name="TrackPoint")
        # program.drop_table(table_name="Activity")
        # program.drop_table(table_name="User")
        # program.drop_table(table_name="Person")

        # program.create_table(person_table_query, "Person")
        # program.create_table(user_table_query, "User")
        # program.create_table(activity_table_query, "Activity")
        # program.create_table(trackpoint_table_query, "TrackPoint")


        # Insert data to the Person table
        # program.insert_data(table_name="Person")

        # Fetch data from the Person table
        # _ = program.fetch_data(table_name="Person")

        # Display the tables
        # program.show_tables()

        # Run the program:
        # program.insert_data_into_sql()
        
        # Run the queries:
        # program.query1()
        # program.query2()
        # program.query3()
        # program.query4()
        # program.query5()
        # program.query6()
        # program.query7a()
        # program.query7b()
        # program.query8()
        # program.query9()
        # program.query10()
        # program.query11()
        # program.query12()
        
        # Delete the tables we created
        # Important that we do it in this order so that we don't break any foreign key constraints
        # Jada
        # program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
