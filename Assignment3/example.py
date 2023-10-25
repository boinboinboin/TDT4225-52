from pprint import pprint 
from DbConnector import DbConnector
import os
import time
from datetime import datetime
from pathlib import Path
from pymongo.errors import BulkWriteError



class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        #self.cursor = self.connection.cursor
        self.counter_ignored = 0
        self.counter_trackpoints = 0
        self.counter_transportation = 0
        self.counter_transportation_ignored = 0

    def create_colls(self, coll_names):

        # Create the collections
        for coll_name in coll_names:
            self.db.create_collection(coll_name)
            print('Created collection: ', coll_name)

    def insert_into_user(self, sorted_users):

        collection = self.db["User"]
        collection.insert_many(sorted_users)


    def insert_into_activity(self, activities):
        
        collection = self.db["Activity"]
        collection.insert_many(activities)

    def insert_transportation_into_activity(self, labeled_activities):
    
        collection = self.db["Activity"]

        for activity in labeled_activities:
            activities_id, user_id, transportation_mode, start_date_time, end_date_time = activity

            # Fetch activities with the same user_id, start_date_time and end_date_time
            matching_activity = collection.find_one({
                "user_id": user_id,
                "start_date_time": start_date_time,
                "end_date_time": end_date_time
            })

            if matching_activity:
                # Update the transportation_mode of the matched activity
                collection.update_one({
                    "_id": matching_activity["_id"]
                }, {
                    "$set": {
                        "transportation_mode": transportation_mode
                    }
                })
                self.counter_transportation += 1
            else:
                self.counter_transportation_ignored += 1

        print("ignored " + str(self.counter_transportation_ignored) + " labeled activities")
        print("updated " + str(self.counter_transportation) + " labeled activities")
        print(len(labeled_activities) - self.counter_transportation, "labeled activities with no date match")


    def insert_trackpoints(self, trackpoints):
        collection = self.db["TrackPoint"]  

        n = 500000
        splicedTrackpoints = [trackpoints[i:i + n] for i in range(0, len(trackpoints), n)]
        size = len(splicedTrackpoints)
        print("spliced trackpoints into " + str(size) + " chunks")

        for index, chunk in enumerate(splicedTrackpoints):
            try:
                collection.insert_many(chunk, ordered=False)
            except BulkWriteError as bwe:
                print(bwe.details)
            print(size - index, "chunks left")


    def insert_documents(self, collection_name):
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                    {'code':'BOI1001', 'name': ' How to become a boi or boierinnaa'}
                    ] 
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT02', 'name': ' Advanced, Distributed Systems'},
                    ] 
            },
            {
                "_id": 3,
                "name": "Bobby",
            }
        ]  
        collection = self.db[collection_name]
        collection.insert_many(docs)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_colls(self, coll_names):

         # Drop the collections
        for coll_name in coll_names:
            collection = self.db[coll_name]
            collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

    def insert_data_into_mongo_db(self):

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
         
        root_dir = os.getcwd()
        users = []
        activities = []
        activities_id = 0
        trackpoints = []
        for foldername, _, filenames in os.walk(root_dir + "/dataset/dataset/Data"):
            
                # Check if the foldername ends with "Trajectory", if not, it is a user folder
                if not foldername.endswith("Trajectory") and not foldername.endswith("Data"):
                    id = foldername[-3:]
                    
                    # Flag and append the user with has_labels if the id is in the id's list
                    if id in ids:
                        print(id, "has labels")
                        users.append(
                            {
                                "_id": id,
                                "has_labels": 1
                            }
                        )

                    # Flag and append the user with has_labels if the id is not in the id's list
                    else:
                        print(id, "has labels")

                        users.append(
                            {
                                "_id": id,
                                "has_labels": 0
                            }
                        )
                
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
                                        start_date_time_str = start_date + " " + start_time
                                        
                                    # Last line gives the end date and time of the activity. The activity is then appended to the activities list
                                    if i == len(file) - 1:
                                        columns_lastline = line.strip().split(',')
                                        end_date, end_time = columns_lastline[5], columns_lastline[6]
                                        end_date_time_str = end_date + " " + end_time
                                        end_date_time = datetime.strptime(end_date_time_str, '%Y-%m-%d %H:%M:%S')
                                        start_date_time = datetime.strptime(start_date_time_str, '%Y-%m-%d %H:%M:%S')

                                        activities.append({
                                            "activities_id" : activities_id,
                                            "user_id":   user_id,
                                            "transportation_mode": None, 
                                            "start_date_time":  start_date_time,
                                            "end_date_time": end_date_time
                                        })
                                        activities_id += 1
                            else:
                                self.counter_ignored += 1

        sorted_users = sorted(users, key=lambda x: x['_id'])
        self.insert_into_user(sorted_users)
        print("Inserted users into db")

        self.insert_into_activity(activities)

        labeled_activities = []
        for foldername, _, filenames in os.walk(root_dir + "/dataset/dataset/Data"):
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
                            #labeled_activities.append((user_id, transportation_mode, start_date_time, end_date_time))
                            labeled_activities.append({
                                            "activities_id" : activities_id,
                                            "user_id":   user_id,
                                            "transportation_mode": transportation_mode, 
                                            "start_date_time":  end_date_time,
                                            "end_date_time": end_date_time
                                        })
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
        
        return



def main():
    program = None
    try:
        program = ExampleProgram()
        program.drop_colls(["User", "Activity", "TrackPoint"])

        program.create_colls(["User", "Activity", "TrackPoint"])

        #program.drop_colls(["User", "Activity", "TrackPoint"])
        

        program.insert_data_into_mongo_db()
        program.show_coll()

        # program.insert_documents(collection_name="Person")
        # program.fetch_documents(collection_name="Person")
        # program.drop_colls(["User", "Activity", "TrackPoint"])
        # program.drop_coll(collection_name='person')
        # program.drop_coll(collection_name='users')
        # Check that the table is dropped
        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
