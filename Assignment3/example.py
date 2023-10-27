from pprint import pprint 
from DbConnector import DbConnector
import os
import time
from datetime import datetime
from pathlib import Path
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne



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
        # bulk = collection.initialize_unordered_bulk_op()
        bulk_operation_list = []

        for activity in labeled_activities:
            # user_id, transportation_mode, start_date_time, end_date_time = activity
            user_id = activity.get("user_id")
            start_date_time = activity.get("start_date_time")
            end_date_time = activity.get("end_date_time")
            # Fetch activities with the same user_id, start_date_time and end_date_time
            # matching_activity = collection.update_one({
            #     "user_id": user_id,
            #     "start_date_time": start_date_time,
            #     "end_date_time": end_date_time
            # }, {
            #     "$set": {"transportation_mode": activity.get("transportation_mode")}
            # })
            
            # # 0 count means no match was found
            # if matching_activity.matched_count == 0:
            #     self.counter_transportation_ignored += 1
            # else:
            #     self.counter_transportation += 1
            
            bulk_operation_list.append(UpdateOne({
                "user_id": user_id,
                "start_date_time": start_date_time,
                "end_date_time": end_date_time
            }, {
                "$set": {"transportation_mode": activity.get("transportation_mode")}
            }))
            
        result = collection.bulk_write(bulk_operation_list)
        print("updated " + str(result.modified_count) + " labeled activities")

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


    def query1(self):
        print("Query 1:")

        user_count = self.db["User"].count_documents({})
        activity_count = self.db["Activity"].count_documents({})
        trackpoint_count =self.db["TrackPoint"].count_documents({})

        print("Number of users:", user_count)
        print("Number of activities:", activity_count)
        print("Number of trackpoints:", trackpoint_count)

    def query2(self):
        print("Query 2:")

        pipeline = [
            {
                "$group": {
                    "_id": "$userId",  
                    "count": {"$sum": 1}  
                }
            },
            {
                "$group": {
                    "_id": None,
                    "averageActivitiesPerUser": {"$avg": "$count"}  
                }
            }
        ]

        result = list(self.db["Activity"].aggregate(pipeline))
        average_activities_per_user = result[0]['averageActivitiesPerUser'] if result else 0

        print(f"Average number of activities per user: {average_activities_per_user}")

    def query3(self):
        print("Query 3:")

        pipeline = [
            {
                "$group": {
                    "_id": "$user_id",  
                    "count": {"$sum": 1}  
                }
            },
            {
                "$sort": {
                    "count": -1
                }
            },
            {
                "$limit": 20
            }
        ]

        result = list(self.db["Activity"].aggregate(pipeline))
        print("Top 20 users with the highest number of activities:", result)

    def query4(self):
        print("Query 4:")

        pipeline = [
            {
                "$match": {
                    "transportation_mode": "taxi"
                }
            },
            {
                "$group": {
                    "_id": "$user_id"
                }
            }
        ]

        result = list(self.db["Activity"].aggregate(pipeline))
        print("Users who have taken a taxi:", result)

    def query5(self):
        print("Query 5:")

        pipeline = [
            {
                "$match": {
                    "transportation_mode": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": "$transportation_mode",
                    "count": {"$sum": 1}
                }
            }
        ]

        result = list(self.db["Activity"].aggregate(pipeline))
        print("Transportation modes and their count:", result)


    def query6(self):

        ###### 6a ######
        print("Query 6a:")

        pipeline = [
            {
                "$group": {
                    "_id": {"$year": "$start_date_time"},
                    "count": {"$sum": 1},
                }
            },
            {
                "$sort": {
                    "count": -1
                }
            },
            {
                "$limit": 1
            }
        ]

        year_most_activities = list(self.db["Activity"].aggregate(pipeline))

        print("Year with the most activities:", year_most_activities)

        ###### 6b ######
        print("Query 6b:")

        pipeline = [
            {
                "$group": {
                    "_id": {"$year": "$start_date_time"},
                    "count": {"$sum": {"$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 3600000]}},
                }
            },
            {
                "$sort": {
                    "count": -1
                }
            },
            {
                "$limit": 1
            }
        ]

        year_most_recorded_hours = list(self.db["Activity"].aggregate(pipeline))

        print("Year with the most recorded hours:", year_most_recorded_hours)

        if year_most_activities == year_most_recorded_hours:
            print("The year with the most activities is also the year with the most recorded hours")

        else:
            print("The year with the most activities is not the year with the most recorded hours")

    
    def query7(self):
        print("Query 7:")

        pipeline = [
            {
                "$match": {
                    "user_id": "112",
                    "transportation_mode": "walk",
                    "start_date_time": {"$gte": datetime(2008, 1, 1, 0, 0, 0)},

                }
            },
            {
                "$group": {
                    "_id": "$user_id",
                    "total_distance": {"$sum": "$distance"}
                }
            }
        ]

        result = list(self.db["Activity"].aggregate(pipeline))

        print("Total distance walked by user 112 in 2008:", result[0]['total_distance'])

    
    def query8(self):
        print("Query 8:")

        pipeline = [
            # Sort by activities_id and date_time
            {
                "$sort": {
                    "activities_id": 1,
                    "date_time": 1
                }
            },
            # Calculate the altitude change for each activity
            {
                "$group": {
                    "_id": "$activities_id",
                    "first_altitude": {"$first": "$alt"},
                    "last_altitude": {"$last": "$alt"}
                }
            },
            {
                "$project": {
                    "altitude_change": {
                        "$subtract": [
                            {"$toDouble": "$last_altitude"},
                            {"$toDouble": "$first_altitude"}
                        ]
                    }
                }
            },
            # Get only the activity changes > 0
            {
                "$match": {
                    "altitude_change": {"$gt": 0}
                }
            },
            # Sum all the altitude changes for each activity for each user
            {
                "$group": {
                    "_id": "$_id",  
                    "total_altitude_gain": {"$sum": "$altitude_change"}
                }
            },
            {
                "$sort": {
                    "total_altitude_gain": -1
                }
            },
            # Get the top 20 users with most total altitude gained
            {
                "$limit": 20
            }
        ]

        # Execute the aggregation
        results = list(self.db["TrackPoint"].aggregate(pipeline))

        print("Top 20 users with the most total gained altitude meters: ", results)

    def query9(self):

        print("Query 9:")

        pipeline_invalid_activities = [
            {
                "$sort": {
                    "activities_id": 1,
                    "date_time": 1
                }
            },
            {
                "$lookup": {
                    "from": "Activity",
                    "localField": "activities_id",
                    "foreignField": "_id",
                    "as": "activity"
                }
            },
            {
                "$unwind": "$activity"
            },
            {
                "$group": {
                    "_id": {
                        "user_id": "$activity.user_id",
                        "activities_id": "$activities_id"
                    },
                    "trackpoints": {
                        "$push": {
                            "date_time": "$date_time",
                            "_id": "$_id"
                        }
                    }
                }
            },
            {
                "$project": {
                    "deviations": {
                        "$map": {
                            "input": {"$range": [0, {"$subtract": [{"$size": "$trackpoints"}, 1]}]},
                            "as": "idx",
                            "in": {
                                "$subtract": [
                                    {"$arrayElemAt": ["$trackpoints.date_time", {"$add": ["$$idx", 1]}]},
                                    {"$arrayElemAt": ["$trackpoints.date_time", "$$idx"]}
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$match": {
                    "deviations": {
                        "$elemMatch": {
                            "$gte": 5 * 60  
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "invalid_activities_count": {
                        "$sum": 1
                    }
                }
            },
            {
                "$project": {
                    "user_id": "$_id",
                    "invalid_activities_count": 1,
                    "_id": 0
                }
            }
        ]

        invalid_activities = list(self.db["TrackPoint"].aggregate(pipeline_invalid_activities))
        print(invalid_activities)



    
    def query11(self):
        print("Query 11:")

        pipeline = [
            # Do not count the rows where mode is null
            {
                "$match": {
                    "transportation_mode": {"$ne": None}
                }
            },
            # Group by user_id and transportation_mode
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "transportation_mode": "$transportation_mode"
                    },
                    "count": {"$sum": 1}
                }
            },
            # Get the most used transportation mode for each user
            {
                "$sort": {
                    "_id.user_id": 1,
                    "count": -1,  
                    "_id.transportation_mode": 1  
                }
            },
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "most_used_transportation_mode": {"$first": "$_id.transportation_mode"}
                }
            },
            # Sort by user_id
            {
                "$sort": {
                    "_id": 1
                }
            }
        ]

        results = list(self.db["Activity"].aggregate(pipeline))

        print("Users with the most used transportation mode:", results)

        # Print the results
        # for result in results:
        #     print(f"User ID: {result['_id']}, Most Used Transportation Mode: {result['most_used_transportation_mode']}")

        
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
        print("working dir: " + root_dir)
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
                        print(id, "not labels")

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
                                        
                                        columns = line.strip().split(',')
                                        lat, lon, alt, date_days, date, the_time = columns[0], columns[1], columns[3], columns[4], columns[5], columns[6]
                                        date_time = date + " " + the_time
                                        # This returns an error if a date does not match the correct format,
                                        # so no corrupted data is added to the database
                                        date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
                                        trackpoints.append({
                                            "_id": self.counter_trackpoints,
                                            "activities_id": activities_id,
                                            "lat": lat,
                                            "lon": lon,
                                            "alt": alt,
                                            "date_days": date_days,
                                            "date_time": date_time
                                        })
                                        self.counter_trackpoints += 1
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
                                            "_id" : activities_id,
                                            "user_id":   user_id,
                                            "transportation_mode": None, 
                                            "start_date_time":  start_date_time,
                                            "end_date_time": end_date_time
                                        })
                                        activities_id += 1
                            else:
                                self.counter_ignored += 1

        sorted_users = sorted(users, key=lambda x: x['_id'])
        tik = time.time()
        self.insert_into_user(sorted_users)
        tok = time.time()
        print("finished inserting users after: " + str(tok - tik) + " seconds.")

        print("starting inserting activities into db...")
        tik = time.time()
        self.insert_into_activity(activities)
        tok = time.time()
        print("finished inserting activities after: " + str(tok - tik) + " seconds.")

        labeled_activities = []
        for foldername, _, filenames in os.walk(root_dir + "/dataset/dataset/Data"):
            for file in filenames:
                # only handle txt files
                if file.endswith("txt"):
                    user_id = foldername[-3:]
                    with open(foldername + '/' + file) as file:
                        for i, line in enumerate(file):
                            # Skip first line
                            if i == 0:
                                continue
                            line = line.strip().split('\t')
                            start_date_time_str, end_date_time_str, transportation_mode = line[0], line[1], line[2]
                            start_date_time = datetime.strptime(start_date_time_str, '%Y/%m/%d %H:%M:%S')
                            end_date_time = datetime.strptime(end_date_time_str, '%Y/%m/%d %H:%M:%S')
                            #labeled_activities.append((user_id, transportation_mode, start_date_time, end_date_time))
                            labeled_activities.append({
                                            "user_id":   user_id,
                                            "transportation_mode": transportation_mode, 
                                            "start_date_time":  start_date_time,
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

        program.query1()
        program.query2()
        program.query3()
        program.query4()
        program.query5()
        program.query6()
        program.query7()
        #program.query8()
        program.query9()
        #program.query11()

        # program.drop_colls(["User", "Activity", "TrackPoint"])

        # program.create_colls(["User", "Activity", "TrackPoint"])

        #program.drop_colls(["User", "Activity", "TrackPoint"])
        

        # program.insert_data_into_mongo_db()
        # program.show_coll()

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
