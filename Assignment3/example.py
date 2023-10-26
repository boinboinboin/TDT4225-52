from pprint import pprint 
from DbConnector import DbConnector
import os
import time
from datetime import datetime
from pathlib import Path
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from tabulate import tabulate
from haversine import haversine



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
        print("Query 1: Number of users, activities and trackpoints")

        user_count = self.db["User"].count_documents({})
        activity_count = self.db["Activity"].count_documents({})
        trackpoint_count =self.db["TrackPoint"].count_documents({})

        print("Number of users:", user_count)
        print("Number of activities:", activity_count)
        print("Number of trackpoints:", trackpoint_count)

    def query2(self):
        print("\nQuery 2: Average number of activities per user")
        # Since every activity must have a user id, we can just count the number of users and activities
        user_count = self.db["User"].count_documents({})
        activity_count = self.db["Activity"].count_documents({})
        print("Average number of activities per user:", activity_count / user_count)

    def query3(self):
        print("\nQuery 3: 20 users with the most activities")

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
        # print("Top 20 users with the highest number of activities:", result)
        
        tabulate_data = [[document["_id"], document["count"]] for document in result]
        print(tabulate(tabulate_data, headers=["User ID", "Count"], tablefmt="grid"))

    def query4(self):
        print("\nQuery 4: User ID's who have taken a taxi")

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
        # print("Users who have taken a taxi:", result)
        
        tabulate_data = [(document["_id"]) for document in result]
        # print(tabulate(tabulate_data, headers=["User ID"], tablefmt="grid"))
        print(tabulate_data)
        print("Total: ", str(len(result)), " users")

    def query5(self):
        print("\nQuery 5:")

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
        # print("Transportation modes and their count:", result)
        
        tabulate_data = [[document["_id"], document["count"]] for document in result]
        print(tabulate(tabulate_data, headers=["Transportation Mode", "Count"], tablefmt="grid"))
        print("Total: ", str(len(result)), " transportation modes")


    def query6(self):

        ###### 6a ######
        print("\nQuery 6a: Year with most activities: ")

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
        print(year_most_activities)

        ###### 6b ######
        print("\nQuery 6b: Year with most recorded hours:")

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

        print(year_most_recorded_hours)

        if year_most_activities == year_most_recorded_hours:
            print("The year with the most activities is also the year with the most recorded hours")

        else:
            print("The year with the most activities is not the year with the most recorded hours")


    def query7(self):
        print("\nQuery 7: Distance walked by user 112:")
        distance_travveled = 0
        
        activities = self.db["Activity"]
        trackpoints = self.db["TrackPoint"]
        
        # find all activities with user_id 112 and transportation_mode walk
        result_activities = activities.find({"user_id": "112", "transportation_mode": "walk"}, {"_id": 1})
        
        # get all activity ids from the result
        activity_ids = [document["_id"] for document in result_activities]
        # print("number of activities: " + str(len(activity_ids)))
        
        # find all trackpoints with the activity_ids from above, make it into list of dict's
        latLonList = list(trackpoints.find({"activities_id": {"$in": activity_ids}}, {"lat": 1, "lon": 1, "_id": 0}))
        # print("number of trackpoints: " + str(len(latLonList)))
        
        # iterate over the list of dict's and calculate the distance between each point based on haversine formula
        for index, dict in enumerate(latLonList):
            if index == 0:
                continue
            # previous lat and lon values
            point1 = (float(latLonList[index-1]["lat"]), float(latLonList[index-1]["lon"]))
            # current values
            point2 = (float(dict["lat"]), float(dict["lon"]))
            distance_travveled += haversine(point1, point2, unit="km")
        print(str(int(distance_travveled)) + "km")
    
    def query8(self):
        print("Query 8: 20 users who have gained the most altitude meters:")

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

        print(results)
        
        tabulate_data = [[document["_id"], document["total_altitude_gain"]] for document in results]
        print(tabulate(tabulate_data, headers=["User ID", "Total Altitude Gain"], tablefmt="grid"))

    def query9(self):

        print("Query 9: Users with invalid activities:")

        pipeline_invalid_activities = [
            {
                "$sort": {
                    "activities_id": 1,
                    "date_time": 1
                }
            },
            {
                "$group": {
                    "_id": "$activities_id",
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
                            "$gte": 5 * 60  # Convert 5 minutes to seconds
                        }
                    }
                }
            },
            {
                "$project": {
                    "activity_id": "$_id"
                }
            }
        ]

        invalid_activities = list(self.db["TrackPoint"].aggregate(pipeline_invalid_activities))
        print(invalid_activities)
        print("Total: ", str(len(invalid_activities)), " activities")
        
    def query10(self):
        print("\nQuery 10: Users with an activity in the Forbidden City of Beijing:")
        trackpoints = self.db["TrackPoint"]
        
        
        pipeline = [
            {
                "$addFields": {
                    "doubleLat": {"$toDouble": "$lat"},
                    "doubleLon": {"$toDouble": "$lon"}
                }
            },
            {
                "$addFields": {
                    "rounded_lat": {"$round": ["$doubleLat", 3]},
                    "rounded_lon": {"$round": ["$doubleLon", 3]}
                }
            },
            {
                "$match": {
                    "rounded_lat": 39.916,
                    "rounded_lon": 116.397
                }
            },
            {
                "$project": {
                    "activities_id": 1
                }
            }
        ]
        result = trackpoints.aggregate(pipeline)
        # Activity ids with lat: 39.916 and lon: 116.397:
        activity_ids = {document["activities_id"] for document in result}
        activities = self.db["Activity"]
        result = activities.find({"_id": {"$in": list(activity_ids)}}, {"user_id": 1})
        user_ids = {document["user_id"] for document in result}
        print(user_ids)
        print("Total: ", str(len(user_ids)), " users")
        
    def query11(self):
        print("Query 11: Users with a transportation mode, and their most used transportation mode:")

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
        program.query8()
        program.query9()
        program.query10()
        program.query11()

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
