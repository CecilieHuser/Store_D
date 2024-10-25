from pprint import pprint
from DbConnector import DbConnector
import datetime
from tabulate import tabulate
import numpy as np

class Part2:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    # 1. Count users, activities, and trackpoints
    def find_number_of(self):
        user_count = self.db['User'].count_documents({})
        activity_count = self.db['Activity'].count_documents({})

        # Unwinding trackpoints to count them
        trackpoint_count = self.db['Activity'].aggregate([
            {"$unwind": "$trackpoints"},
            {"$group": {"_id": None, "count": {"$sum": 1}}}
        ])

        trackpoint_count_value = list(trackpoint_count)[0]['count'] if trackpoint_count else 0
        print(f"Users: {user_count}, Activities: {activity_count}, Trackpoints: {trackpoint_count_value}")

    # 2. Average number of activities per user
    def find_avg_activities_per_user(self):
        user_activity_count = self.db['Activity'].aggregate([
            {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}}
        ])

        total_users = self.db['User'].count_documents({})
        total_activities = sum(item['activity_count'] for item in user_activity_count)

        if total_users > 0:
            average = total_activities / total_users
            print(f"The average number of activities per user is: {round(average, 2)}")
        else:
            print("No users found.")

    # 3. Find the top 20 users with the highest number of activities
    def find_most_active_20_users(self):
        top_users = self.db['Activity'].aggregate([
            {"$group": {"_id": "$user_id", "number_of_activities": {"$sum": 1}}}, 
            {"$sort": {"number_of_activities": -1}}, 
            {"$limit": 20}
        ])

        # print the results
        rows = [[user['_id'], user['number_of_activities']] for user in top_users]
        print(tabulate(rows, headers=['User ID', 'Activity Count'], tablefmt="fancy_grid"))

    # 4. Find all users who have taken a taxi
    def find_taxi_users(self):
        taxi_users = self.db['Activity'].distinct("user_id", {"transportation_mode": "taxi"})
        print(tabulate([[user] for user in taxi_users], headers=["User ID"], tablefmt="fancy_grid"))

    #5. Find all types of transportation modes and count how many activities that are
    # tagged with these transportation mode labels. Do not count the rows where the mode is null
    def count_transportation_modes(self):
        mode_counts = self.db['Activity'].aggregate([
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
        ])
        
        # Print the results
        rows = [[mode['_id'], mode['count']] for mode in mode_counts]
        print(tabulate(rows, headers=['Mode', 'Activity Count'], tablefmt="fancy_grid"))

    #6. a) Find the year with the most activities.
    def find_year_with_most_activities(self):
        year_activities = self.db['Activity'].aggregate([
            {"$group": {"_id": {"$year": "$start_time"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ])

        # Print the results
        year = list(year_activities)
        if year:
            print(f"Year with the most activities: {year[0]['_id']} with {year[0]['count']} activities")
        else:
            print("No activities found.")
        return year[0]['_id']
    
    # 6. b) Is this also the year with most recorded hours?
    def find_year_with_most_hours(self):
        year_hours = self.db['Activity'].aggregate([
            {
                "$group": {
                    "_id": {"$year": "$start_time"},
                    "recorded_hours": {
                        "$sum": {
                            "$divide": [
                                {"$subtract": ["$end_time", "$start_time"]},  # Calculate duration in milliseconds
                                3600000  # Convert milliseconds to hours
                            ]
                        }
                    }
                }
            },
            {"$sort": {"recorded_hours": -1}},  # Sort by recorded hours in descending order
            {"$limit": 1}  # Limit to the highest
        ])

        year_with_most_hours = list(year_hours)

        if year_with_most_hours:
            print(f"Year with the most recorded hours: {year_with_most_hours[0]['_id']} with {year_with_most_hours[0]['recorded_hours']:.2f} hours.")
        else:
            print("No recorded hours found.")
            return None, 0
        
        #Comparing to the year with the most activities
        most_activities_year = self.find_year_with_most_activities()
        if most_activities_year == year_with_most_hours[0]['_id']:
            print(f"Yes, the year {most_activities_year[0]} has the most activities and also the most recorded hours.")
        else:
            print(f"No, the year with the most activities ({most_activities_year}) is different from the year with the most recorded hours ({year_with_most_hours[0]['_id']}).")
        return year_with_most_hours

    #7. Find the total distance (in km) walked in 2008, by user with id=112
    def find_total_distance_walked_2008_user112(self):
        """
        Finds the total distance (in km) walked in 2008 by user with id=112 using the haversine formula.
        
        Source for haversine: https://stackoverflow.com/questions/29545704/fast-haversine-approximation-python-pandas/29546836#29546836
        """
        user_id = 112

        # Finding trackpoints for user 112 in 2008 where the person walked
        trackpoints = self.db['Activity'].aggregate([
            {
                "$match": {
                    "user_id": user_id,
                    "transportation_mode": "walk",
                    "start_time": {
                        "$gte": datetime.datetime(2008, 1, 1),
                        "$lt": datetime.datetime(2009, 1, 1),
                    }
                }
            },
            {"$unwind": "$trackpoints"},
            {
                "$project": {
                    "lat": "$trackpoints.lat",
                    "lon": "$trackpoints.lon",
                    "date_time": "$trackpoints.date_time"
                }
            }
        ])

        trackpoints_list = list(trackpoints)

        if len(trackpoints_list) < 2:
            return 0.0

        total_distance = 0.0

        # Loop through trackpoints and calculate the total distance in kilometers
        for i in range(1, len(trackpoints_list)):
            previous_point = trackpoints_list[i - 1]
            current_point = trackpoints_list[i]

            # Extract latitudes and longitudes for both points
            lat1, lon1 = previous_point['lat'], previous_point['lon']
            lat2, lon2 = current_point['lat'], current_point['lon']

            # Converting degrees to radians
            lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

            # Haversine formula components
            dlon = lon2 - lon1
            dlat = lat2 - lat1

            a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
            c = 2 * np.arcsin(np.sqrt(a))

            #Radius of Earth = 6378.137 km
            distance_km = 6378.137 * c

            # Accumulate the total distance
            total_distance += distance_km

        print(f"Total distance walked by user {user_id} in 2008: {round(total_distance, 2)} km")
        return total_distance

    # 8. Find the top 20 users who have gained the most altitude meters
    def find_altitude_gain_top_20_users(self):

        altitude_gain = self.db['Activity'].aggregate([
            {"$unwind": "$trackpoints"},
            {
                "$match": {
                    "trackpoints.altitude": {"$gt": -777}  # Filter out invalid altitudes (-777)
                }
            },
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",  # Group by user
                        "activity_id": "$_id"  # Group by activity
                    },
                    "trackpoints": {
                        "$push": "$trackpoints.altitude"  # Fetch all altitudes for an activity
                    }
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "total_gain": {
                        "$sum": {
                            "$map": {
                                "input": {"$range": [1, {"$size": "$trackpoints"}]},  # Iterate over trackpoints
                                "as": "idx",
                                "in": {
                                    "$cond": [
                                        {"$gt": [{"$arrayElemAt": ["$trackpoints", "$$idx"]}, {"$arrayElemAt": ["$trackpoints", {"$subtract": ["$$idx", 1]}]}]},
                                        {
                                            "$multiply": [
                                                {"$subtract": [
                                                    {"$arrayElemAt": ["$trackpoints", "$$idx"]},  # Current altitude
                                                    {"$arrayElemAt": ["$trackpoints", {"$subtract": ["$$idx", 1]}]}  # Previous altitude
                                                ]},
                                                0.3048  # Convert feet to meters
                                            ]
                                        },
                                        0
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$_id.user_id",  # Group by user to get total gain on all activities
                    "total_gain": {"$sum": "$total_gain"}
                }
            },
            {"$sort": {"total_gain": -1}},  # Sort by total altitude gain
            {"$limit": 20}  # Get top 20 users
        ])

        # Print the results
        altitude_gain = list(altitude_gain)
        if altitude_gain:
            rows = []
            for doc in altitude_gain:
                rows.append([doc["_id"], doc["total_gain"]])
            print(tabulate(rows, headers=['User ID', 'Total Altitude Gain (meters)'], tablefmt="fancy_grid"))
        else:
            print("No altitude gain data found.")

    # 9. Find all users who have invalid activities, and the number of invalid activities per user 
    def find_invalid_activities(self):
        activities = self.db['Activity'].aggregate([
            {"$unwind": "$trackpoints"},  # Unwind trackpoints
            {"$group": {
                "_id": "$_id",
                "user_id": {"$first": "$user_id"},
                "trackpoints": {"$push": "$trackpoints"}
            }},
        ])

        invalid_activities_per_user = {}

        # Iterate through the activities trackpoints
        for activity in activities:
            user_id = activity['user_id']
            trackpoints = activity['trackpoints']

            # Check if consecutive trackpoints have timestamps that deviate by at least 5 minutes
            invalid_activity_found = False
            for i in range(1, len(trackpoints)):
                time_difference = trackpoints[i]['date_time'] - trackpoints[i - 1]['date_time']
                if time_difference.total_seconds() >= 300:  # 300 seconds = 5 minutes
                    invalid_activity_found = True
                    break
            
            # Add it to the user's invalid activities if it is found
            if invalid_activity_found:
                if user_id not in invalid_activities_per_user:
                    invalid_activities_per_user[user_id] = 1
                else:
                    invalid_activities_per_user[user_id] += 1

        # Print results
        if invalid_activities_per_user:
            rows = [[user_id, invalid_activities_per_user[user_id]] for user_id in invalid_activities_per_user]
            print(tabulate(rows, headers=["User ID", "Invalid Activity Count"], tablefmt="fancy_grid"))
        else:
            print("No invalid activities found.")

    # 10. Find the users who have tracked an activity in the Forbidden City of Beijing
    def find_users_in_forbidden_city(self):

        users_in_forbidden_city = self.db['Activity'].distinct("user_id", {
                "trackpoints": {
                    "$elemMatch": {
                        "lat": {"$gte": 39.916000, "$lte": 39.916999},
                        "lon": {"$gte": 116.397000, "$lte": 116.397999}
                    }
                }
            })
        
        # Print the results
        if users_in_forbidden_city:
            rows = [[user] for user in users_in_forbidden_city]
            print(tabulate(rows, headers=["User ID"], tablefmt="fancy_grid"))
        else:
            print("No users found in the Forbidden City.")

    def find_most_used_transportation_per_user(self):
        most_used_mode = self.db['Activity'].aggregate([
            {"$match": {"transportation_mode": {"$ne": None}}},  # Filter out those that have no mode
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "transportation_mode": "$transportation_mode"
                    },
                    "count": {"$sum": 1}  # Count the number of each transportation mode per user
                }
            },
            {"$sort": {"_id.user_id": 1, "count": -1}},  # Sort by user_id
            {
                "$group": {
                    "_id": "$_id.user_id",  # Group by user_id
                    "most_used_transportation_mode": {"$first": "$_id.transportation_mode"}  # Get the most used mode
                }
            },
            {"$sort": {"_id": 1}}  # Sort by user_id
        ])

        # Print the results
        rows = [[mode['_id'], mode['most_used_transportation_mode']] for mode in most_used_mode]
        print(tabulate(rows, headers=["User ID", "Most Used Transportation Mode"], tablefmt="fancy_grid"))

    
if __name__ == "__main__":

    try:
        part2 = Part2()

        print("1. Count users, activities, and trackpoints:")
        part2.find_number_of()
        
        print("\n2. Average number of activities per user:")
        part2.find_avg_activities_per_user()
        
        print("\n3. Top 20 users with the highest number of activities:")
        part2.find_most_active_20_users()
        
        print("\n4. Find all users who have taken a taxi:")
        part2.find_taxi_users()
        
        print("\n5. Count of transportation modes:")
        part2.count_transportation_modes()
        
        print("\n6. a) Year with the most activities:")
        part2.find_year_with_most_activities()
        
        print("\n6. b) Year with the most recorded hours:")
        part2.find_year_with_most_hours()
        
        print("\n7. Total distance walked in 2008 by user with id=112:")
        part2.find_total_distance_walked_2008_user112()
        
        print("\n8. Top 20 users who have gained the most altitude:")
        part2.find_altitude_gain_top_20_users()
        
        print("\n9. Users with invalid activities and number of invalid activities:")
        part2.find_invalid_activities()
        
        print("\n10. Users who have tracked activity in the Forbidden City of Beijing:")
        part2.find_users_in_forbidden_city()
        
        print("\n11. Users with registered transportation modes and their most used mode:")
        part2.find_most_used_transportation_per_user()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if part2:
            part2.connection.close_connection()