from pprint import pprint
from DbConnector import DbConnector
import datetime
import os
import itertools
from tabulate import tabulate
import numpy as np

class Part2:
    """
    Class for analyzing the Geolife dataset stored in MongoDB.
    """

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

        print("Total counts:")
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

        print("Top 20 users by activity count:")
        for user in top_users:
            print(f"User ID: {user['_id']}, Activity Count: {user['number_of_activities']}")

    # 4. Find all users who have taken a taxi
    def find_taxi_users(self):
        taxi_users = self.db['Activity'].distinct("user_id", {"transportation_mode": "taxi"})
        print("Users who took a taxi:")
        print(tabulate([[user] for user in taxi_users], headers=["User ID"]))

    #5. Find all types of transportation modes and count how many activities that are
    # tagged with these transportation mode labels. Do not count the rows where the mode is null
    def count_transportation_modes(self):
        mode_counts = self.db['Activity'].aggregate([
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
        ])
        
        print("Transportation modes and their activity counts:")
        for mode in mode_counts:
            pprint(mode)

    #6. a) Find the year with the most activities.
    def find_year_with_most_activities(self):
        """
        Find the year with the most activities.
        """
        year_activities = self.db['Activity'].aggregate([
            {"$group": {"_id": {"$year": "$start_time"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ])

        year = list(year_activities)
        if year:
            print(f"Year with the most activities: {year[0]['_id']} with {year[0]['count']} activities")
        else:
            print("No activities found.")
        return year[0]['_id']
    
    #6. b) Is this also the year with most recorded hours?
    def find_year_with_most_hours(self):
        """
        Find the year with the most recorded hours.
        """
        year_hours = self.db['Activity'].aggregate([
            {
                "$group": {
                    "_id": {"$year": "$start_time"},  # Use $year to extract the year from start_time
                    "total_hours": {
                        "$sum": {
                            "$divide": [
                                {"$subtract": ["$end_time", "$start_time"]},  # Calculate duration in milliseconds
                                3600000  # Convert milliseconds to hours
                            ]
                        }
                    }
                }
            },
            {"$sort": {"total_hours": -1}},  # Sort by total hours in descending order
            {"$limit": 1}  # Limit to the top result
        ])

        year_with_most_hours = list(year_hours)

        if year_with_most_hours:
            print(f"Year with the most recorded hours: {year_with_most_hours[0]['_id']} with {year_with_most_hours[0]['total_hours']:.2f} hours.")
        else:
            print("No recorded hours found.")
            return None, 0  # Return None if no hours found
        
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
        # Define user ID and date range
        user_id = 112

        # Aggregate to get trackpoints for user 112 in 2008 with transportation_mode as "walk"
        pipeline = [
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
        ]

        # Execute aggregation pipeline
        trackpoints = self.db['Activity'].aggregate(pipeline)

        # Convert trackpoints to a list for easy access
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

            # Radius of Earth = 6371.0 km
            distance_km = 6371.0 * c  # Updated to 6371.0 for more accuracy

            # Accumulate the total distance
            total_distance += distance_km

        print(f"Total distance walked by user {user_id} in 2008: {round(total_distance, 2)} km")
        return total_distance

    #8. Find the top 20 users who have gained the most altitude meters
    def find_altitude_gain_top_20_users(self):
        """
        Find the top 20 users who have gained the most altitude in meters.
        """
        altitude_gain = self.db['Activity'].aggregate([
            {"$unwind": "$trackpoints"},  # Unwind the trackpoints array
            {
                "$match": {
                    "trackpoints.altitude": {"$gte": -413}  # Filter out invalid altitudes
                }
            },
            {
                "$group": {
                    "_id": "$user_id",  # Group by user_id
                    "altitudes": {"$push": "$trackpoints.altitude"}  # Collect all altitudes
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "total_gain": {
                        "$reduce": {
                            "input": {
                                "$map": {
                                    "input": {"$slice": ["$altitudes", 1, -1]},  # Ignore the first element
                                    "as": "altitude",
                                    "in": {
                                        "$cond": [
                                            {
                                                "$gt": [
                                                    {"$arrayElemAt": ["$altitudes", "$$altitude"]},
                                                    {"$arrayElemAt": ["$altitudes", {"$subtract": ["$$altitude", 1]}]}
                                                ]
                                            },
                                            {"$subtract": [
                                                {"$arrayElemAt": ["$altitudes", "$$altitude"]},
                                                {"$arrayElemAt": ["$altitudes", {"$subtract": ["$$altitude", 1]}]}
                                            ]},
                                            0  # No gain
                                        ]
                                    }
                                }
                            },
                            "initialValue": 0,
                            "in": {"$add": ["$$value", "$$this"]}
                        }
                    }
                }
            },
            {"$sort": {"total_gain": -1}},  # Sort by total gain
            {"$limit": 20}  # Limit to top 20 users
        ])

        print("Top 20 users by total altitude gained:")
        for user in altitude_gain:
            print(f"User ID: {user['_id']}, Total Altitude Gain: {user['total_gain']} meters")


    def invalid_activities(self):
        """
        Find all users with invalid activities and the number of invalid activities per user.
        """
        invalid_activities = self.db['Activity'].aggregate([
            {"$unwind": "$trackpoints"},
            {
                "$group": {
                    "_id": "$user_id",
                    "invalid_count": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$gt": [
                                        {"$subtract": [
                                            {"$arrayElemAt": ["$trackpoints.date_time", 1]},
                                            {"$arrayElemAt": ["$trackpoints.date_time", 0]}
                                        ]},
                                        300  # 5 minutes in seconds
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    }
                }
            },
            {"$match": {"invalid_count": {"$gt": 0}}}
        ])
        
        print("Users with invalid activities and their invalid activity count:")
        for user in invalid_activities:
            pprint(user)

    def activities_in_forbidden_city(self):
        """
        Find the users who have tracked an activity in the Forbidden City of Beijing.
        """
        forbidden_city_coords = {"lat": 39.916, "lon": 116.397}
        users_in_forbidden_city = self.db['Activity'].distinct("user_id", {
            "trackpoints": {
                "$elemMatch": {
                    "lat": {"$eq": forbidden_city_coords["lat"]},
                    "lon": {"$eq": forbidden_city_coords["lon"]}
                }
            }
        })
        
        print("Users who have tracked an activity in the Forbidden City:")
        pprint(users_in_forbidden_city)

    def most_used_transportation_mode(self):
        """
        Find all users who have registered transportation_mode and their most used mode.
        """
        most_used_mode = self.db['Activity'].aggregate([
            {"$match": {"transportation_mode": {"$ne": None}}},
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "transportation_mode": "$transportation_mode"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id.user_id": 1, "count": -1}},
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "most_used_transportation_mode": {"$first": "$_id.transportation_mode"}
                }
            }
        ])
        
        print("Users and their most used transportation mode:")
        for mode in most_used_mode:
            pprint(mode)

    def get_activities_for_users(self, user_ids):
        """
        Fetch and print activities for specified users.
        
        Args:
            user_ids (list): List of user IDs to fetch activities for.
        """
        # Get first 10 activities for user 0
        activities_user_0 = self.db['Activity'].find({"user_id": user_ids[0]}).limit(10)
        
        print(f"First 10 activities for user {user_ids[0]}:")
        for activity in activities_user_0:
            pprint(activity)

        # Get first 10 labeled activities for user 10 (non-null transportation_mode)
        activities_user_10 = self.db['Activity'].find({
            "user_id": user_ids[1],
            "transportation_mode": {"$ne": None}  # Non-null transportation_mode
        }).limit(10)
        
        print(f"\nFirst 10 labeled activities for user {user_ids[1]}:")
        for activity in activities_user_10:
            pprint(activity)

    def show_user_activities_with_trackpoints(self, user_id):
        """
        Fetch and print user activities along with the first three trackpoints for each activity.
        
        Args:
            user_id (int): The ID of the user to fetch activities for.
        """
        activities = self.db['Activity'].find({"user_id": user_id}).limit(3)
        
        print(f"Activities for user {user_id}:")
        for activity in activities:
            print(f"\nActivity Start: {activity['start_time']}, End: {activity['end_time']}")
            print(f"Transportation Mode: {activity['transportation_mode']}")
            
            trackpoints = activity['trackpoints'][:3]  # Get the first three trackpoints
            for idx, trackpoint in enumerate(trackpoints):
                print(f"Trackpoint {idx + 1}: Latitude: {trackpoint['lat']}, Longitude: {trackpoint['lon']}, Altitude: {trackpoint['altitude']}, Timestamp: {trackpoint['date_time']}")

    def show_user_labeled_activities_with_trackpoints(self, user_id):
        """
        Fetch and print user labeled activities along with the first three trackpoints for each activity.
        
        Args:
            user_id (int): The ID of the user to fetch activities for.
        """
        # Fetch the first three labeled activities for the user (where transportation_mode is not null)
        activities = self.db['Activity'].find({
            "user_id": user_id,
            "transportation_mode": {"$ne": None}  # Non-null transportation_mode
        }).limit(3)
        
        print(f"First three labeled activities for user {user_id}:")
        for activity in activities:
            print(f"\nActivity Start: {activity['start_time']}, End: {activity['end_time']}")
            print(f"Transportation Mode: {activity['transportation_mode']}")
            
            trackpoints = activity['trackpoints'][:3]  # Get the first three trackpoints
            for idx, trackpoint in enumerate(trackpoints):
                print(f"Trackpoint {idx + 1}: Latitude: {trackpoint['lat']}, Longitude: {trackpoint['lon']}, Altitude: {trackpoint['altitude']}, Timestamp: {trackpoint['date_time']}")
    
    
    def check_user_data(self, user_id):
        """
        Fetch and print all activities and their associated trackpoints for a specified user.
        
        Args:
            user_id (int): The ID of the user to fetch data for.
        """
        # Fetch activities for the user
        activities = list(self.db['Activity'].find({"user_id": user_id}))

        if len(activities) == 0:
            print(f"No activities found for user {user_id}.")
            return

        print(f"Activities for user {user_id}:")
        for activity in activities:
            print(f"Activity Start: {activity['start_time']}, End: {activity['end_time']}, Transportation Mode: {activity['transportation_mode']}")
            
            # Check trackpoints
            if 'trackpoints' in activity and activity['trackpoints']:
                print("Trackpoints:")
                for idx, trackpoint in enumerate(activity['trackpoints']):
                    print(f"  Trackpoint {idx + 1}: Lat: {trackpoint['lat']}, Lon: {trackpoint['lon']}, Altitude: {trackpoint['altitude']}, Timestamp: {trackpoint['date_time']}")
            else:
                print("  No trackpoints found for this activity.")

    def count_activities_by_transportation_mode_user112(self):
        """
        Fetch and print the number of activities for each transportation mode for user 112.
        """
        # Aggregate to count activities by transportation mode for user 112
        transport_mode_counts = self.db['Activity'].aggregate([
            {"$match": {"user_id": 112, "transportation_mode": {"$ne": None}}},  # Only consider activities with non-null transportation mode
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}  # Sort by count in descending order
        ])

        # Convert cursor to a list for easy access
        transport_mode_counts_list = list(transport_mode_counts)

        if len(transport_mode_counts_list) == 0:
            print("No activities found for user 112 with non-null transportation mode.")
            return

        print(f"Activity counts by transportation mode for user 112:")
        for mode_count in transport_mode_counts_list:
            print(f"Transportation Mode: {mode_count['_id']}, Count: {mode_count['count']}")




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