from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine
from insertions_faster import InsertGeolifeDataset
import numpy as np


class Part2:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    #1. How many users, activities and trackpoints are there in the dataset
    def find_number_of(self):
        # Counting users
        self.cursor.execute("SELECT COUNT(*) FROM User")
        users_count = self.cursor.fetchone()[0]
        print(f"Total number of users: {users_count}")

        # Counting activities
        self.cursor.execute("SELECT COUNT(*) FROM Activity")
        activities_count = self.cursor.fetchone()[0]
        print(f"Total number of activities: {activities_count}")

        # Count trackpoints
        self.cursor.execute("SELECT COUNT(*) FROM TrackPoint")
        trackpoints_count = self.cursor.fetchone()[0]
        print(f"Total number of trackpoints: {trackpoints_count}")
        
        return users_count, activities_count, trackpoints_count

    #2. Find the average number of activities per user, including users with zero activities
    def find_avg_activities_per_user(self):
        query = """
            SELECT AVG(activity_count) FROM (
                SELECT u.id, 
                    CASE 
                        WHEN COUNT(a.id) IS NULL THEN 0
                        ELSE COUNT(a.id)
                    END AS activity_count
                FROM User u
                LEFT JOIN Activity a ON u.id = a.user_id
                GROUP BY u.id
            ) AS activity_per_user;
        """
        self.cursor.execute(query)
        avg_activities = self.cursor.fetchone()[0]
        print(f"The average number of activities per user is: {round(avg_activities, 2)}")
        return avg_activities


    #3. Find the top 20 users with the highest number of activities
    def find_most_active_20_users(self):
        query = """
            SELECT user_id, COUNT(*) as number_of_activities 
            FROM Activity 
            GROUP BY user_id 
            ORDER BY number_of_activities DESC 
            LIMIT 20;
        """
        self.cursor.execute(query)
        top_users = self.cursor.fetchall()
        print(tabulate(top_users, headers=["User ID", "Activity count"]))
        return top_users

    #4. Find all users who have taken a taxi
    def find_taxi_users(self):
        query = """SELECT DISTINCT user_id 
        FROM Activity 
        WHERE transportation_mode = 'taxi';
        """
        self.cursor.execute(query)
        taxi_users = self.cursor.fetchall()
        print(tabulate(taxi_users, headers=["User ID"]))
        return taxi_users

    #5. Find all types of transportation modes and count how many activities that are
    # tagged with these transportation mode labels. Do not count the rows where the mode is null
    def count_transportation_modes(self):
        query = """
            SELECT transportation_mode, COUNT(*) 
            FROM Activity 
            WHERE transportation_mode IS NOT NULL 
            GROUP BY transportation_mode;
        """
        self.cursor.execute(query)
        transportation_mode = self.cursor.fetchall()
        print(tabulate(transportation_mode, headers=["Transportation mode", "Count"]))
        return transportation_mode

    #6. a) Find the year with the most activities.
    def find_year_with_most_activities(self):
        query = """
            SELECT YEAR(start_date_time) as year, COUNT(*) as number_of_activities
            FROM Activity
            GROUP BY year
            ORDER BY number_of_activities DESC
            LIMIT 1;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print(f"Year with most activities: {result[0]} with {result[1]} activities.")
        return result
    
    #6. b) Is this also the year with most recorded hours?
    def find_year_with_most_hours(self):
        query = """
            SELECT YEAR(start_date_time) as year, 
                SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as total_hours
            FROM Activity
            GROUP BY year
            ORDER BY total_hours DESC
            LIMIT 1;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print(f"Year with most recorded hours: {result[0]} with {result[1]} hours.")

        #Comparing to the year with the most activities
        most_activities_year = self.find_year_with_most_activities()
        if most_activities_year[0] == result[0]:
            print(f"Yes, the year {most_activities_year[0]} has the most activities and also the most recorded hours.")
        else:
            print(f"No, the year with the most activities ({most_activities_year[0]}) is different from the year with the most recorded hours ({result[0]}).")
        return result
    
    #7. Find the total distance (in km) walked in 2008, by user with id=112
    def find_total_distance_walked_2008_user112(self):
        """
        Finds the total distance (in km) walked in 2008 by user with id=112 using the haversine formula.
        
        Source for haversine: https://stackoverflow.com/questions/29545704/fast-haversine-approximation-python-pandas/29546836#29546836
        """
        # Query to select latitude and longitude for walks in 2008
        query = """
            SELECT lat, lon
            FROM TrackPoint tp
            JOIN Activity a ON tp.activity_id = a.id
            WHERE a.user_id = 112 AND a.transportation_mode = 'walk'
            AND YEAR(a.start_date_time) = 2008
            ORDER BY tp.id;
        """
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()

        total_distance = 0.0

        #Looping through trackpoints and calculating the total distance in kilometers
        for i in range(1, len(trackpoints)):
            previous_point = trackpoints[i-1]
            current_point = trackpoints[i]

            #Extract latitudes and longitudes for both points
            lat1, lon1 = previous_point
            lat2, lon2 = current_point

            #Converting degrees to radians
            lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

            #Haversine formula components
            dlon = lon2 - lon1
            dlat = lat2 - lat1

            a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
            c = 2 * np.arcsin(np.sqrt(a))

            #Radius of Earth = 6378.137 km
            distance_km = 6378.137 * c

            #Accumulate the total distance
            total_distance += distance_km

        print(f"Total distance walked by user 112 in 2008: {round(total_distance, 2)} km")
        return total_distance


    #8. Find the top 20 users who have gained the most altitude meters
    def find_altitude_gain_top_20_users(self):
        # Fetching altitude differences directly in meters, excluding invalid (-777) and negative altitude values below -413
        query = """
            SELECT a.user_id, 
                SUM((tp2.altitude - tp1.altitude) * 0.3048) AS altitude_gain_meters
            FROM TrackPoint tp1
            JOIN TrackPoint tp2 ON tp1.activity_id = tp2.activity_id
                                AND tp2.id = tp1.id + 1
            JOIN Activity a ON tp1.activity_id = a.id
            WHERE tp2.altitude != -777
            AND tp1.altitude != -777
            AND tp2.altitude > tp1.altitude
            AND tp1.altitude >= -413
            AND tp2.altitude >= -413
            GROUP BY a.user_id
            ORDER BY altitude_gain_meters DESC
            LIMIT 20;
        """

        self.cursor.execute(query)
        top_users_meters = self.cursor.fetchall()
        print(tabulate(top_users_meters, headers=["User ID", "Total Altitude Gained (meters)"]))
        return top_users_meters

    
        # 9. Find all users who have invalid activities, and the number of invalid activities per user 

    
    def find_invalid_activities(self):
        query = """
            SELECT a.user_id, COUNT(DISTINCT a.id) AS number_of_invalid_activities
            FROM Activity a
            JOIN TrackPoint tp1 ON a.id = tp1.activity_id
            JOIN TrackPoint tp2 ON a.id = tp2.activity_id 
                AND tp2.id = tp1.id + 1
            WHERE TIMESTAMPDIFF(MINUTE, tp1.date_time, tp2.date_time) >= 5
            GROUP BY a.user_id;
        """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # Format rows for 4 columns per row, with vertical lines between ID-Invalid pairs
        compact_rows = []
        for i in range(0, len(rows), 6):
            row = []
            for j in range(6):
                if i + j < len(rows):
                    # Left-align ID, right-align Invalid Activities
                    row.append(f"{rows[i + j][0]:<6} {rows[i + j][1]:>7}")
                else:
                    row.append(" " * 12)  # Fill with spaces if fewer than 4 users
            compact_rows.append(row)

        # Create custom headers
        headers = ["ID      Count", "ID      Count", "ID      Count", "ID      Count", "ID      Count",  "ID      Count"]

        # Create table with vertical separators only between ID-Invalid pairs
        print(tabulate(compact_rows, headers=headers, tablefmt="grid"))

        return rows

    #10. Find the users who have tracked an activity in the Forbidden City of Beijing
    def find_users_in_forbidden_city(self):
        query = """
            SELECT DISTINCT a.user_id
            FROM TrackPoint tp
            JOIN Activity a ON tp.activity_id = a.id
            WHERE tp.lat BETWEEN 39.9160000 AND 39.9169999
            AND tp.lon BETWEEN 116.3970000 AND 116.3979999;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=["User ID"]))
        return rows


    #11. Find all users who have registered transportation_mode and their most used transportation_mode
    def find_most_used_transportation_per_user(self):
        query = """
            SELECT user_id, transportation_mode, COUNT(*) as mode_count
            FROM Activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
            ORDER BY user_id, mode_count DESC;
        """
        self.cursor.execute(query)
        users_transportation_mode = self.cursor.fetchall()
        
        #Finding most used mode per user
        most_used_modes = {}
        for row in users_transportation_mode:
            user_id = row[0]
            if user_id not in most_used_modes:
                most_used_modes[user_id] = row[1]

        result = [(user_id, mode) for user_id, mode in most_used_modes.items()]
        print(tabulate(result, headers=["User ID", "Most used transportation mode"]))
        return result

    
    def close_connection(self):
        self.connection.close_connection()

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
