from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine
from insertions_faster import InsertGeolifeDataset


class Part2:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    # 1. How many users, activities and trackpoints are there in the dataset
    def find_number_of(self):
        # Count users
        self.cursor.execute("SELECT COUNT(*) FROM Users")
        users_count = self.cursor.fetchone()[0]
        print(f"Total number of users: {users_count}")

        # Count activities
        self.cursor.execute("SELECT COUNT(*) FROM Activity")
        activities_count = self.cursor.fetchone()[0]
        print(f"Total number of activities: {activities_count}")

        # Count trackpoints
        self.cursor.execute("SELECT COUNT(*) FROM TrackPoint")
        trackpoints_count = self.cursor.fetchone()[0]
        print(f"Total number of trackpoints: {trackpoints_count}")
        
        return users_count, activities_count, trackpoints_count

    # 2. Find the average number of activities per user
    def find_avg_activities_per_user(self):
        query = """
            SELECT AVG(number_of_activities) FROM 
            (SELECT COUNT(*) as number_of_activities FROM Activity GROUP BY user_id) as avg_activity;
        """
        self.cursor.execute(query)
        avg_activities = self.cursor.fetchone()[0]
        print(f"The average number of activities per user is: {round(avg_activities,2)}")
        return avg_activities

    # 3. Find the top 20 users with the highest number of activities
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

    # 4. Find all users who have taken a taxi
    def find_taxi_users(self):
        query = """SELECT DISTINCT user_id 
        FROM Activity 
        WHERE transportation_mode = 'taxi';
        """
        self.cursor.execute(query)
        taxi_users = self.cursor.fetchall()
        print(tabulate(taxi_users, headers=["User ID"]))
        return taxi_users

    # 5. Find all types of transportation modes and count how many activities that are
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

    # 6. a) Find the year with the most activities.
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
    
    # 6. b) Is this also the year with most recorded hours?
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

        # Compare to the year with the most activities
        most_activities_year = self.find_year_with_most_activities()
        if most_activities_year[0] == result[0]:
            print(f"Yes, the year {most_activities_year[0]} has the most activities and also the most recorded hours.")
        else:
            print(f"No, the year with the most activities ({most_activities_year[0]}) is different from the year with the most recorded hours ({result[0]}).")
        return result

    # 7. Find the total distance (in km) walked in 2008, by user with id=112
    def find_total_distance_walked_2008_user112(self):
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
        for i in range(1, len(trackpoints)):
            # Fetch the latitude and longitude of the trackpoints
            previous_point = (trackpoints[i-1][0], trackpoints[i-1][1])
            current_point = (trackpoints[i][0], trackpoints[i][1])
            # Calculate the distance between the two points using the haversine function
            total_distance += haversine(previous_point, current_point)

        
        print(f"Distance walked by user 112 in 2008: {round(total_distance,2)} km")
        return total_distance


    # Denne må nok testes litt! Mulig det er mer komplisert enn det må være
    # Går veldig tregt
    # 8. Find the top 20 users who have gained the most altitude meters
    # def find_altitude_gain_top_20_users(self):
    #     query = """
    #         SELECT a.user_id, SUM(tp2.altitude - tp1.altitude) AS altitude_gain
    #         FROM TrackPoint tp1
    #         JOIN TrackPoint tp2 ON tp1.activity_id = tp2.activity_id
    #                             AND tp2.id = tp1.id + 1
    #         JOIN Activity a ON tp1.activity_id = a.id
    #         WHERE tp2.altitude != -777
    #         AND tp1.altitude != -777
    #         AND tp2.altitude > tp1.altitude
    #         GROUP BY a.user_id
    #         ORDER BY altitude_gain DESC
    #         LIMIT 20;
    #     """
    #     self.cursor.execute(query)
    #     top_users = self.cursor.fetchall()
    #     print(tabulate(top_users, headers=["User ID", "Total altitude gained"]))
    #     return top_users
    



    def find_altitude_gain_top_20_users(self):
        # Fetch altitude differences in feet, not converting in SQL
        query = """
            SELECT a.user_id, 
                SUM(tp2.altitude - tp1.altitude) AS altitude_gain_feet
            FROM TrackPoint tp1
            JOIN TrackPoint tp2 ON tp1.activity_id = tp2.activity_id
                                AND tp2.id = tp1.id + 1
            JOIN Activity a ON tp1.activity_id = a.id
            WHERE tp2.altitude != -777
            AND tp1.altitude != -777
            AND tp2.altitude > tp1.altitude
            GROUP BY a.user_id
            ORDER BY altitude_gain_feet DESC
            LIMIT 20;
        """

        self.cursor.execute(query)
        top_users_feet = self.cursor.fetchall()

        # Convert altitude gain from feet to meters
        top_users_meters = [(user_id, int(round(float(altitude_gain) * 0.3048))) for user_id, altitude_gain in top_users_feet]
        print(tabulate(top_users_meters, headers=["User ID", "Total Altitude Gained (meters)"]))
        return top_users_meters

    

    # Går veldig tregt
    # 9. Find all users who have invalid activities, and the number of invalid activities per user 
    def find_invalid_activities(self):
        query = """
            SELECT a.user_id, COUNT(a.id) AS number_of_invalid_activities
            FROM Activity a
            JOIN TrackPoint trackpoint1 ON a.id = trackpoint1.activity_id
            JOIN TrackPoint trackpoint2 ON a.id = trackpoint2.activity_id 
                AND trackpoint2.id = trackpoint1.id + 1
            WHERE TIMESTAMPDIFF(MINUTE, trackpoint1.date_time, trackpoint2.date_time) >= 5
            GROUP BY a.user_id;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=["User ID", "Number of invalid activities"]))
        return rows

    # 10. Find the users who have tracked an activity in the Forbidden City of Beijing
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


    # 11. Find all users who have registered transportation_mode and their most used transportation_mode
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
        
        # Finding most used mode per user
        most_used_modes = {}
        for row in users_transportation_mode:
            user_id = row[0]
            if user_id not in most_used_modes:
                most_used_modes[user_id] = row[1]

        # Display the results
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
