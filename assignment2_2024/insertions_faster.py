import os
from datetime import datetime
from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import time
from intervaltree import Interval, IntervalTree
import itertools


class InsertGeolifeDataset:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    # CREATE TABLES
    def create_user_table(self):
        query = f"""CREATE TABLE IF NOT EXISTS Users (
                   id INT NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = f"""CREATE TABLE IF NOT EXISTS Activity (
            id INT PRIMARY KEY AUTO_INCREMENT,
            user_id INT,
            transportation_mode VARCHAR(30),
            start_date_time DATETIME,
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES Users(id)
)
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_track_point_table(self):
        query = f"""CREATE TABLE IF NOT EXISTS TrackPoint (
            id INT PRIMARY KEY AUTO_INCREMENT,
            activity_id INT,
            lat DOUBLE,
            lon DOUBLE,
            altitude DOUBLE,
            date_days DOUBLE,
            date_time DATETIME,
            FOREIGN KEY (activity_id) REFERENCES Activity(id))
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    # INSERT USERS IN BATCH
    def insert_users_batch(self, users):
        """
        Inserts users in a batch to avoid individual insertions.
        """
        try:
            query = f"INSERT IGNORE INTO Users (id, has_labels) VALUES (%s, %s)"
            self.cursor.executemany(query, users)
            self.db_connection.commit()
            print(f"Inserted {len(users)} users into the database.")
        except Exception as e:
            print(f"Failed to insert users batch: {e}")
            
            
    def insert_user(self, user_id, has_labels):
        try:
            query = """INSERT INTO Users (id, has_labels) VALUES (%s, %s)"""
            values = (user_id, has_labels)
            self.cursor.execute(query, values)
            self.db_connection.commit()
            print(f"Inserted user {user_id} with labels: {has_labels}")
        except Exception as e:
            print(f"Failed to insert user {user_id}: {e}")

    # INSERT ACTIVITY DATA
    def insert_activity_data(self, user_id, transportation_mode, start_date_time, end_date_time):
        try:
            query = """INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) 
                       VALUES (%s, %s, %s, %s)"""
            values = (user_id, transportation_mode, start_date_time, end_date_time)
            self.cursor.execute(query, values)
            self.db_connection.commit()
            activity_id = self.cursor.lastrowid
            print(f"Inserted activity {activity_id} for user {user_id}, mode: {transportation_mode}, start: {start_date_time}, end: {end_date_time}")
            return activity_id  # Return the auto-generated activity_id
        except Exception as e:
            print(f"Failed to insert activity for user {user_id}: {e}")

    # INSERT TRACKPOINTS IN BATCH
    def insert_track_points_batch(self, track_points):
        try:
            query = f"""INSERT IGNORE INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) 
                        VALUES (%s, %s, %s, %s, %s, %s)"""
            self.cursor.executemany(query, list(track_points))
            self.db_connection.commit()
            print(f"Inserted {len(track_points)} trackpoints into the database.")
        except Exception as e:
            print(f"Failed to insert trackpoints: {e}")

    def read_labels(self, labels_file_path):
        """
        Reads the labels.txt file and returns a set of user IDs (as integers) with a label of True.
        """
        labeled_users = set()
        with open(labels_file_path, 'r') as file:
            for line in file:
                labeled_users.add(int(line.strip()))  # Assume each line contains a user ID (e.g., 45)
        return labeled_users

    def create_label_hashmap(self, labels_file_path):
        labels = {}

        with open(labels_file_path, 'r') as file:
            next(file)  # Skip header line
            for line in file:
                start_time_str, end_time_str, transportation_mode = line.strip().split('\t')
                start_time = datetime.strptime(start_time_str, "%Y/%m/%d %H:%M:%S")
                end_time = datetime.strptime(end_time_str, "%Y/%m/%d %H:%M:%S")
                # Hashmap keyed by start_time and end_time, value is transport mode and activity status
                labels[(start_time, end_time)] = {
                    'transportation_mode': transportation_mode,
                    'activity_id': None  # Will be filled in once activity is inserted
                }
        return labels


    def create_label_interval_tree(self, labels_file_path):
        labels_tree = IntervalTree()

        with open(labels_file_path, 'r') as file:
            next(file)  # Skip header line
            for line in file:
                start_time_str, end_time_str, transportation_mode = line.strip().split('\t')
                start_time = datetime.strptime(start_time_str, "%Y/%m/%d %H:%M:%S")
                end_time = datetime.strptime(end_time_str, "%Y/%m/%d %H:%M:%S")

                # Insert the interval into the interval tree with associated metadata
                labels_tree[start_time:end_time] = {
                    'transportation_mode': transportation_mode,
                    'activity_id': None  # Will be filled in once activity is inserted
                }

        return labels_tree

    def find_label_for_timestamp(labels_tree, timestamp):
        """Find labels for a given timestamp."""
        results = labels_tree[timestamp]
        if results:
            for result in results:
                print("Found:", result.data)
        else:
            print("No label found for timestamp:", timestamp)

    




    # TRAVERSE THE FOLDER STRUCTURE
    def traverse_folder(self, folder_path):
        """
        Traverses the folder structure, inserts users and associated activities, trackpoints in bulk
        """
        labeled_users_file = os.path.join(folder_path, "labeled_ids.txt")
        labeled_users = self.read_labels(labeled_users_file)


        for root, dirs, files in os.walk(os.path.join(folder_path, "Data")):
            dirs.sort()  # Sort directories to ensure correct order

            for user_folder in dirs:
                
                user_folder_path = os.path.join(root, user_folder)
                user_id = int(user_folder)  # Extract user ID from folder
                has_labels = 1 if user_id in labeled_users else 0
                trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
                labels_hashmap = None
                if has_labels:
                    labels_hashmap = self.create_label_hashmap(os.path.join(user_folder_path, 'labels.txt'))

                # Collect users data for bulk insertion
                print(f"Processing user {user_id}, labeled: {has_labels}")
                self.insert_user(user_id, has_labels)
                # if has_labels:
                #     print("User has labels, inserting activities and trackpoints.")
                #     self.labels_insert_activities_and_trackpoints(user_folder_path, user_id)
                # else:
                #     print("User has no labels, inserting activities and trackpoints.")
                
                self.no_label_insert_activities_and_trackpoints(labels_hashmap,trajectory_folder_path, user_id, has_labels)

    def no_label_insert_activities_and_trackpoints(self, labels_hashmap, trajectory_folder_path, user_id, label):
        for root, dirs, files in os.walk(trajectory_folder_path):
            # files.sort()
            
            trackpoints_to_insert = []
            BATCH_SIZE = 2000  # Batch size for inserting trackpoints

            for plt_file in files:
                if plt_file.endswith('.plt'):
                    plt_file_path = os.path.join(trajectory_folder_path, plt_file)
                    # Limit file size to avoid large trajectory files
                    with open(plt_file_path, 'r') as f:
                        total_lines = sum(1 for _ in f)
                    if total_lines - 6 > 2500:  # Skip files with more than 2500 trackpoints
                        print(f"Skipping large file {plt_file_path} with {total_lines - 6} trackpoints.")
                        continue
                    
                    with open(plt_file_path, 'r') as file:
                        lines = file.readlines()

                    # Extract the 7th line and the last line
                    seventh_line = lines[6].strip()  # 0-indexed, so 6 is the 7th line
                    last_line = lines[-1].strip()     # Last line

                    # Split the lines into elements
                    start_date = seventh_line.split(',')
                    end_date = last_line.split(',')

                    # Get the 5th and 6th elements from each line
                    start_datetime_str = f"{start_date[5]} {start_date[6]}"
                    end_datetime_str = f"{end_date[5]} {end_date[6]}"

                    # Format as datetime objects
                    start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M:%S")
                    end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M:%S")
                    transportation_mode=None
                    # Step 3: Insert activity into the database 
                    if not label:
                        activity_id = self.insert_activity_data(user_id, transportation_mode, start_datetime, end_datetime)
                    
                    if label:
                        # Loop through the labels hashmap
                        for (start_time, end_time), data in labels_hashmap.items():
                            if start_time == start_datetime and end_time == end_datetime:
                                transportation_mode = data['transportation_mode']
                                activity_id = self.insert_activity_data(user_id, transportation_mode, start_datetime, end_datetime)
                                      
                                break
                        if transportation_mode is None:
                            activity_id = self.insert_activity_data(user_id, transportation_mode, start_datetime, end_datetime)
                            
                        # Step 4: Insert all trackpoint in the plt file to the batch
                    with open(plt_file_path, 'r') as f:
                        for line in itertools.islice(f, 6, None):
                            parts = line.strip().split(',')
                            lat, lon, altitude, date_days = float(parts[0]), float(parts[1]), float(parts[3]), float(parts[4])
                            timestamp = datetime.strptime(f"{parts[5]} {parts[6]}", "%Y-%m-%d %H:%M:%S")
                            trackpoints_to_insert.append((activity_id, lat, lon, altitude, date_days, timestamp))
                    
                    
                    
                    #insert trackpoints in batch
                    if len(trackpoints_to_insert) >= BATCH_SIZE:
                        self.insert_track_points_batch(trackpoints_to_insert)
                        trackpoints_to_insert = []  
                        
                    #insert remaining trackpoints
            if trackpoints_to_insert:   
                self.insert_track_points_batch(trackpoints_to_insert)
                trackpoints_to_insert = []  
 
                    
                    
                    
                    
                    
                        
            #insert remaining trackpoints
            if trackpoints_to_insert:
                self.insert_track_points_batch(trackpoints_to_insert)
                trackpoints_to_insert = []





     








    def labels_insert_activities_and_trackpoints(self, file_path, user_id):
        """
        Inserts activities and trackpoints in bulk for both labeled and non-labeled users
        """
        trajectory_folder_path = os.path.join(file_path, 'Trajectory')

       
        labels_file_path = os.path.join(file_path, 'labels.txt')
        labels_tree = self.create_label_interval_tree(labels_file_path)
        
        BATCH_SIZE = 2000  # Batch size for inserting trackpoints
        track_points_batch = set()

        # parse_date_time = lambda row: datetime.strptime(f"{row[5]} {row[6]}", "%Y-%m-%d %H:%M:%S")

        for root, dirs, files in os.walk(trajectory_folder_path):
            # files.sort()
            
            plt_data = set()  # Store timestamps from all .plt files
            for plt_file in files:
                if plt_file.endswith('.plt'):
                    plt_file_path = os.path.join(trajectory_folder_path, plt_file)
                    
                    # Limit file size to avoid large trajectory files
                    with open(plt_file_path, 'r') as f:
                        total_lines = sum(1 for _ in f)
                    if total_lines - 6 > 2500:  # Skip files with more than 2500 trackpoints
                        print(f"Skipping large file {plt_file_path} with {total_lines - 6} trackpoints.")
                        continue

        
                    #traverse through all .plt files to collect timestamps
                    with open(plt_file_path, 'r') as plt_file:
                        for line in itertools.islice(plt_file, 6, None):
                            parts = line.strip().split(',')
                            date = parts[5]
                            time = parts[6]
                            timestamp = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S")
                            plt_data.add(timestamp)
                    
            # traverse the intervals in the interval label tree too see if start and end time exists in the .plt files  
            for interval in labels_tree:
                start_time = interval.begin
                end_time = interval.end
                transportation_mode = interval.data['transportation_mode']
                # Check if both start_time and end_time exist in plt_data
                start_exists = start_time in plt_data
                end_exists = end_time in plt_data

                if start_exists and end_exists:
                    # print(f"Start and end time found in plt data for interval: {start_time} - {end_time}")
                    # Step 3: Insert activity into the database
                    activity_id = self.insert_activity_data(user_id, transportation_mode, start_time, end_time)

                    # Step 4: Insert all trackpoints within the interval
                    
                    for plt_file in files:
                        if plt_file.endswith('.plt'):
                            plt_file_path = os.path.join(trajectory_folder_path, plt_file)                  
                            activity_trackpoints, end_found = self.get_trackpoints_for_an_activity(activity_id, plt_file_path, start_time, end_time)
                            
                            for trackpoint in activity_trackpoints:
                                track_points_batch.add(trackpoint)   
                            
                            if end_found:
                                break
                    if len(track_points_batch) >= BATCH_SIZE:
                        self.insert_track_points_batch(track_points_batch)
                        track_points_batch = set()
                        
            #insert remaining trackpoints   
            if not track_points_batch:
                self.insert_track_points_batch(track_points_batch)

                track_points_batch = set()

                    
                            



              
                    
                    
    def get_trackpoints_for_an_activity(self, activity_id, plt_file_path, start_time, end_time):
        trackpoints = []
        with open(plt_file_path, 'r') as file:
            for line in itertools.islice(file, 6, None):

                parts = line.strip().split(',')
                timestamp = datetime.strptime(f"{parts[5]} {parts[6]}", "%Y-%m-%d %H:%M:%S")
                if start_time <= timestamp <= end_time:
                    lat, lon, altitude, datedays = float(parts[0]), float(parts[1]), float(parts[3]), float(parts[4])
                    trackpoints.append((activity_id, lat, lon, altitude, datedays, timestamp))
                if timestamp == end_time:
                    return trackpoints, True
        return trackpoints, False

    def fetch_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.db_connection.commit()

    def show_20_rows(self, table_name):
        query = f"SELECT * FROM {table_name} LIMIT 10"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows
    
def main():
    program = None
    try:
        program = InsertGeolifeDataset()

        data_folder = "/Users/ceciliehuser/Documents/skole/NTNU/h24/store_distr_data/dataset"  # Update with actual path

        program.drop_table("TrackPoint")
        program.drop_table("Activity")
        program.drop_table("Users")
        
        program.create_user_table()
        program.create_activity_table()
        program.create_track_point_table()

        program.traverse_folder(data_folder)


        #Show first 10 rows of Users, Activity, and TrackPoint tables
        print("\nFirst 10 rows from Users table:")
        program.show_20_rows("Users")

        print("\nFirst 10 rows from Activity table:")
        program.show_20_rows("Activity")

        print("\nFirst 10 rows from TrackPoint table:")
        program.show_20_rows("TrackPoint")
        
        
        #Get activity for user 10
        print("\n get activity from user 10")
        program.cursor.execute("SELECT * FROM Activity WHERE user_id = 10")
        rows = program.cursor.fetchall()
        print(tabulate(rows, headers=program.cursor.column_names))
        
        
        

    except Exception as e:
        print(f"ERROR: Failed to use database: {e}")
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()