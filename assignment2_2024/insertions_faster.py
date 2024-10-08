import os
from datetime import datetime
from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import time


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

                # Collect users data for bulk insertion
                print(f"Processing user {user_id}, labeled: {has_labels}")
                self.insert_user(user_id, has_labels)
                self.insert_activities_and_trackpoints(user_folder_path, user_id, has_labels)

            
    def insert_activities_and_trackpoints(self, file_path, user_id, has_labels):
        """
        Inserts activities and trackpoints in bulk for both labeled and non-labeled users
        """
        trajectory_folder_path = os.path.join(file_path, 'Trajectory')

        if has_labels:
            labels_file_path = os.path.join(file_path, 'labels.txt')
            label_hashmap = self.create_label_hashmap(labels_file_path)
        else:
            label_hashmap = None

        track_points_batch = set()

        parse_date_time = lambda row: datetime.strptime(f"{row[5]} {row[6]}", "%Y-%m-%d %H:%M:%S")

        for root, dirs, files in os.walk(trajectory_folder_path):
            files.sort()

            for plt_file in files:
                if plt_file.endswith('.plt'):
                    plt_file_path = os.path.join(trajectory_folder_path, plt_file)
                    
                    # Limit file size to avoid large trajectory files
                    with open(plt_file_path, 'r') as f:
                        total_lines = sum(1 for _ in f)
                    if total_lines - 6 > 2500:  # Skip files with more than 2500 trackpoints
                        print(f"Skipping large file {plt_file_path} with {total_lines - 6} trackpoints.")
                        continue

                    df_plt_file = pd.read_csv(plt_file_path, skiprows=6, header=None, sep=',')

                    # For non-labeled users, insert activities and trackpoints
                    if not has_labels:
                        start_date_time = parse_date_time(df_plt_file.iloc[0])
                        end_date_time = parse_date_time(df_plt_file.iloc[-1])
                        activity_id = self.insert_activity_data(user_id, None, start_date_time, end_date_time)

                        for row in df_plt_file.itertuples(index=False):
                            track_points_batch.add((activity_id, row[0], row[1], row[3], row[4], parse_date_time(row)))

                    # For labeled users, check start and end time and insert accordingly
                    else:
                        for (start_time, end_time), activity_data in label_hashmap.items():
                            # Check if this trajectory matches a labeled activity
                            for row in df_plt_file.itertuples(index=False):
                                date_time = parse_date_time(row)
                                if start_time <= date_time <= end_time:
                                    if activity_data['activity_id'] is None:
                                        # Insert the labeled activity and get the ID
                                        activity_data['activity_id'] = self.insert_activity_data(user_id, activity_data['transportation_mode'], start_time, end_time)
                                    # Add track points to batch
                                    track_points_batch.add((activity_data['activity_id'], row[0], row[1], row[3], row[4], date_time))

                    # If track_points_batch exceeds threshold, flush it to DB
                    if len(track_points_batch) >= 1500:
                        self.insert_track_points_batch(track_points_batch)
                        track_points_batch.clear()

            # Insert remaining trackpoints after processing all files
            if track_points_batch:
                self.insert_track_points_batch(track_points_batch)

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
        query = f"SELECT * FROM {table_name} LIMIT 20"
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

        #Show first 20 rows of Users, Activity, and TrackPoint tables
        print("\nFirst 20 rows from Users table:")
        program.show_20_rows("Users")

        print("\nFirst 20 rows from Activity table:")
        program.show_20_rows("Activity")

        print("\nFirst 20 rows from TrackPoint table:")
        program.show_20_rows("TrackPoint")
        
        
        
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