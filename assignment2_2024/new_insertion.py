import os
from datetime import datetime
from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import random



class InsertGeolifeDataset:
    
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    
    #CREATE TABLES
    def create_user_table(self, table_name):
        # Create a table with 'id' as the user number and 'label' as True/False
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                   id INT NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        self.cursor.execute(query)
        self.db_connection.commit()
        
    def create_activity_table(self, table_name):
        # Create a table with 'id' as the user number and 'label' as True/False
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            id INT PRIMARY KEY,
            user_id INT,
            transportation_mode VARCHAR(30),
            start_date_time DATETIME,
            end_date_time DATETIME)
                """
        self.cursor.execute(query)
        self.db_connection.commit()
        
    def create_track_point_table(self, table_name):
        # Create a table with 'id' as the user number and 'label' as True/False
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            activity_id INT,
            lat DOUBLE,
            lon DOUBLE,
            altitude DOUBLE,
            date_days DOUBLE,
            date_time DATETIME)
                """
        self.cursor.execute(query)
        self.db_connection.commit()


    #INSERT DATA
    def insert_user_data(self, user_id, has_labels):
        query = f"INSERT IGNORE INTO Users (id, has_labels) VALUES ('{user_id}', {has_labels})"
        self.cursor.execute(query)
        self.db_connection.commit()
    
    def insert_activity_data(self, id, user_id, transportation_mode, start_date_time, end_date_time):
        query = f"""INSERT IGNORE INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) 
        VALUES (%s, %s, %s, %s, %s)"""
        
        values = (id, user_id, transportation_mode, start_date_time, end_date_time)

        
        self.cursor.execute(query, values)
        self.db_connection.commit()
    
    def insert_track_points_batch(self, track_points):
        query = f"""INSERT IGNORE INTO TrackPoint ( activity_id, lat, lon, altitude, date_days, date_time) 
        VALUES ( %s, %s, %s, %s, %s, %s)"""
        self.cursor.executemany(query, list(track_points))
        self.db_connection.commit()
        
    
        
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

                # Convert string timestamps to datetime objects for comparison
                # Convert to datetime objects using the original format
                start_time = datetime.strptime(start_time_str, "%Y/%m/%d %H:%M:%S")
                end_time = datetime.strptime(end_time_str, "%Y/%m/%d %H:%M:%S")

                # Format the datetime objects to the new format "YYYY-MM-DD HH:MM:SS"
                start_time_formatted = start_time.strftime("%Y-%m-%d %H:%M:%S")
                end_time_formatted = end_time.strftime("%Y-%m-%d %H:%M:%S")

                # Generate a unique activity ID using current timestamp and random number
                current_time_str = datetime.now().strftime("%Y%m%d%H%M%S")
                random_number = random.randint(1000, 9999)  # Append random number to avoid collisions
                activity_id = f"{current_time_str}{random_number}"

                # Create a dictionary entry for this label
                labels[activity_id] = {
                    'start_time': start_time_formatted,
                    'end_time': end_time_formatted,
                    'transportation_mode': transportation_mode,
                    'start_found': False,  # Bit for tracking if start_time is found
                    'end_found': False     # Bit for tracking if end_time is found
                }

        return labels
        
    #TRAVERSE THE FOLDER STRUCTURE
    def traverse_folder(self, folder_path):
        # Traverse through all user folders within the Data folder
        labeled_users_file = os.path.join(folder_path, "labeled_ids.txt")
        labeled_users = self.read_labels(labeled_users_file)
        
        for root, dirs, files in os.walk(folder_path):
            # Sort the directories to ensure proper order of user folders (000, 001, ..., 180)
            dirs.sort()
            path_to_users = os.path.join(folder_path, "Data")
            print(f"Processing user folders in: {path_to_users}")

            if root == path_to_users:
                # Only process user folders (root should be Data folder and dirs contain the user folders)
                for user_folder in dirs:
                    user_folder_path = os.path.join(root, user_folder)
                    print(f"\nProcessing user folder: {user_folder_path}")

                    #---INSERTING USERS---#
                    user_id = int(user_folder)  # Extract the user ID from the folder name
                    label = user_id in labeled_users  # Check if the user ID is labeled
                    self.insert_user_data(user_id, label)  # Insert user if not already in the database


                    #---INSERTING ACTIVITIES AND TRACKPOINTS---#
                    self.insert_activities_and_trackpoints(user_folder_path, user_id, label)
                # Since we are handling directories manually in this loop, we stop os.walk from processing them automatically
                break  # Prevent further traversal by os.walk

            
            
        
        
    
                    
                    
                        
                    

    def fetch_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows
    
    def show_20_rows(self, table_name):
        query = f"SELECT * FROM {table_name} LIMIT 20"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print(f"Dropping table {table_name}...")
        query = f"DROP TABLE {table_name}"
        self.cursor.execute(query)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def pretty_print_label_hashmap(self, label_hashmap):
        for activity_id, activity_data in label_hashmap.items():
            print(f"Activity ID: {activity_id}")
            print(f"Start time: {activity_data['start_time']}")
            print(f"End time: {activity_data['end_time']}")
            print(f"Transportation mode: {activity_data['transportation_mode']}")
            print(f"Start found: {activity_data['start_found']}")
            print(f"End found: {activity_data['end_found']}")
            print()
            
    def insert_activities_and_trackpoints(self, file_path, user_id, label):
        trajectory_folder_path = os.path.join(file_path, 'Trajectory')
        if label:
            labels_file_path = os.path.join(file_path, 'labels.txt')
            label_hashmap = self.create_label_hashmap(labels_file_path)
        # self.pretty_print_label_hashmap(label_hashmap)
        print(f"Processing user trajectory folder path: {trajectory_folder_path}")
        for trajectory_root, trajectory_dirs, trajectory_files in os.walk(trajectory_folder_path):
            # Sort trajectory files to ensure order (if order matters)
            trajectory_files.sort()
            for plt_file in trajectory_files:
                if plt_file.endswith('.plt'):

                    plt_file_path = os.path.join(trajectory_folder_path, plt_file)
                    plt_file_id = int(plt_file.split('.')[0])  # Extract the activity ID from the file 
                    # Check if the .plt file is too large
                    # Check the line count, excluding the header (skiprows=6)
                    with open(plt_file_path, 'r') as f:
                        total_lines = sum(1 for _ in f)
                    # Skip the file if it exceeds 2500 data lines (total_lines includes header)
                    if total_lines - 6 > 2500:
                        continue  # Skip this file and move to the next
                    
                    df_plt_file = pd.read_csv(plt_file_path, skiprows=6, header=None, sep=',', engine='python')

                    #---INSERTING activity for non labeled users---#
                    if not label:

                        # Extracting the start date and time (from the first row)
                        start_date = df_plt_file.iloc[0, 5]  # 6th column is the date (index 5)
                        start_time = df_plt_file.iloc[0, 6]  # 7th column is the time (inde
                        # Extracting the end date and time (from the last row)
                        end_date = df_plt_file.iloc[-1, 5]  # Last row's 6th column (index 5) is the date
                        end_time = df_plt_file.iloc[-1, 6]  # Last row's 7th column (index 6) is the t
                        # Step 3: Combine date and time into datetime objects
                        start_date_time = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M:%S")
                        end_date_time = datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M:%S")
                        # Step 4: Generate surrogate key for the activity (combination of user_id, start, and end times)
                        # surrogate_key = f"{user_id}_{start_date_time}_{end_date_tim
                        # Step 5: Insert the activity into the database
                        self.insert_activity_data(plt_file_id, user_id, None, start_date_time, end_date_time)



                    BATCH_SIZE = 1500  # Batch size for inserting track po
                    track_points_batch = set()  # set to accumulate batch of track points

                    #iterate through the rows in the plt file
                    for index, row in df_plt_file.iterrows():
                        latitude = row[0]
                        longitude = row[1]
                        altitude = row[3]
                        date_days = row[4]
                        date_time = row[5]
                        date_clock = row[6]
                        date_and_time_str = f"{date_time} {date_clock}"
                        date_and_time = datetime.strptime(date_and_time_str, "%Y-%m-%d %H:%M:%S")


                        if not label:
                            # Collect the track point data into a tuple (tuples are hashable, so can be stored in a set)
                            track_point = (plt_file_id, latitude, longitude, altitude, date_days, date_and_time)
                            track_points_batch.add(track_point)
                            # When batch size reaches BATCH_SIZE, insert the batch into the database
                            if len(track_points_batch) >= BATCH_SIZE:
                                self.insert_track_points_batch(track_points_batch)
                                # print(f"Inserted batch of {BATCH_SIZE} track points for activity {activity_file_id}")
                                track_points_batch.clear()  # Reset the b
                        if label:
                            # Check if the current track point falls within any labeled activity
                            for labeled_activity_id, activity_data in list(label_hashmap.items()):
                                # Assuming start_time and end_time are strings in the format "%Y-%m-%d %H:%M:%S"
                                start_time_str = activity_data['start_time']
                                end_time_str = activity_data['end_time']
                                # Convert strings to datetime objects
                                start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
                                
                                
                                end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                                # print("starttid",start_t
                                # Check if the track point's date_time is within the start_time and end_time
                                
                                if date_and_time == start_time:
                                    activity_data['start_found'] = True
                                if date_and_time == end_time:
                                    activity_data['end_found'] = True
                                    
                                if activity_data['start_found'] and activity_data['end_found']:
                                    print("starttid og sluttid funnet",start_time, end_time)
                                    transportation_mode = activity_data['transportation_mode']
                                    print("transportation_mode",transportation_mode)
                                    self.insert_activity_data(labeled_activity_id, user_id, transportation_mode, start_time, end_time)
                                    print(f"Inserted activity {labeled_activity_id} for user {user_id}, with mode {transportation_mode}")
                                    
                                    # Remove the activity from the hashmap to avoid duplicate insertions
                                    label_hashmap.pop(labeled_activity_id)
                                    track_points_batch.add((labeled_activity_id, latitude, longitude, altitude, date_days, date_and_time))
                          
                                
                                        
                            if len(track_points_batch) >= BATCH_SIZE:
                                self.insert_track_points_batch(track_points_batch)
                                # print(f"Inserted batch of {BATCH_SIZE} track points for activity {activity_file_id}")
                                track_points_batch.clear()  # Reset the batch


                    # Insert any remaining track points in the last batch
                    if track_points_batch:
                        self.insert_track_points_batch(track_points_batch)
                        # print(f"Inserted final batch of {len(track_points_batch)} track points for activity {activity_file_i
            break  # Ensures we only process the contents of Trajectory, no further recursion
        
        
def main():
    program = None
    try:
        program = InsertGeolifeDataset()
        
        
        data_folder = "/Users/ceciliehuser/Documents/skole/NTNU/h24/store_distr_data/dataset"  # Update with actual path

        program.drop_table("TrackPoint")
        program.drop_table("Users")
        program.drop_table("Activity")


        program.create_user_table("Users")
        program.create_activity_table("Activity")
        program.create_track_point_table("TrackPoint")
        
        
        # user010_folderpath = "/Users/ceciliehuser/Documents/skole/NTNU/h24/store_distr_data/dataset/Data/010"
        
        # Step 2: Insert users from the Data folder, assigning labels based on the labels.txt file
        program.traverse_folder(data_folder)

        # program.set_labels(user010_folderpath)
        # Step 3: Fetch data to verify insertion
        # program.show_20_rows("Users")
        
      
        
        
        
        # print activites from user 010 
        query = "SELECT * FROM Activity WHERE user_id = 010"
        program.cursor.execute(query)   
        rows = program.cursor.fetchall()
        print("Data from table Activity, tabulated:")
        print(tabulate(rows, headers=program.cursor.column_names))
        
        
     
        
        
        



       

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()