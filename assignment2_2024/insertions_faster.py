import os
from datetime import datetime
from DbConnector import DbConnector
from tabulate import tabulate
import itertools


class InsertGeolifeDataset:
    """
    Class for insertion of the Geolife dataset into the database.
    """
    

    def __init__(self):
        """
        Initializes the class and creates the connection to the database. 
        """
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

#--------------------------CREATE TABLES-----------------------------
    def create_user_table(self):
        """
        Creates the Users table in the database.
        
        Table schema:
            - id (INT): Primary key, unique for each user.
            - has_labels (BOOLEAN): Indicates if the user has labels.
        """
        query = f"""CREATE TABLE IF NOT EXISTS User (
                   id INT NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        """
        Creates the Activity table in the database.
        
        Table schema:
            - id (INT): Primary key, unique for each activity.
            - user_id (INT): Foreign key to the Users table.
            - transportation_mode (VARCHAR): The mode of transportation.
            - start_date_time (DATETIME): The start date and time of the activity.
            - end_date_time (DATETIME): The end date and time of the activity.
        """
            
        query = f"""CREATE TABLE IF NOT EXISTS Activity (
            id INT PRIMARY KEY AUTO_INCREMENT,
            user_id INT,
            transportation_mode VARCHAR(30),
            start_date_time DATETIME,
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES User(id)
)
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_track_point_table(self):
        """
        Creates the TrackPoint table in the database.
        
        Table schema:
            - id (INT): Primary key, unique for each trackpoint.
            - activity_id (INT): Foreign key to the Activity table.
            - lat (DOUBLE): The latitude of the trackpoint.
            - lon (DOUBLE): The longitude of the trackpoint.
            - altitude (DOUBLE): The altitude of the trackpoint.
            - date_days (DOUBLE): The number of days since the start of the activity.
            - date_time (DATETIME): The date and time of the trackpoint.
        """
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

#--------------------------INSERT DATA-----------------------------
    # Insert a user
    def insert_user(self, user_id, has_labels):
        """
        Inserts a user into the Users table.
        
        Args:
            user_id (int): The user ID.
            has_labels (bool): Indicates if the user has labels.
        """
        labeltext = " "
        if has_labels:
            labeltext = "LABELED"
        try:
            query = """INSERT INTO User (id, has_labels) VALUES (%s, %s)"""
            values = (user_id, has_labels)
            self.cursor.execute(query, values)
            self.db_connection.commit()
            
            print(f"\n{'='*40}\nINSERTED USER {user_id} | {labeltext}\n{'='*40}\n")
        except Exception as e:
            print(f"Failed to insert user {user_id}: {e}")

    # Insert an activity and return the auto-generated activity_id 
    def insert_activity_data(self, user_id, transportation_mode, start_date_time, end_date_time):
        """
        Inserts an activity into the Activity table.
        
        Args:
            user_id (int): The user ID.
            transportation_mode (str): The mode of transportation.
            start_date_time (datetime): The start date and time of the activity.
            end_date_time (datetime): The end date and time of the activity.
            
        Returns:
            activity_id (int): The auto-generated activity ID.
            """
        try:
            query = """INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) 
                       VALUES (%s, %s, %s, %s)"""
            values = (user_id, transportation_mode, start_date_time, end_date_time)
            self.cursor.execute(query, values)
            self.db_connection.commit()
            activity_id = self.cursor.lastrowid
            # print(f"Inserted activity {activity_id} for user {user_id}, mode: {transportation_mode}, start: {start_date_time}, end: {end_date_time}")
            return activity_id  # Return the auto-generated activity_id
        except Exception as e:
            print(f"Failed to insert activity for user {user_id}: {e}")

    # Insert trackpoints in batch
    def insert_track_points_batch(self, track_points):
        """
        Inserts a batch of trackpoints into the TrackPoint table.
        
        Args:
            track_points (list): A list of tuples containing trackpoint data.
        """
        try:
            query = f"""INSERT IGNORE INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) 
                        VALUES (%s, %s, %s, %s, %s, %s)"""
            self.cursor.executemany(query, list(track_points))
            self.db_connection.commit()
            # print(f"Inserted {len(track_points)} trackpoints into the database.")
        except Exception as e:
            print(f"Failed to insert trackpoints: {e}")

#--------------------------LABELS DATASTRUCTURES-----------------------------
    def read_labels(self, labels_file_path):
        """
        Reads the labels.txt file and returns a set of user IDs (as integers) with a label of True.
        """
        labeled_users = set()
        with open(labels_file_path, 'r') as file:
            for line in file:
                labeled_users.add(int(line.strip()))  #Assume each line contains a user ID (e.g., 45)
        return labeled_users

    def create_label_hashmap(self, labels_file_path):
        """ 
        Reads the labels.txt file and returns a hashmap of labels.
        Args:
            labels_file_path (str): The path to the labels.txt file.
        
        Returns:
            labels (dict): A hashmap of labels with start and end times as keys.
        """
        labels = {}

        with open(labels_file_path, 'r') as file:
            next(file)  #Skip header line
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

#----------------------------TRAVERSE THE FOLDER STRUCTURE and INSERT DATA-----------------------------
    def traverse_folder(self, folder_path):
        """
        Traverses the folder structure, inserts users and associated activities, trackpoints in bulk
        
        Args:
            folder_path (str): The path to the Geolife dataset folder.
            
        """
        labeled_users_file = os.path.join(folder_path, "labeled_ids.txt")
        labeled_users = self.read_labels(labeled_users_file)

        # Traverse the folder structure
        for root, dirs, files in os.walk(os.path.join(folder_path, "Data")):
            dirs.sort()  # Sort directories to ensure correct order

            #Iterate through each user folder
            for user_folder in dirs:
                if user_folder == "Trajectory":
                    break
                
                user_folder_path = os.path.join(root, user_folder)
                user_id = int(user_folder)  # Extract user ID from folder
                has_labels = 1 if user_id in labeled_users else 0
                trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
                labels_hashmap = None
                
                
                #create labels hashmap if user has labels
                if has_labels:
                    labels_hashmap = self.create_label_hashmap(os.path.join(user_folder_path, 'labels.txt'))

                # Collect users data and insert into the database
                self.insert_user(user_id, has_labels)
              
                #Insert activities and trackpoints
                self.insert_activities_and_trackpoints(labels_hashmap,trajectory_folder_path, user_id, has_labels)

    def insert_activities_and_trackpoints(self, labels_hashmap, trajectory_folder_path, user_id, label):
        for root, dirs, files in os.walk(trajectory_folder_path):

                        
            trackpoints_to_insert = [] #List to store trackpoints for batch insert
            BATCH_SIZE = 2000  #Batch size for inserting trackpoints

            #Iterate through each .plt file inside the trajectory folder and add activity and trackpoints to database
            for plt_file in files:
                if plt_file.endswith('.plt'):
                    plt_file_path = os.path.join(trajectory_folder_path, plt_file)
                    
                    # Limit file size to less than 2500 trackpoint lines
                    with open(plt_file_path, 'r') as f:
                        total_lines = sum(1 for _ in f)
                    if total_lines - 6 > 2500:  # Skip files with more than 2500 trackpoints
                        # print(f"Skipping large file {total_lines-6} trackpoints.")
                        continue
                    
                    
                    # get start and end date from plt file
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
                    
                    # Insert activity into the database 
                    if not label:
                        activity_id = self.insert_activity_data(user_id, transportation_mode, start_datetime, end_datetime)
                    
                    if label:
                        # Loop through the labels hashmap
                        for (start_time, end_time), data in labels_hashmap.items():
                            
                            # if the start and end time of the activity has exact match in the labels hashmap, insert the activity with the transportation mode.
                            if start_time == start_datetime and end_time == end_datetime:
                                transportation_mode = data['transportation_mode']
                                activity_id = self.insert_activity_data(user_id, transportation_mode, start_datetime, end_datetime)
                                break
                        # if the start and end time of the activity does not have exact match in the labels hashmap, insert the activity without the transportation mode.
                        if transportation_mode is None:
                            activity_id = self.insert_activity_data(user_id, transportation_mode, start_datetime, end_datetime)
                            
                    # Insert all trackpoints in the plt file to the batch
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
 

#--------------------------OTHER FUNCTIONS-----------------------------

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
        
        
#--------------------------GET RELATIVE PATH FOR THE DATASET-----------------------------
        current_dir = os.path.dirname(os.path.realpath(__file__))

        dataset_dir = os.path.join(current_dir, '../../dataset')

        dataset_dir = os.path.normpath(dataset_dir)




#--------------------------DELETE TABLES-----------------------------
        # Drop tables if they exist
        program.drop_table("TrackPoint")
        print("TrackPoint table dropped")
        program.drop_table("Activity")
        print("Activity table dropped")
        program.drop_table("User")
        print("User table dropped")

#------------------ CREATE TABLES & INSERT DATA---------------------
        
        # Create tables
        program.create_user_table()
        program.create_activity_table()
        program.create_track_point_table()

        # Insert data
        print(f"Accessing dataset from: {dataset_dir}\n...")
        program.traverse_folder(dataset_dir)

#--------------------------SHOW DATA-----------------------------
        #Show first 10 rows of Users, Activity, and TrackPoint tables
        print("\nFirst 10 rows from Users table:")
        program.show_20_rows("User")

        print("\nFirst 10 rows from Activity table:")
        program.show_20_rows("Activity")

        print("\nFirst 10 rows from TrackPoint table:")
        program.show_20_rows("TrackPoint")
        
        
#-----------------Get activity for user 10---------------------
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