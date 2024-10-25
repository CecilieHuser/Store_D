from pprint import pprint
from DbConnector import DbConnector
from datetime import datetime
import os
import itertools


class InsertGeolifeDatasetMongo:
    """
    Class for insertion of the Geolife dataset into MongoDB.
    """

    def __init__(self):
        """
        Initializes the MongoDB connection.
        """
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        
        
#--------------------------CREATE COLLECTIONS-----------------------------

    def create_coll(self, collection_name):
        """
        Creates a collection in the MongoDB database.
        Args:
            collection_name ( ): The name of the collection to create.
        """
        try:
            self.db.create_collection(collection_name)    
            print(f'Created collection: {collection_name}')
        except Exception as e:
            print(f"Failed to create collection {collection_name}: {e}")


#--------------------------INSERT DOCUMENTS-----------------------------
    def insert_user(self, user_id, has_labels):
        """
        Inserts a user into the MongoDB collection 'User'.
        """
        try:
            user_data = {
                "_id": user_id,
                "has_labels": has_labels
            }
            self.db['User'].insert_one(user_data)
            print(f"Inserted user {user_id} | Labeled: {has_labels}")
        except Exception as e:
            print(f"Failed to insert user {user_id}: {e}")

    def insert_activity_data(self, user_id, transportation_mode, start_date_time, end_date_time, trackpoints):
        """
        Inserts an activity into the MongoDB collection 'Activity'.
        """
        try:
            activity_data = {
                "user_id": user_id,
                "transportation_mode": transportation_mode,
                "start_time": start_date_time,
                "end_time": end_date_time,
                "trackpoints": trackpoints
            }
            self.db['Activity'].insert_one(activity_data)
            print(f"Inserted activity for user {user_id} with transportation mode {transportation_mode}.")
        except Exception as e:
            print(f"Failed to insert activity for user {user_id}: {e}")

#--------------------------LABELS DATASTRUCTURES-----------------------------

    def read_labels(self, labels_file_path):
        labeled_users = set()
        with open(labels_file_path, 'r') as file:
            for line in file:
                labeled_users.add(int(line.strip()))
        print(f"Read labeled users: {labeled_users}")
        return labeled_users

    def create_label_hashmap(self, labels_file_path):
        labels = {}
        with open(labels_file_path, 'r') as file:
            next(file)  # Skip header line
            for line in file:
                start_time_str, end_time_str, transportation_mode = line.strip().split('\t')
                start_time = datetime.strptime(start_time_str, "%Y/%m/%d %H:%M:%S")
                end_time = datetime.strptime(end_time_str, "%Y/%m/%d %H:%M:%S")
                labels[(start_time, end_time)] = {
                    'transportation_mode': transportation_mode
                }
        print(f"Created label hashmap: {labels}")
        return labels


#--------------------------TRAVERSE FOLDER-----------------------------
    def traverse_folder(self, folder_path):
        labeled_users_file = os.path.join(folder_path, "labeled_ids.txt")
        labeled_users = self.read_labels(labeled_users_file)

        for root, dirs, files in os.walk(os.path.join(folder_path, "Data")):
            dirs.sort()

            for user_folder in dirs:
                if user_folder == "Trajectory":
                    break

                user_folder_path = os.path.join(root, user_folder)
                user_id = int(user_folder)
                has_labels = user_id in labeled_users
                trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')

                labels_hashmap = None
                if has_labels:
                    labels_hashmap = self.create_label_hashmap(os.path.join(user_folder_path, 'labels.txt'))

                print(f"Processing user {user_id} with labels: {has_labels}")

                self.insert_user(user_id, has_labels)
                self.insert_activities_and_trackpoints(labels_hashmap, trajectory_folder_path, user_id, has_labels)

#--------------------------INSERT ACTIVITIES AND TRACKPOINTS-----------------------------
  
    def insert_activities_and_trackpoints(self, labels_hashmap, trajectory_folder_path, user_id, label):
        for root, dirs, files in os.walk(trajectory_folder_path):
            for plt_file in files:
                if plt_file.endswith('.plt'):
                    plt_file_path = os.path.join(trajectory_folder_path, plt_file)

                    with open(plt_file_path, 'r') as f:
                        total_lines = sum(1 for _ in f)
                    if total_lines - 6 > 2500:
                        print(f"Skipping file {plt_file} for user {user_id} due to size limit.")
                        continue

                    with open(plt_file_path, 'r') as file:
                        lines = file.readlines()

                    seventh_line = lines[6].strip()
                    last_line = lines[-1].strip()

                    start_date = seventh_line.split(',')
                    end_date = last_line.split(',')

                    start_datetime_str = f"{start_date[5]} {start_date[6]}"
                    end_datetime_str = f"{end_date[5]} {end_date[6]}"

                    start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M:%S")
                    end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M:%S")
                    transportation_mode = None

                    trackpoints = []
                    for line in itertools.islice(lines, 6, None):
                        parts = line.strip().split(',')
                        lat, lon, altitude, date_days = float(parts[0]), float(parts[1]), float(parts[3]), float(parts[4])
                        timestamp = datetime.strptime(f"{parts[5]} {parts[6]}", "%Y-%m-%d %H:%M:%S")
                        trackpoints.append({
                            "lat": lat,
                            "lon": lon,
                            "altitude": altitude,
                            "date_days": date_days,
                            "date_time": timestamp
                        })

                    if label and labels_hashmap:
                        for (start_time, end_time), data in labels_hashmap.items():
                            if start_time == start_datetime and end_time == end_datetime:
                                transportation_mode = data['transportation_mode']
                                print(f"Found transportation mode {transportation_mode} for user {user_id} from labels.")
                                break

                    self.insert_activity_data(user_id, transportation_mode, start_datetime, end_datetime, trackpoints)

#--------------------------DROP COLLECTIONS-----------------------------
    def drop_coll(self, collection_name):
        """
        Drops a collection from the MongoDB database.
        Args:
            collection_name ( ): The name of the collection to drop.
        """
        try:
            self.db[collection_name].drop()
            print(f"Dropped collection: {collection_name}")
        except Exception as e:
            print(f"Failed to drop collection {collection_name}: {e}")
            
#--------------------------FETCH DOCUMENTS-----------------------------
    def fetch_first_10_users(self):
        """
        Fetch and print the first 20 documents from the User collection.
        """
        users = self.db['User'].find().limit(10)
        print("First 10 users:")
        for user in users:
            pprint(user)
            
    def fetch_first_10_activities(self):
        """
        Fetch and print the first activity document from the Activity collection.
        Only include the first two trackpoints, followed by "..." to indicate more trackpoints exist.
        """
        activities = self.db['Activity'].find().limit(10)
        no=0
        for activity in activities:
            no+=1
            print("#", no)
            activity['trackpoints'] = [{"..."},"...", {"..."}]
            pprint(activity)
            
        print("Printed activities:")
   
   
   
    def fetch_first_10_trackpoints_in_activity(self):
        """
        Fetch and print the first activity document from the Activity collection.
        Only include the first two trackpoints, followed by "..." to indicate more trackpoints exist.
        """
        activities = self.db['Activity'].find().limit(1)
        print("First 10 trackpoints for user ", activities[0]['user_id'])
        for activity in activities:
            if 'trackpoints' in activity and len(activity['trackpoints']) > 2:
                activity['trackpoints'] = activity['trackpoints'][:10] 
            pprint(activity)
            
    
    


def main():
    program = None
    try:
        program = InsertGeolifeDatasetMongo()
        
#--------------------------DROP COLLECTIONS-----------------------------
        program.drop_coll(collection_name="User")
        program.drop_coll(collection_name="Activity")

#--------------------------CREATE COLLECTIONS-----------------------------

        program.create_coll(collection_name="User")
        program.create_coll(collection_name="Activity")

        current_dir = os.path.dirname(os.path.realpath(__file__))
        dataset_dir = os.path.join(current_dir, '../../dataset')
        dataset_dir = os.path.normpath(dataset_dir)

        program.traverse_folder(dataset_dir)



#--------------------------FETCH DOCUMENTS-----------------------------
        program.fetch_first_10_users()
        print("-----------------------------------------------")
        program.fetch_first_10_activities()
        print("-----------------------------------------------")
        program.fetch_first_10_trackpoints_in_activity()
        
        
    except Exception as e:
        print(f"ERROR: Failed to use MongoDB: {e}")
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
