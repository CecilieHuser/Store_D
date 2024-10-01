import os
from datetime import datetime

from DbConnector import DbConnector
from tabulate import tabulate


class InsertGeolifeDataset:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self, table_name):
        # Create a table with 'id' as the user number and 'label' as True/False
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                   id INT NOT NULL PRIMARY KEY,
                   label BOOLEAN)
                """
        self.cursor.execute(query)
        self.db_connection.commit()
        
    def create_activity_table(self, table_name):
        # Create a table with 'id' as the user number and 'label' as True/False
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                   id INT NOT NULL PRIMARY KEY,
                   user_id INT,
                   start_date_time DATETIME,
                   end_date_time DATETIME)
                """
        self.cursor.execute(query)
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

    def insert_users(self, table_name, data_folder, labels_file_path):
        """
        Inserts users into the table, assigning a label of True or False based on the labels.txt file.
        """
        labeled_users = self.read_labels(labels_file_path)

        # Iterate through each user folder (e.g., 000, 001, ..., 180)
        for user_folder in os.listdir(data_folder):
            try:
                user_id = int(user_folder)  # Convert folder name to an integer for user ID
                user_id_str = f"{user_id:03d}"  # Format the user ID as a three-digit string (e.g., '000', '001')
                label = user_id in labeled_users  # True if the user ID is in the labeled_users set, otherwise False
            
                # Check if the user already exists in the table
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id = %s", (user_id_str,))
                result = self.cursor.fetchone()

                if result[0] > 0:
                    print(f"User {user_id_str} already exists in the table, skipping.")
                else:
                    # Insert the user and label into the table
                    query = f"INSERT INTO {table_name} (id, label) VALUES (%s, %s)"
                    self.cursor.execute(query, (user_id_str, label))
                    print(f"Inserted user {user_id_str} with label {label}")
            except ValueError:
                print(f"Skipping invalid folder: {user_folder}")
            except Exception as e:
                print(f"Error inserting user {user_id}: {e}")
        
        self.db_connection.commit()



    def insert_activity(self, table_name, data_folder):
        """
    Iterates through each user folder, finds the trajectory folder, reads .plt files for activities,
    and extracts the start and end datetime for each activity. Then, inserts the activity into the database.
    """
        for user_folder in os.listdir(data_folder):
            try:
                user_id = int(user_folder)  # Convert folder name to an integer for user ID
                trajectory_folder = os.path.join(data_folder, user_folder, 'Trajectory')
            
                if not os.path.exists(trajectory_folder):
                    print(f"Skipping {user_folder}: No trajectory folder found.")
                    continue

                # Iterate through each .plt file inside the trajectory folder
                for plt_file in os.listdir(trajectory_folder):
                    if plt_file.endswith('.plt'):
                        plt_file_path = os.path.join(trajectory_folder, plt_file)
                        start_date_time, end_date_time = None, None
                    
                        with open(plt_file_path, 'r') as file:
                            lines = file.readlines()
                            trackpoints = [line.strip().split(', ') for line in lines[6:]]  # Skip first 6 lines (header)
                        
                            # Extract the first and last datetime from the trackpoints
                            if trackpoints:
                                start_date = trackpoints[0][5]  # 6th element is the date
                                start_time = trackpoints[0][6]  # 7th element is the time
                                end_date = trackpoints[-1][5]  # 6th element from the last line is the date
                                end_time = trackpoints[-1][6]  # 7th element from the last line is the time
                            
                                # Combine date and time into datetime objects
                                start_date_time = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M:%S")
                                end_date_time = datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M:%S")

                            # Generate surrogate key for the activity: user_id + start/end datetimes
                            surrogate_key = f"{user_id}_{start_date_time}_{end_date_time}"

                            # Insert the activity into the table
                            query = f"INSERT INTO {table_name} (id, user_id, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"
                            self.cursor.execute(query, (surrogate_key, user_id, start_date_time, end_date_time))
                            print(f"Inserted activity for user {user_id}: {plt_file}")

                self.db_connection.commit()

            except ValueError:
                print(f"Skipping invalid folder: {user_folder}")
            except Exception as e:
                print(f"Skipping .plt file due to error: {e}")


    def fetch_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Data from table %s, tabulated:" % table_name)
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


def main():
    program = None
    try:
        program = InsertGeolifeDataset()
        #insert users
        data_folder = "/Users/ceciliehuser/Documents/skole/NTNU/h24/store_distr_data/ex_2_dataset/Data"  # Update with actual path
        labels_file_path = "/Users/ceciliehuser/Documents/skole/NTNU/h24/store_distr_data/ex_2_dataset/labeled_ids.txt"  # Update with actual path

        # Step 1: Create tables
        program.create_user_table("Users")
        program.create_activity_table("Activity")

        # Step 2: Insert users from the Data folder, assigning labels based on the labels.txt file
        program.insert_users("Users", data_folder=data_folder, labels_file_path=labels_file_path)
        program.insert_activity("Activity", data_folder=data_folder)

        # Step 3: Fetch data to verify insertion
        program.fetch_data("Users")
        program.fetch_data("Activity")

        # Uncomment if you want to drop the table after the operation
        # program.drop_table(table_name=table_name)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
