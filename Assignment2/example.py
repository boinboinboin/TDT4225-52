from DbConnector import DbConnector
from tabulate import tabulate


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, query, table_name):
        try:
            self.cursor.execute(query)
            self.db_connection.commit()
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"ERROR: Failed to create table {table_name}:", e)


    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

        


def main():
    program = None
    try:
        program = ExampleProgram()

        
        # Define the queries for creating tables
        # Person Query
        person_table_query =    """CREATE TABLE IF NOT EXISTS Person (
                                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                                    name VARCHAR(30))"""
      
        # User Query
        user_table_query =      """CREATE TABLE IF NOT EXISTS User (
                                    id VARCHAR(255) NOT NULL PRIMARY KEY,
                                    has_labels BOOLEAN)"""

        # Activity Query
        activity_table_query =  """CREATE TABLE IF NOT EXISTS Activity (
                                    id INT AUTO_INCREMENT PRIMARY KEY,
                                    user_id VARCHAR(255) NOT NULL,
                                    transportation_mode VARCHAR(255),
                                    start_date_time DATETIME,
                                    end_date_time DATETIME,
                                    FOREIGN KEY (user_id) REFERENCES User(id))"""

        # Trackpoint Query
        trackpoint_table_query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                                      id INT AUTO_INCREMENT PRIMARY KEY,
                                      activity_id INT NOT NULL,
                                      lat DOUBLE,
                                      lon DOUBLE,
                                      altitude INT,
                                      date_days DOUBLE,
                                      date_time DATETIME,
                                      FOREIGN KEY (activity_id) REFERENCES Activity(id))"""
        
        # Creating the tables
        program.create_table(person_table_query, "Person")
        program.create_table(user_table_query, "User")
        program.create_table(activity_table_query, "Activity")
        program.create_table(trackpoint_table_query, "TrackPoint")

        # Insert data to the Person table
        program.insert_data(table_name="Person")

        # Fetch data from the Person table
        _ = program.fetch_data(table_name="Person")

        # Display the tables
        program.show_tables()

        # Delete the tables we created
        # Important that we do it in this order so that we don't break any foreign key constraints
        program.drop_table(table_name="TrackPoint")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="User")
        program.drop_table(table_name="Person")

        # Jada
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
