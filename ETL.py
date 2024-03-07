import json
import pandas as pd
import mysql.connector
import logging
import os

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add a file handler to log to a file
log_file = 'etl.log'
file_handler = logging.FileHandler(log_file)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
class ETL:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        self.db_host = self.config['host']
        self.db_user = self.config['user']
        self.db_password = self.config['password']
        self.db_name = self.config['db']
        self.conn = None
        self.cur = None
        self.create_connection()

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            return json.load(f)
        
    def create_connection(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                auth_plugin='caching_sha2_password'  # Specify a compatible authentication plugin
            )
            if self.conn.is_connected():
                self.cur = self.conn.cursor()
                logger.info("Connected to MySQL database")
            else:
                logger.error("Failed to connect to MySQL database")
        except mysql.connector.Error as err:
            logger.error(f"Error connecting to MySQL database: {err}")
            
    def create_connection(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name
            )
            self.cur = self.conn.cursor()
            logger.info("Connected to MySQL database")
        except mysql.connector.Error as err:
            logger.error(f"Error connecting to MySQL database: {err}")

    def create_table(self):
        self.cur.execute('''CREATE TABLE IF NOT EXISTS data_files (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            year VARCHAR(255),
                            Mileage_thousands INT,
                            Price FLOAT)''')
        logger.info("Table 'data' created successfully")

    def extract(self, files):
        data_frames = []
        for file in files:
            if os.path.isfile(file):
                logger.info(f"Extracting data from {file}")
                df = pd.read_csv(file)
                data_frames.append(df)
            else:
                logger.warning(f"File {file} does not exist.")
        return pd.concat(data_frames, ignore_index=True)

    def transform(self, data):
        logger.info("Transforming data")
        # Normalize numeric columns
        numeric_columns = data.select_dtypes(include=['int64', 'float64']).columns
        data[numeric_columns] = (data[numeric_columns] - data[numeric_columns].min()) / (data[numeric_columns].max() - data[numeric_columns].min())
        return data
        print(data.columns)
        return data
    def load(self, data):
        logger.info("Loading data to MySQL database")
        columns = ', '.join(data.columns)
        
        placeholders = ', '.join(['%s'] * len(data.columns))
        for _, row in data.iterrows():
            sql = f"INSERT INTO data_files ({columns}) VALUES ({placeholders})"
            values = tuple(row[column] for column in data.columns)
            try:
                self.cur.execute(sql, values)
            except mysql.connector.Error as err:
                logger.error(f"Error executing SQL query: {err}")
        self.conn.commit()
    logger.info("Data loaded successfully")
    def run_pipeline(self, files):
        self.create_table()
        extracted_data = self.extract(files)
        transformed_data = self.transform(extracted_data)
        self.load(transformed_data)
        logger.info("ETL Pipeline completed")

if __name__ == "__main__":
    config_file = 'config.json'  # Configuration file name
    etl = ETL(config_file)
    files = ['ford_escort.csv']  # List of CSV files
    etl.run_pipeline(files)