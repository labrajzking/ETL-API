from fastapi import FastAPI
from fastapi.responses import JSONResponse
import mysql.connector
import json
import logging
from logging.handlers import RotatingFileHandler

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a file handler for logging
file_handler = RotatingFileHandler("app.log", maxBytes=1000000, backupCount=1)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)


app = FastAPI()

def load_db_config():
    with open("config.json", "r") as config_file:
        return json.load(config_file)

# Load database configurations from config.json
db_config = load_db_config()

def fetch_data_from_db():
    try:
        conn = mysql.connector.connect(**db_config, auth_plugin='caching_sha2_password')
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM data_files LIMIT 10")  # Fetch first 10 rows
        data = cursor.fetchall()
        return data
    except mysql.connector.Error as err:
        logger.error(f"Error fetching data from database: {err}")  # Log error
        return []
@app.get ('/')
async def root():
    return {"message": "Hello World"}
@app.get("/read/first-chunk")
async def read_first_chunk():
    data = fetch_data_from_db()
    return JSONResponse(content=data, status_code=200)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)