import os
from dotenv import load_dotenv

load_dotenv()


DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = os.environ.get('DB_PORT', 5432)
DB_OPTIONS = os.environ.get('DB_OPTIONS',  '-c search_path=content')


DSN = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASS,
    'host': DB_HOST,
    'port': DB_PORT,
    'options': DB_OPTIONS,
}
