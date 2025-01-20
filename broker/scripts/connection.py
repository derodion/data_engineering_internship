import os

import dotenv
from sqlalchemy import create_engine

dotenv.load_dotenv()


user = os.getenv("USER_GP")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")
schema = os.getenv("SCHEMA")


engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")