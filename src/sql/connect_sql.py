# src/sql/connect_sql.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

def get_sql_connection(
    db_type="mysql",      # or "postgresql"
    username="root",
    password="yourpassword",
    host="localhost",
    port="3306",          # or "5432" for PostgreSQL
    database="hospital_db"
):
    """
    Create SQLAlchemy engine and session for connecting to the SQL database.
    Returns both the engine and a scoped session.
    """

    if db_type == "mysql":
        connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
    elif db_type == "postgresql":
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    else:
        raise ValueError("Unsupported database type. Use 'mysql' or 'postgresql'.")

    try:
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        print(" Successfully connected to SQL database.")
        return engine, session
    except Exception as e:
        print(f" Failed to connect to the database: {e}")
        return None, None
