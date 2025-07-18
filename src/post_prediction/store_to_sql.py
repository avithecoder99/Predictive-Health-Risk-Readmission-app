from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData, DateTime
from sqlalchemy.orm import sessionmaker
from datetime import datetime

def get_engine():
    DB_TYPE = "mysql"
    USER = "admin"
    PASSWORD = "admin123"
    HOST = "localhost"
    PORT = "3306"
    DB_NAME = "hospital_db"
    return create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")

def create_predictions_table(engine):
    metadata = MetaData()
    Table(
        'predictions_log',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('patient_id', String(20)),
        Column('age', Integer),
        Column('gender', String(10)),
        Column('blood_pressure', String(20)),
        Column('heart_rate', Integer),
        Column('cholesterol', Float),
        Column('blood_sugar', Float),
        Column('oxygen_saturation', Float),
        Column('temperature', Float),
        Column('readmitted_prediction', Integer),
        Column('readmitted_probability', Float),
        Column('timestamp', DateTime),
    )
    metadata.create_all(engine)
    print(" Table `predictions_log` is ready.")

def store_prediction_to_sql(patient_input: dict, prediction: dict):
    engine = get_engine()
    create_predictions_table(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        query = f"""
        INSERT INTO predictions_log (
            patient_id, age, gender, blood_pressure, heart_rate, cholesterol, blood_sugar,
            oxygen_saturation, temperature, readmitted_prediction, readmitted_probability, timestamp
        )
        VALUES (
            '{patient_input["patient_id"]}', {patient_input["age"]}, '{patient_input["gender"]}',
            '{patient_input["blood_pressure"]}', {patient_input["heart_rate"]},
            {patient_input["cholesterol"]}, {patient_input["blood_sugar"]},
            {patient_input["oxygen_saturation"]}, {patient_input["temperature"]},
            {prediction["readmitted_prediction"]}, {prediction["readmitted_probability"]},
            '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}'
        );
        """
        session.execute(query)
        session.commit()
        print(f" Stored prediction for patient {patient_input['patient_id']} into SQL DB.")
    except Exception as e:
        print(f" Failed to insert prediction: {e}")
        session.rollback()
    finally:
        session.close()
