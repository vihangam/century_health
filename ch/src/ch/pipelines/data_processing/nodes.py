import pandas as pd
from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import DoubleType
from sqlalchemy import create_engine
from kedro.io import DataCatalog
from kedro_datasets.pandas import SQLTableDataset

COLUMN_MAPPINGS = {
    "patients": {
        "PATIENT_ID" : "patient_id",
        "BIRTHDATE" : "birth_date",
        "DEATHDATE" : "death_date",
        "SSN" : "social_security_number",
        "FIRST" : "first_name",
        "LAST" : "last_name",
        "RACE" : "race",
        "GENDER" : "sex",
        "ADDRESS" : "address",
        "CITY" : "city",
        "STATE" : "state",
        "COUNTY" : "county",
        "ZIP" : "zip_code",
        "LAT" : "latitude",
        "LON" : "longitude"
    },
    "encounters": {
        "Id" : "encounter_id",
        "START" : "encounter_start_date",
        "STOP" : "encounter_end_date",
        "PATIENT" : "patient_id",
        "ORGANIZATION" : "facility_name",
        "ENCOUNTERCLASS": "encounter_type",
        "CODE" : "primary_diagnosis_code",
        "DESCRIPTION" : "primary_diagnosis_description",
        "BASE_ENCOUNTER_COST" : "charge_amount",
        "TOTAL_CLAIM_COST" : "paid_amount",
        "PAYER_COVERAGE" : "allowed_amount"
    },
    "symptoms": {
        "OBSERVATION_ID": "observation_id",  # Concatenated the patient id and pathology to create a unique key
        "PATIENT": "patient_id"
    },
    "conditions": {
        "START" : "onset_date",
        "STOP" : "resolved_date",
        "PATIENT" : "patient_id",
        "ENCOUNTER" : "encounter_id",
        "CODE" : "condition_id",
        "DESCRIPTION" : "normalized_description"
    },
    "medications": {
        "CODE": "medication_id", 
        "PATIENT": "patient_id",
        "ENCOUNTER": "encounter_id"
    }
    
}


def standardize_column_names(df: pd.DataFrame, dataset_name: str):
    mappings = COLUMN_MAPPINGS.get(dataset_name, {})
    df = df.rename(columns=mappings)
    df.columns = [col.strip().lower() for col in df.columns]
    return df

def write_to_db(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    if dataset_name == "patients":
       df = standardize_column_names(df, "patients")
    elif dataset_name == "encounters":
        df = standardize_column_names(df, "encounters")
    elif dataset_name == "conditions":
        df = standardize_column_names(df, "conditions")
    elif dataset_name == "symptoms":
        df = standardize_column_names(df, "symptoms")
    elif dataset_name == "medications":
        df = standardize_column_names(df, "medications")
    return df

def clean_patients_data(patients: pd.DataFrame, patient_gender: pd.DataFrame) -> pd.DataFrame:
    # Example: remove rows with missing birth_date or invalid dates
    patients = patients[patients["birth_date"].notnull()]
    patients = patients[patients["birth_date"] != "9999-99-99"]
    
    # Convert to datetime
    patients["birth_date"] = pd.to_datetime(patients["birth_date"], errors="coerce")
    
    # Drop rows with parsing errors
    patients = patients[patients["birth_date"].notnull()]
    
    merged_df = patients.drop(columns=["GENDER"], errors="ignore").merge(
        patient_gender[["Id", "GENDER"]],
        left_on="patient_id",
        right_on="Id",
        how="left"
    )
    patients = merged_df[merged_df["state"] == "Texas"].copy()

    return patients

# def clean_encounters(encounters: pd.DataFrame) -> pd.DataFrame:
#     encounters = encounters.rename(columns={"PATIENT": "PATIENT_ID"})
#     encounters = encounters.rename(columns={"Id": "ENCOUNTER_ID"})

# def clean_conditions(conditions: pd.DataFrame) -> pd.DataFrame:
#     conditions = conditions.rename(columns={"PATIENT": "PATIENT_ID"})
#     conditions = conditions.rename(columns={"ENCOUNTER": "ENCOUNTER_ID"})

# def clean_medications(medications: pd.DataFrame) -> pd.DataFrame:
#     medications = medications.rename(columns={"PATIENT": "PATIENT_ID"})
#     medications = medications.rename(columns={"ENCOUNTER": "ENCOUNTER_ID"})

# def clean_symptoms(symptoms: pd.DataFrame) -> pd.DataFrame:
#     symptoms = symptoms.rename(columns={"PATIENT": "PATIENT_ID"})

def extract_symptoms_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Split and map each symptom entry into a dict with lowercase keys
    expanded = df["symptoms"].str.split(";").apply(
        lambda items: {
            item.split(":")[0].strip().lower().replace(" ", "_"): int(item.split(":")[1].strip())
            for item in items if ":" in item
        }
    )

    # Create DataFrame from those dicts
    symptoms_df = pd.DataFrame(expanded.tolist())

    # Merge with original DataFrame (excluding original 'SYMPTOMS' column)
    df = pd.concat([df.drop(columns=["symptoms"]), symptoms_df], axis=1)
    return df

def merge_datasets(df_left, df_right, key, how="left", suffixes=("_left", "_right")):
    return df_left.merge(
        df_right,
        on=key,
        how=how,
        suffixes=suffixes
    )

def merge_patients_and_symptoms(patients: pd.DataFrame, symptoms: pd.DataFrame) -> pd.DataFrame:
    return merge_datasets(patients, symptoms, key=["patient_id"])

def merge_patient_sysmtoms_and_encounters(patients_and_symptoms_merged: pd.DataFrame, encounters: pd.DataFrame) -> pd.DataFrame:
    return merge_datasets(patients_and_symptoms_merged, encounters, key=["patient_id"])

def merge_patient_sysmtoms_encounters_and_conditions(patient_sysmtoms_and_encounters_merged: pd.DataFrame, conditions: pd.DataFrame) -> pd.DataFrame:
    return merge_datasets(patient_sysmtoms_and_encounters_merged, conditions, key=["patient_id","encounter_id"])

def merge_patient_sysmtoms_encounters_conditions_and_medications(patient_sysmtoms_encounters_and_condition_merged: pd.DataFrame, medications: pd.DataFrame) -> pd.DataFrame:
    return merge_datasets(patient_sysmtoms_encounters_and_condition_merged, medications, key=["patient_id","encounter_id"])
