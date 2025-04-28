from kedro.pipeline import Pipeline, node, pipeline
from .nodes import write_to_db
from .nodes import *
from kedro.io import DataCatalog


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=write_to_db,
                inputs="patients",   
                outputs="t_patients", 
                name="write_patients_to_db",
            ),
            node(
                func=write_to_db,
                inputs="encounters",   
                outputs="t_encounters", 
                name="write_encounters_to_db",
            ),
            node(
                func=write_to_db,
                inputs="symptoms",   
                outputs="t_symptoms", 
                name="write_symptoms_to_db",
            ),
            node(
                func=write_to_db,
                inputs="medications",   
                outputs="t_medications", 
                name="write_medications_to_db",
            ),
            node(
                func=write_to_db,
                inputs="conditions",   
                outputs="t_conditions", 
                name="write_conditions_to_db",
            ),
            node(
                func=clean_patients_data,
                inputs="catalog",   
                outputs="t_patients_cleaned", 
                name="clean_patients_data",
            ),
            
        ]
    )
