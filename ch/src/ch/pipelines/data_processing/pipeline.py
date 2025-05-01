from kedro.pipeline import Pipeline, node, pipeline
from .nodes import write_to_db
from .nodes import *
from kedro.io import DataCatalog


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=write_to_db,
                inputs=["patients","params:patients_dataset_name"],   
                outputs="t_patients", 
                name="write_patients_to_db",
            ),
            node(
                func=write_to_db,
                inputs=["encounters","params:encounters_dataset_name"],   
                outputs="t_encounters", 
                name="write_encounters_to_db",
            ),
            node(
                func=write_to_db,
                inputs=["symptoms","params:symptoms_dataset_name"],   
                outputs="t_symptoms", 
                name="write_symptoms_to_db",
            ),
            node(
                func=write_to_db,
                inputs=["medications","params:medications_dataset_name"],   
                outputs="t_medications", 
                name="write_medications_to_db",
            ),
            node(
                func=write_to_db,
                inputs=["conditions","params:conditions_dataset_name"],   
                outputs="t_conditions", 
                name="write_conditions_to_db",
            ),
            node(
                func=clean_patients_data,
                inputs=["t_patients","patient_gender"],
                outputs="cleaned_patients",
                name="clean_patients_node"
            ),
            node(
                func = extract_symptoms_columns,
                inputs = ['t_symptoms'],
                outputs = 'symptoms_intermediate',
                name = "split_symptoms"
            ),
            node(
                func = write_to_db,
                inputs = ['symptoms_intermediate',"params:symptoms_dataset_name"],
                outputs = 't_symptoms_intermediate',
                name = "write_symptoms_intermediate_to_db"
            ),
            node(
            func=merge_patients_and_symptoms,
            inputs=["cleaned_patients", "t_symptoms_intermediate"],
            outputs="patients_and_symptoms_merged",
            name="merge_patients_and_symptoms_node",
            ),
            node(
            func=merge_patient_sysmtoms_and_encounters,
            inputs=["patients_and_symptoms_merged", "t_encounters"],
            outputs="patients_symptoms_encounters_merged",
            name="merge_patient_sysmtoms_and_encounters_node",
            ),
            node(
            func=merge_patient_sysmtoms_encounters_and_conditions,
            inputs=["patients_symptoms_encounters_merged", "t_conditions"],
            outputs="patients_symptoms_encounters_condtitions_merged",
            name="merge_patient_sysmtoms_encounters_and_conditions_node",
            ),
            node(
            func=merge_patient_sysmtoms_encounters_conditions_and_medications,
            inputs=["patients_symptoms_encounters_condtitions_merged", "t_medications"],
            outputs="t_master_table",
            name="merge_patient_sysmtoms_encounters_conditions_and_medications_node",
            )
        ]
    )
