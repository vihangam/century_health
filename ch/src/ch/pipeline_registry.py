"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from ch.pipelines.data_processing import pipeline as my_pipeline


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    return {
        "__default__": my_pipeline.create_pipeline(),
        "load_data_to_db": my_pipeline.create_pipeline(),
    }
