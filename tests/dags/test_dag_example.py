"""Example DAGs test. This test ensures that all Dags have tags, retries set to two, and no import errors. This is an example pytest and may not be fit the context of your DAGs. Feel free to add and remove tests."""

import os
import logging
from contextlib import contextmanager
import pytest
from pathlib import Path
from airflow.models import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from airflow.utils.session import create_session
from airflow.models import Connection

import utils as test_utils

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent.parent / "anyscale_provider/example_dags"


def get_dags(dag_folder=None):
    # Generate a tuple of dag_id, <DAG objects> in the DagBag
    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False) if dag_folder else DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    dags_info = [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]
    
    # Print the paths
    for dag_id, dag, fileloc in dags_info:
        print(f"DAG ID: {dag_id}, File Location: {fileloc}")
    
    return dags_info

@pytest.mark.integration
@pytest.mark.parametrize("dag_id,dag, fileloc", get_dags(EXAMPLE_DAGS_DIR), ids=[x[2] for x in get_dags()])
def test_dag_runs(dag_id, dag, fileloc):

    with create_session() as session:
        create_default_connections(session)
        conn = Connection(conn_id="anyscale_conn",
                          conn_type="anyscale",
                          password=os.getenv("ANYSCALE_CLI_TOKEN"))
        session.add(conn)
        session.commit()  # Ensure the connection is committed to the database

        # Run the example dags
        test_utils.run_dag(dag)