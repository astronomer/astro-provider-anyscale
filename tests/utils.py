from airflow import __version__ as airflow_version
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from packaging import version

AIRFLOW_VERSION = version.parse(airflow_version)


def test_dag(dag: DAG) -> DagRun:
    """Test a DAG run."""
    if AIRFLOW_VERSION >= version.Version("3.1"):
        # Airflow 3.1+ requires DAG to be serialized to database before calling dag.test()
        # because create_dagrun() checks for DagVersion and DagModel records

        from airflow.models.dagbag import DagBag, sync_bag_to_db
        from airflow.models.dagbundle import DagBundleModel
        from airflow.utils.session import create_session

        # Create DagBundle if it doesn't exist (required for DagModel foreign key)
        # This mimics what get_bagged_dag does via manager.sync_bundles_to_db()
        with create_session() as session:
            dag_bundle = DagBundleModel(name="test_bundle")
            session.merge(dag_bundle)
            session.commit()

        # This creates both DagModel and DagVersion records
        dagbag = DagBag(include_examples=False)
        dagbag.bag_dag(dag)
        sync_bag_to_db(dagbag, bundle_name="test_bundle", bundle_version="1")
        dag_run = dag.test(logical_date=timezone.utcnow())
    elif AIRFLOW_VERSION >= version.Version("3.0"):
        dag_run = dag.test(logical_date=timezone.utcnow())
    else:
        dag_run = dag.test()
    return dag_run
