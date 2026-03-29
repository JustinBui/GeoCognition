# To run this file individually, be inside of your root directory. Then run
#       `astro dev bash`
#       `pytest tests/test_usgs_eq_pipeline.py -q`
#       To exit the bash container, type `exit`


from airflow.models import DagBag


def test_dag_loaded():
    dag_bag = DagBag()
    dag = dag_bag.get_dag("usgs_to_minio_daily_http_operator")

    assert dag is not None
    assert len(dag.tasks) > 0


def test_dag_tasks():
    dag_bag = DagBag()
    dag = dag_bag.get_dag("usgs_to_minio_daily_http_operator")

    task_ids = [t.task_id for t in dag.tasks]

    expected_tasks = [
        "fetch_usgs_events",
        "validate_eq_payload",
        "upload_raw_eq_json_to_minio",
        "flatten_eq_json_to_df",
        "rename_df_columns",
        "upload_flattened_eq_to_minio",
        "create_postgis_table",
        "load_eq_to_postgres",
    ]

    for task in expected_tasks:
        assert task in task_ids
