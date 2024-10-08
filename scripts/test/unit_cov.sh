pytest \
    -vv \
    --cov=anyscale_provider \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m "not (integration or perf)" \
    --ignore=tests/dags/test_dag_example.py
