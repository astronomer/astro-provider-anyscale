pytest \
    -vv \
    --cov=anyscale_provider \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    --durations=0 \
    -m "not (integration or perf)"
