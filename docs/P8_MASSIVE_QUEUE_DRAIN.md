# Confirm Massive Celery queues are drained after P8 switchover.
#
# Prerequisites: Redis broker reachable; no stocks_massive / options_massive workers running.
#
#   celery -A bifrost_worker.celery.celery_app inspect reserved -Q stocks_massive,stocks_massive_high
#   celery -A bifrost_worker.celery.celery_app inspect reserved -Q options_massive,options_massive_high
#   celery -A bifrost_worker.celery.celery_app inspect active -Q stocks_massive,options_massive
#
# Expected: empty / no workers on those queues. stocks_ib may still be active.
#
# Code gate: bifrost_worker.data.massive.celery_queues.MASSIVE_QUEUES_DISABLED = True
#   - empties Celery Beat Massive schedule
#   - insert_job_massive_backfill refuses new rows (no orphan pending)
#   - run_massive_job no-ops and marks the job status=failed with reason=massive_queues_disabled
#   - _enqueue_massive_job / apply_async_massive_pending_job / pending_dispatch top-up no-op
#   - Ops Massive retry endpoints and Massive/data_readiness enqueue APIs return
#     error "Massive queues disabled; use market-data plugin"
