"""Redis keys for the Account Sync Daemon."""

from bifrost_core.core.redis_health_keys import BIFROST_HEALTH_ACCOUNT_SYNC_DAEMON

ACCOUNT_SYNC_HEALTH_KEY = BIFROST_HEALTH_ACCOUNT_SYNC_DAEMON
ACCOUNT_SYNC_LOG_STREAM_KEY = "bifrost:console:account_sync_daemon"
ACCOUNT_SYNC_CONSUMER_GROUP = "account_sync_group"
ACCOUNT_SYNC_CONSUMER_NAME = "consumer_0"
