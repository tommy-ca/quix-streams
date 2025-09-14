import json
import os

import pytest

from quixstreams.state.slatedb.exceptions import SlateDBLockError
from quixstreams.state.slatedb.options import SlateDBOptions
from quixstreams.state.slatedb.partition import SlateDBStorePartition


def test_lock_respected_when_pid_exists(tmp_path):
    path = (tmp_path / "busy").as_posix()
    lock_path = path + ".lock"
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Seed lock with current, existing PID
    with open(lock_path, "w") as f:
        json.dump({"pid": os.getpid(), "ts": 0}, f)

    # With retries set to 1, we should immediately fail with SlateDBLockError (non-stale)
    opts = SlateDBOptions(open_max_retries=1, open_retry_backoff=0)
    with pytest.raises(SlateDBLockError):
        SlateDBStorePartition(path=path, options=opts)
