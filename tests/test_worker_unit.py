import pytest

from kap_platform import worker
from kap_platform.kap.client import KapError


class FakeKap:
    def __init__(self, result=None, error=None):
        self.calls = 0
        self.result, self.error = result, error

    async def list_users(self, tenant_id):
        self.calls += 1
        if self.error:
            raise self.error
        return self.result


@pytest.fixture(autouse=True)
def clear_cache():
    worker._USER_NAMES.clear()


async def test_user_names_are_cached_per_tenant():
    kap = FakeKap([{"id": "u1", "username": "probe_admin"}])
    assert await worker.user_names(kap, "t1") == {"u1": "probe_admin"}
    assert await worker.user_names(kap, "t1") == {"u1": "probe_admin"}
    assert kap.calls == 1


async def test_user_names_empty_without_permission():
    kap = FakeKap(error=KapError(403, {"type": "forbidden_error", "message": "no"}))
    assert await worker.user_names(kap, "t1") == {}


async def test_user_names_other_errors_propagate():
    kap = FakeKap(error=KapError(503, {"type": "unavailable", "message": "down"}))
    with pytest.raises(KapError):
        await worker.user_names(kap, "t1")
