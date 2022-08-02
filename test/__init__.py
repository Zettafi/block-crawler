from unittest.mock import MagicMock


class AsyncContextManager:
    """
    Helper for mocking
    """

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


def async_context_manager_mock():
    return MagicMock(AsyncContextManager)
