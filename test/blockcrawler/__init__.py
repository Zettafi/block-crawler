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


def assert_timer_run(mocked_stats_service: MagicMock, timer):
    mocked_stats_service.timer.assert_called_once_with(timer)
    # enter starts the timer
    mocked_stats_service.timer.return_value.__enter__.assert_called_once()
    # exit ends the timer and records
    mocked_stats_service.timer.return_value.__exit__.assert_called_once()
