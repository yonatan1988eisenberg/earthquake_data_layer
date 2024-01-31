class DoneCollectingError(Exception):
    pass


class StorageConnectionError(Exception):
    pass


class RemainingRequestsError(Exception):
    pass


class NoHealthyRequestsError(Exception):
    pass
