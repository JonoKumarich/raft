from typing import Any, Protocol


class DataStore(Protocol):
    def get(self, key: str) -> Any: ...

    def set(self, key: str, value: Any) -> None: ...

    def delete(self, key: str) -> None: ...

    def incr(self, key: str, value: Any) -> None: ...


class DictStore(DataStore):
    def __init__(self) -> None:
        self.data = {}

    def get(self, key: str) -> Any:
        return self.data.get(key)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value

    def delete(self, key: str) -> None:
        del self.data[key]

    # TODO: Handle incr properly, it now just appends strings to eachother
    def incr(self, key: str, value: Any) -> None:
        self.data[key] += value
