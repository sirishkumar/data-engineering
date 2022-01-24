from typing import Iterable, TypeVar

from rich.live import Live
from rich.text import Text
from rich.spinner import Spinner


T = TypeVar("T")


# Create a decorator to iterate over all elements in the input iterator
def spinner(iterator: Iterable[T]):
    with Live(Spinner("point", text=Text(repr("spinner_name"), style="green")), refresh_per_second=20):
        for item in iterator:
            yield item