from uuid import UUID
from datetime import datetime, timezone

from marshy import dump

from persisty.stored import stored


@stored(batch_size=10)
class NumberName:
    """Item linking a string representing a number with an integer value. Also has a uuid, and timestamps."""

    id: UUID
    title: str
    value: int
    created_at: datetime
    updated_at: datetime


_num2words = {
    1: "One",
    2: "Two",
    3: "Three",
    4: "Four",
    5: "Five",
    6: "Six",
    7: "Seven",
    8: "Eight",
    9: "Nine",
    10: "Ten",
    11: "Eleven",
    12: "Twelve",
    13: "Thirteen",
    14: "Fourteen",
    15: "Fifteen",
    16: "Sixteen",
    17: "Seventeen",
    18: "Eighteen",
    19: "Nineteen",
    20: "Twenty",
    30: "Thirty",
    40: "Forty",
    50: "Fifty",
    60: "Sixty",
    70: "Seventy",
    80: "Eighty",
    90: "Ninety",
    0: "Zero",
}

NUMBER_NAMES = []
for n in range(1, 100):
    title = _num2words.get(n)
    if not title:
        title = _num2words[n - n % 10] + _num2words[n % 10].lower()
    NUMBER_NAMES.append(
        NumberName(
            id=UUID("00000000-0000-0000-0000-000000000" + (str(1000 + n)[1:])),
            title=title,
            value=n,
            created_at=datetime.fromtimestamp(0, tz=timezone.utc),
            updated_at=datetime.fromtimestamp(0, tz=timezone.utc),
        )
    )


NUMBER_NAMES_DICTS = [dump(n) for n in NUMBER_NAMES]
