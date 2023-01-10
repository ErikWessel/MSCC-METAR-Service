from datetime import date
from typing import List, Optional

import numpy as np
import pandas as pd


class DateRange:
    def __init__(self, start:date, end:date) -> None:
        self.start: date = start
        self.end: date = end
    
    def __repr__(self) -> str:
        return f'DateRange(start={self.start}, end={self.end})'
    
    def get_days(self) -> int:
        return (self.end - self.start).days + 1
    
    def extend(self, start:Optional[date], end:Optional[date]):
        if start is not None and start < self.start:
            self.start = start
        if end is not None and end > self.end:
            self.end = end

def get_days_between_ranges(first:DateRange, second:DateRange):
    if (first.start - second.start).days > 0:
        # Second is before first date-range - swap positions
        return get_days_between_ranges(second, first)
    if (first.end - second.start).days >= -1:
        # Overlapping or directly adjecent date-ranges
        return 0
    # First is before second date-range with no overlap or adjecency
    return (second.start - first.end).days - 1

def get_days_overlap(first:DateRange, second:DateRange):
    if (first.start - second.start).days > 0:
        # Second is before first date-range - swap positions
        return get_days_overlap(second, first)
    overlap = (first.end - second.start).days
    return max(0, overlap + 1)

class DateChunker:

    def build_contiguous_chunks_from_dates(dates:np.ndarray[np.datetime64]) -> List[DateRange]:
        current_chunk = 0
        split_gap_days = 1
        current_date: date = pd.to_datetime(dates.item(0)).date()
        chunks: List[DateRange] = [DateRange(current_date, current_date)]
        diffs: np.ndarray[np.timedelta64] = np.diff(dates)
        if np.size(diffs) == 0:
            # Only one date in array
            return chunks
        for date_diff in diffs:
            assert isinstance(date_diff, np.timedelta64)
            current_date = current_date + pd.Timedelta(date_diff)
            if date_diff > split_gap_days:
                chunks += [DateRange(current_date, current_date)]
                current_chunk += 1
            chunks[current_chunk].end = current_date
        return chunks
    
    def extend_chunks(chunks:List[DateRange]) -> List[DateRange]:
        for chunk in chunks:
                # Extend to next day
                chunk.end = chunk.end + pd.Timedelta(days=1)
        return chunks
