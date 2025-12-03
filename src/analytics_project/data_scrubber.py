"""DataScrubber utility module for the Key Lime Pie BI project.

Provides reusable methods for cleaning raw datasets:
- Standardizing column names
- Stripping whitespace
- Dropping empty rows and duplicates
- Exporting cleaned DataFrames

Designed to be chained for flexible preprocessing pipelines.
"""

from __future__ import annotations

from pathlib import Path  # noqa: TC003
from typing import Self

import pandas as pd


class DataScrubber:
    """A class for cleaning and preprocessing pandas DataFrames."""

    def __init__(self, df: pd.DataFrame) -> None:
        """Initialize the DataScrubber with a pandas DataFrame.

        Args:
            df (pd.DataFrame): The raw DataFrame to clean.
        """
        self.df = df

    @classmethod
    def from_csv(cls: type[Self], filepath: Path, **kwargs) -> Self:
        """Load a CSV file into a DataScrubber instance.

        Args:
            filepath (Path): Path to the CSV file.
            **kwargs: Additional arguments passed to pandas.read_csv.

        Returns:
            DataScrubber: An instance containing the loaded DataFrame.
        """
        df = pd.read_csv(filepath, **kwargs)
        return cls(df)

    def standardize_column_names(self) -> Self:
        """Convert column names to lowercase and replace spaces with underscores.

        Returns:
            DataScrubber: The current instance with updated column names.
        """
        self.df.columns = self.df.columns.str.strip().str.lower().str.replace(" ", "_")
        return self

    def strip_whitespace(self) -> Self:
        """Strip leading/trailing whitespace from string columns.

        Returns:
            DataScrubber: The current instance with cleaned string columns.
        """
        str_cols = self.df.select_dtypes(include="object").columns
        self.df[str_cols] = self.df[str_cols].apply(lambda col: col.str.strip())
        return self

    def drop_empty_rows(self) -> Self:
        """Drop rows that are completely empty (all NaN).

        Returns:
            DataScrubber: The current instance with empty rows removed.
        """
        self.df = self.df.dropna(how="all")
        return self

    def drop_duplicates(self) -> Self:
        """Drop duplicate rows.

        Returns:
            DataScrubber: The current instance with duplicates removed.
        """
        self.df = self.df.drop_duplicates()
        return self

    def to_csv(self, filepath: Path, **kwargs) -> Self:
        """Save the cleaned DataFrame to CSV.

        Args:
            filepath (Path): Output file path.
            **kwargs: Additional arguments passed to pandas.DataFrame.to_csv.

        Returns:
            DataScrubber: The current instance after saving.
        """
        self.df.to_csv(filepath, **kwargs)
        return self
