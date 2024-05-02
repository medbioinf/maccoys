"""definition of search engine type
"""
# std imports
from enum import Enum, unique
from typing import List, Self


@unique
class SearchEngineType(Enum):
    """Defines a search engine type for handling, e.g. IO operations for different search engines."""

    COMET = "comet"
    """ Comet search engine
    """

    @classmethod
    def from_str(cls, value: str) -> Self:
        """
        Creates the search engine type from a string

        Parameters
        ----------
        value : str
            Name of search engine

        Returns
        -------
        SearchEngineType
            Search engine type

        Raises
        ------
        ValueError
            If search engine is unknown
        """
        match value.lower():
            case cls.COMET.value:
                return cls.COMET
            case _:
                raise ValueError(f"Unknown search engine type: {value}")

    @classmethod
    def get_all_names(cls) -> List[str]:
        """
        Returns all search engine names

        Returns
        -------
        List[str]
            List of search engine names
        """
        return [x.value for x in cls]
