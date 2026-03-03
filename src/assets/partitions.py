"""Module partitions.py"""
import typing

import numpy as np
import pandas as pd


class Partitions:
    """
    Partitions for parallel computation.
    """

    def __init__(self, data: pd.DataFrame, arguments: dict):
        """

        :param data:
        :param arguments:
        """

        self.__data = data
        self.__arguments = arguments

    def __get_listings(self) -> pd.DataFrame:
        """

        :return:
        """

        codes = np.unique(np.array(self.__arguments.get('excerpt')))

        if codes.size == 0:
            return  self.__data

        catchments = self.__data.loc[self.__data['ts_id'].isin(codes), 'catchment_id'].unique()
        data = self.__data.copy().loc[self.__data['catchment_id'].isin(catchments), :]

        return data if data.shape[0] > 0 else self.__data

    def exc(self) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
        """

        :return:
        """

        # Instead
        listings = self.__get_listings()
        partitions = listings[['catchment_id', 'ts_id']].drop_duplicates()

        return partitions, listings
