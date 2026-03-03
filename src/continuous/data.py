"""Module data.py"""

import datetime
import time
import typing

import dask.dataframe as ddf
import numpy as np
import pandas as pd

import src.elements.partition as pr
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.prefix


class Data:
    """
    Data
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, arguments: dict):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.
        :param arguments: A set of arguments vis-à-vis calculation & storage objectives.
        """

        self.__s3_parameters = s3_parameters
        self.__arguments = arguments

        self.__limit = self.__get_limit()
        self.__endpoint, self.__bucket_name = self.__get_interfaces()
        self.__pre = src.s3.prefix.Prefix(service=service, bucket_name=self.__bucket_name)

        # Focus
        self.__dtype = {'timestamp': np.float64, 'ts_id': np.float64, 'measure': float}

    def __get_limit(self) -> float:
        """

        :return:
        """

        spanning = self.__arguments.get('spanning')
        as_from = datetime.date.today() - datetime.timedelta(days=round(spanning*365.25))

        return 1000 * time.mktime(as_from.timetuple())

    def __get_interfaces(self) -> typing.Tuple[str, str]:
        """

        :return:
        """

        __s3_parameters: dict = self.__s3_parameters._asdict()
        __s3_arguments: dict = self.__arguments['s3']
        endpoint = __s3_parameters[__s3_arguments.get('p_prefix')] + __s3_arguments.get('affix') + '/'
        bucket_name = __s3_parameters[__s3_arguments.get('p_bucket')]

        return endpoint, bucket_name

    def __get_data(self, keys: list[str]):
        """

        :param keys:
        :return:
        """

        try:
            block: pd.DataFrame = ddf.read_csv(
                keys, header=0, usecols=list(self.__dtype.keys()), dtype=self.__dtype).compute()
        except ImportError as err:
            raise err from err

        block.reset_index(drop=True, inplace=True)
        block.sort_values(by='timestamp', ascending=True, inplace=True)
        block.drop_duplicates(subset='timestamp', keep='first', inplace=True)

        return block

    def exc(self, partition: pr.Partition) -> pd.DataFrame:
        """

        :param partition: Refer to src.elements.partitions
        :return:
        """

        # In focus
        prefix = self.__endpoint + str(partition.catchment_id) + '/' + str(partition.ts_id)

        # Hence
        listings = self.__pre.objects(prefix=prefix)
        keys = [f's3://{self.__bucket_name}/{listing}' for listing in listings]
        block = self.__get_data(keys=keys)

        # Starting from ...
        data = block.copy().loc[block['timestamp'] >= self.__limit, :]

        return data[['timestamp', 'measure']]
