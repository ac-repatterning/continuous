"""
Module config
"""
import os


class Config:
    """
    Class Config

    For project settings
    """

    def __init__(self):
        """
        Constructor
        """

        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.pathway = os.path.join(self.warehouse, 'continuous')
        self.points_ = os.path.join(self.pathway, 'points')
        self.menu_ = os.path.join(self.pathway, 'menu')

        # Template
        self.s3_parameters_key = 's3_parameters.yaml'
        self.arguments_key = 'continuous/arguments.json'
        self.metadata_key = 'continuous/external/metadata.json'

        # The prefix of the Amazon repository where the quantiles will be stored
        self.prefix = 'warehouse/continuous'

        # Project metadata
        self.project_tag = 'hydrography'
        self.project_key_name = 'HydrographyProject'
