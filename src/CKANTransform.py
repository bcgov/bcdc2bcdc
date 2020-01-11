"""
Used to transform data based on contents of the transformation configuration 
file.

"""
import json
import logging
import os.path

import constants

LOGGER = logging.getLogger(__name__)


def validateType(dataType):
    """ Transformation types are different CKAN object types that can be 
    configured for transformations.  They are referred to as strings.  This 
    method verifies that the strings line up with a valid type.
    
    
    :param dataType: [description]
    :type dataType: [type]
    """
    if dataType not in constants.VALID_TRANSFORM_TYPES:
        msg = (
            f"a transformation type of: {dataType} was specified however "
            + "this is not a valid transformation type.  Valid options are: "
            + f"{constants.VALID_TRANSFORM_TYPES}"
        )
        raise InValidTransformationTypeError(msg)

def getTransformationConfig(transformConfigFile=None):
    """Loads the transformation configuration information from the config file
    
    :return transConfData: the transformation configuration data loaded from 
        the transformation config file
    :rtype: dict
    """
    if transformConfigFile is None:
        transformConfigFile = os.path.join(
            os.path.dirname(__file__),
            "..",
            constants.TRANSFORM_CONFIG_DIR,
            constants.TRANSFORM_CONFIG_FILE_NAME,
        )
    with open(transformConfigFile) as json_file:
        transConfData = json.load(json_file)
    return transConfData

class TransformDataSet:
    def __init__(self, dataType, transformData, transformConfigFile=None):
        self.validateType(dataType)
        self.dataType = dataType
        self.transformData = transformData

        if not isinstance(self.transformData, list):
            msg = "transformation data needs to be a list data type, " + \
                  f"transformData provided is type: {type(transformData)}"
            raise InValidTransformationData(msg)



    def getComparisonData(self):
        """removed machine generated data from the data allowing for comparison
        between two instances.
        """
        comparisonData = []
        for datasetItem in self.transformData:





    # TODO: make an iterator that returns transform records

class TransformRecord:

    def __init__(self, datatype, record, transformConfigFile=None):
        self.validateType(datatype)
        self.record = record
        if transformConfigFile is None:
            getTransformationConfig(transformConfigFile):


    def getComparisonRecord(self):

    


class InValidTransformationTypeError(AttributeError):
    def __init__(self, message):
        self.message = message

class InValidTransformationData(AttributeError):
    def __init__(self, message):
        self.message = message

