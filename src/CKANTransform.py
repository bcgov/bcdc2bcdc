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

# TODO: Idea of transform data set is going to be moved to its own module
class TransformDataSet:
    """Ties together a transform config with the data allowing you to apply
    transformations to the dataset as a whole.  Includes a record iterator
    allowing you to iterate over each record in the dataset.
    
    
    :raises InValidTransformationData: [description]
    """
    def __init__(self, dataType, transformData, transformConfigFile=None):
        self.validateType(dataType)
        self.dataType = dataType
        self.transformData = transformData

        if not isinstance(self.transformData, list):
            msg = "transformation data needs to be a list data type, " + \
                  f"transformData provided is type: {type(transformData)}"
            raise InValidTransformationData(msg)

def next(self):
    """ Iterate over features in the dataset
    """
    # TODO: implement this method, should transform record one at a time
    pass

    def getComparisonData(self):
        """removed machine generated data from the data allowing for comparison
        between two instances.
        """
        comparisonData = []
        for datasetItem in self.transformData:
            # TODO: Logic that goes here
            pass

class TransformationConfig:
    """Reads the transformation config file and provides methods to help 
    retrieve the desired information from the configuration.
    """

    def __init__(self, transformationConfigFile=None):
        self.transConf = getTransformationConfig(transformationConfigFile)

    def __parseNestForBools(self, data, boolVal, parsedData=None):
        """A recursive method that works its way through a transformation config
        record.  The method expects properties to be defined as either:

        * list
        * dict
        * bool

        returns back the same data structure but only with elements that are
        equal to 'boolVal'
        
        :param data: The input dataset that needs to be recursed through
        :type data: dict or list
        :param boolVal: Only properties that are equal to this value are included
            in the return dataset.
        :type boolVal: bool
        :param parsedData: Once recursion starts this property gets populated with
            the values that have already been parsed, subsequent recursions add
            to this datastruct
        :type parsedData: dict or list, optional
        :raises ValueError: Value errors are raised if the recusion runs into a
            type in input data structure that is not dict / list / bool
        :return: Same data structure but with values not meeting the 'boolval'
            removed.
        :rtype: dict or list
        """
        # TODO: could potentially implement using map. instead of if
        LOGGER.debug(f"data: {data}, boolVal: {boolVal}, parsedData: {parsedData}")
        if isinstance(data, dict):
            if parsedData is None:
                parsedData = {}

            for key, value in data.items():
                if isinstance(value, bool):
                    if value == boolVal:
                        LOGGER.debug(f"key: {key} value: {value} value type: {type(value)}")
                        parsedData[key] = value
                elif isinstance(value, dict) or isinstance(value, list):
                    parsedData[key] = self.__parseNestForBools(value, boolVal)
                else:
                    # a type that shouldn't be, raise error.
                    msg = f"The type associated with the key {key} is not a " + \
                        "dict / list or bool, fix the config file and rerun" + \
                        f"type is: {type(value)} value is {value}"
                    raise ValueError(msg)
        elif isinstance(data, list):
            if parsedData is None:
                parsedData = []

            for item in data:
                if isinstance(item, bool):
                    parsedData.append(item)
                elif isinstance(item, dict) or isinstance(val, list):
                    parsedData.append(self.__parseNestForBools(item, boolVal))
                else:
                    msg = f"The type associated with the item {item} is not a " + \
                        "dict / list or bool, fix the config file and rerun"
                    raise ValueError(msg)
        return parsedData

    def __getProperties(self, datatype, section, sectionValue):
        """using datatype and section as a dictionary keys, verifies that the
        requested sections exist in the transformation config document.  Values
        in the config all resolve to booleans.  In reading the doc returns
        
        :param datatype: the key value representing the data type to be retrieved
            from the transformation configuration
        :type datatype: str
        :param section: The section associated with the datatype that is to 
            be retrieved
        :type section: str
        :param sectionValue: The boolean value associated with properties in 
            the datastructure that should be included in the return dataset
        :type sectionValue: bool
        :return: Data Structure that has been filtered for values that match
            'sectionValue'
        :rtype: dict
        """
        retData = None
        if datatype in self.transConf:
            if section in self.transConf[datatype]:
                properties = self.transConf[datatype][section]
                LOGGER.debug(f"properties: {properties}")
                retData = self.__parseNestForBools(properties, sectionValue)
        return retData

    def getUserPopulatedProperties(self, datatype):
        """If the user populated parameters are defined in the config file for 
        the given datatype they will be returned.  If they are not defined then
        will return None.
        
        :param datatype: [description]
        :type datatype: [type]
        :return: [description]
        :rtype: [type]
        """

        validateType(datatype)
        section = constants.TRANSFORM_PARAM_USER_POPULATED_PROPERTIES
        userPopulated = self.__getProperties(datatype, section, True)
        return userPopulated

    def getAutoPopulatedProperties(self, datatype):
        """retrieves from the transformation config file the fields that are 
        defined as auto / machine generated.  These are fields that cannot
        be populated directory.  Example of a autogenerated field could be
        "Modification Date"
        
        :param datatype: The datatype. Valid data types are defined in the
            constants file in the parameter VALID_TRANSFORM_TYPES
        :type datatype: str
        :return: a list of fields that are defined in the config file as auto / 
            machine populated.
        :rtype: list, str
        """
        validateType(datatype)
        section = constants.TRANSFORM_PARAM_USER_POPULATED_PROPERTIES
        autoPopulated = self.__getProperties(datatype, section, False)
        return autoPopulated


class TransformRecord:

    def __init__(self, datatype, record, transformationConfig):
        self.validateType(datatype)
        self.record = record
        self.transformationConfig = transformationConfig


    def getComparisonRecord(self):
        usrFields = self.transformationConfig.getUserPopulatedProperties(datatype)
        compRecord = {}
        # for usrFields:
        #     #TODO: need to add this logic
        #     pass



    


class InValidTransformationTypeError(AttributeError):
    def __init__(self, message):
        self.message = message

class InValidTransformationData(AttributeError):
    def __init__(self, message):
        self.message = message

