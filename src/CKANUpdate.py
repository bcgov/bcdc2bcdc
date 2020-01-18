"""
Functionality that can:
    * identify two CKANData.CKANDataSet ( or subclasses of these ) and identify 
      differences between the two
    * Using the CKAN module, update one or the other.
"""

# pylint: disable=logging-format-interpolation

import CKAN
import logging
import abc

LOGGER = logging.getLogger(__name__)


class CKANUpdate_abc(abc.ABC):
    """
    """
    def __init__(self, ckanWrapper=None):
        self.CKANWrap = ckanWrapper
        if ckanWrapper is None:
            self.CKANWrap = CKAN.CKANWrapper()

    @abc.abstractmethod
    def update(self, deltaObj):
        pass

    @abc.abstractmethod
    def doAdds(self, addStruct):
        pass

    @abc.abstractmethod
    def doDeletes(self, delStruct):
        pass

    @abc.abstractmethod
    def doUpdates(self, updtStruct):
        pass


class CKANUserUpdate(CKANUpdate_abc):

    def __init__(self, ckanWrapper=None):
        CKANUpdate_abc.__init__(self, ckanWrapper)

    def update(self, deltaObj):
        #delta = self.destCKANDataSet.getDelta(self.srcCKANDataSet)

        adds = deltaObj.getAddData()
        self.doAdds(adds)
        # TODO: define then uncomment these calls
        # deletes = deltaObj.getDeleteData()
        # self.doDeletes(deletes)
        # updates = deltaObj.getUpdateData()
        # self.doUpdates(updates)

    def doAdds(self, addStruct):
        for addData in addStruct:
            LOGGER.debug(f"adding dataset: {addData['name']}")

    def doDeletes(self, delStruct):
        pass


    def doUpdates(self, updtStruct):
        pass

# for subsequent types will define their own updates, or think about making a 
# mapping between different types and equivalent methods in CKAN.ckanWrapper 
# class 
