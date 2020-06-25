"""
Up to now been using the amazing deepdiff module to calculate
differences between two objects.

Want more callback functionality in deep diff so that I can:
* identify logic for comparison of specific keys in the dict
* logic would identify if the boolean representation of the value is False
* if that is the case for both the objects we are trying to compare then
  no diff should be reported.

example:
obj1 = root['source_data_path'] = None
obj2 = root['source_data_path'] = [] or '' or {}

in all cases above want to consider this to be no change.

Implementation:

* going to cludge this using the existing callback option: exclude_obj_callback
* putting in its own class so that the callback function will have easy access
   to the original object that is being compared
* first time it encounters a key: it does nothing,
* second time it encounters the key it will:
   * retrieve the value the first time it encountered it
   * get the current value
   * do they both resolve to False
   * if so then return True to exclude

"""
import deepdiff
import logging

LOGGER = logging.getLogger(__name__)

class Diff:

    def __init__(self, data1, data2):
        self.data1 = data1
        self.data2 = data2

        # idea here is if I eventually want to specify the specific fields
        # that I want the logic to apply to...  ... would require lots
        # more logic
        #self.typeIgnoreKeys = typeIgnoreKeys

        self.keysFound = {}

    def getDiff(self):
        diff = deepdiff.DeepDiff(self.data1,
                                 self.data2,
                                 ignore_order=True,
                                 exclude_obj_callback=self.excludeCallback)
        return diff

    def excludeCallback(self, *args):
        keyVal = args[0]
        keyRef = args[1]
        retVal = False
        if keyRef not in self.keysFound:
            self.keysFound[keyRef] = keyVal
        else:
            # this is the second time the key has been encountered,
            # exists in both datasets.
            if not keyVal and not self.keysFound[keyRef]:
                LOGGER.debug("ignore: {keyVal}")
                retVal = True
        return retVal
