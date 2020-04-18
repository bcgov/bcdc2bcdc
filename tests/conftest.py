
# dealing with the fixture imports

import sys
import os


# While in development run tests off of dev code vs packaged code
devPath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
print(f'devPath: {devPath}')
sys.path.insert(0, devPath)

from tests.fixtures.BCDC_fixtures import *
from tests.fixtures.Transformation_fixures import *
from tests.fixtures.CKANDataSet_fixtures import *
from tests.fixtures.Constants_fixtures import *
from tests.fixtures.CKAN_fixtures import *
from tests.fixtures.DataCache_fixtures import *

#import tests.fixtures.DataCache_fixtures as DataCache_fixtures
