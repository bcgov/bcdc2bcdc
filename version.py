'''
Created on Jun. 18, 2019

@author: KJNETHER

Gets the latest version from pypi and increments by 1
'''
import logging
import os
import re

import packaging.version

import bcdc2bcdc
import distlib.index

pkg_name = bcdc2bcdc.name

if ('PKG_TYPE' in os.environ) and os.environ['PKG_TYPE'] in ['MASTER', 'PROD']:
    pkg_name = bcdc2bcdc.name.replace('_', '-')
elif ('PKG_TYPE' in os.environ) and os.environ['PKG_TYPE'] in ['DEV']:
    pkg_name = 'bcdc2bcdc-dev'
else:
    msg = 'The package name is calculated based on the value of the environment' + \
          ' variable, PKG_TYPE.  This environment variable is either not set ' + \
          'or contains an unexpected value.  Valid values include: "MASTER", ' + \
          '"PROD" and "DEV"'
    raise EnvironmentError(msg)

print(f'package name: {pkg_name}')

LOGGER = logging.getLogger(__name__)
# LOGGER.setLevel(logging.DEBUG)
# hndlr = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
# hndlr.setFormatter(formatter)
# LOGGER.addHandler(hndlr)

def get_package_version():
    '''
    looks in the __init__ for the package to see if there is a variable
    defined for version
    '''
    version = None
    try:
        version = bcdc2bcdc.version  # @UndefinedVariable
    except AttributeError:
        pass
    return version

def get_current_pypy_version():
    '''
    Query pypi to determine the latest version of the package
    '''
    LOGGER.debug('getting pypi package version')
    LOGGER.debug("package name: %s", pkg_name)
    pkg_indx = distlib.index.PackageIndex()
    srch = pkg_indx.search(pkg_name)
    version = '0.0.0'

    pkg_info = None

    for i in srch:
        LOGGER.debug("pkg: %s", i)
        # package names sometimes have _ other times -, so considering them
        # interchangeably
        if i['name'] == pkg_name or i['name'].replace('-', '_') == pkg_name:
            LOGGER.debug("pkg : %s", i)
            pkg_info = i

    if pkg_info:
        version = pkg_info['version']


    return version


def is_less_than(next_version, current_version):
    '''
    tests to see if the next_version is less than the current version,

    This is in place to address the situation where the version is populated
    in the packages __init__.py file, but hasn't been updated.

    any non numeric characters that are encountered in the versions will
    be stripped out.  If after stripping out the characters there is a
    blank string will assume the number should be 0

    '''
    next_lst = next_version.split('.')
    cur_lst = current_version.split('.')

    # strip out numeric characters
    next_lst = [re.sub("[^0-9]", "", i) if not i.isdigit() else i for i in next_lst]
    cur_lst = [re.sub("[^0-9]", "", i) if not i.isdigit() else i for i in cur_lst]

    # if any elements are blank then make then 0
    next_lst = [0 if not i.strip() else i for i in next_lst]
    cur_lst = [0 if not i.strip() else i for i in cur_lst]

    next_is_less_than_cur = False

    # going to raise an error if the patch version is not a number
    if len(next_lst) != 3:
        msg = (
            'next version is currently set to {0}.  Expecting a semantic '
              'version in the form of <major>.<minor>.<patch>'
        )
        msg.format(next_version)
        raise ValueError(msg)
    if len(cur_lst) != 3:
        msg = 'current version for the package is set to {0}.  Expecting a ' + \
              'semantic version in the form of <major>.<minor>.<patch>'
        msg.format(current_version)
        raise ValueError(msg)

    next_nostr = '.'.join(next_lst)
    cur_nostr = '.'.join(cur_lst)

    if packaging.version.parse(next_nostr) <= packaging.version.parse(cur_nostr):
        next_is_less_than_cur = True
    return next_is_less_than_cur


def increment_version(version):
    '''
    :param version: the semantic versioning string, increments the patch number
                    by one
    :type version: str
    '''
    version_lst = version.split('.')
    patch_version = version_lst[2]
    if patch_version.isdigit():
        patch_version = int(patch_version) + 1
        version_lst[2] = str(patch_version)
    else:
        msg = 'The patch version cannot be incremented as it is not a number. ' + \
              'version is: {0}, and extracted patch number is {1}'
        msg = msg.format(version, patch_version)
        raise ValueError(msg)
    newversion = '.'.join(version_lst)
    return newversion


version = get_current_pypy_version()
next_version = get_package_version()

print(f'{version} -- {next_version}')


if not next_version:
    next_version = increment_version(version)

if next_version == version or is_less_than(next_version, version):
    next_version = increment_version(version)

print(f'current version is: {version}')
print(f'next version is: {next_version}')
