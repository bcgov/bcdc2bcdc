'''
Created on Jun. 6, 2019

@author: KJNETHER

using date as versions to simplify

useful links on setup params:
* https://stackoverflow.com/questions/24347450/how-do-you-add-additional-files-to-a-wheel
* https://setuptools.readthedocs.io/en/latest/setuptools.html#including-data-files
* https://packaging.python.org/guides/distributing-packages-using-setuptools/?highlight=package_data#package-data

'''
import setuptools
import version
import os.path
# Need to move source code into a folder and define a __init__.py file that
# includes the versions.
import bcdc2bcdc

with open("readme.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    requires = f.read().splitlines()
    print(f'requirements: {requires}')

# create list of config files
confFiles = []
for confFile in os.listdir('bcdc2bcdc_config'):
    confFileWithPath = os.path.join('bcdc2bcdc_config', confFile)
    if os.path.isfile(confFileWithPath):
        confFiles.append(confFileWithPath)

setuptools.setup(
    #name=bcdc_apitests.name,
    name=version.pkg_name,
    # version=datetime.datetime.now().strftime('%Y.%m.%d'),
    version=version.next_version,
    author="Kevin Netherton",
    author_email="kevin.netherton@gov.bc.ca",
    description="Utility for moving data between CKAN instances using the API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bcgov/bcdc2bcdc",
    packages=setuptools.find_packages(),
    python_requires='>=3.6.*, <4',
    install_requires=requires,
    include_package_data=True,
    scripts=['bin/runBCDC2BCDC.py'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development :: Testing",
        "Operating System :: OS Independent",
    ],
    package_data={
         'bcdc2bcdc':
		     ['../bcdc2bcdc_config/transformationConfig_NewDataModel.json',
              '../bcdc2bcdc_config/transformationConfig_prod2cat.json',
              '../bcdc2bcdc_config/transformationConfig_prod2cat.json',
              '../bcdc2bcdc_config/transformationConfig_prod2cati.json',
              '../bcdc2bcdc_config/logger.config']}
)
