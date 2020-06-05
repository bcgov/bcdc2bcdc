'''
Created on Jun. 6, 2019

@author: KJNETHER

using date as versions to simplify
'''
import setuptools
import datetime
import version
# Need to move source code into a folder and define a __init__.py file that
# includes the versions.
# may want to re-org the entire repo, based on layout provided by pyscaffold
import bcdc2bcdc

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    requires = f.read().splitlines()
    print(f'requirements: {requires}')

setuptools.setup(
    #name=bcdc_apitests.name,
    name=version.pkg_name,
    # version=datetime.datetime.now().strftime('%Y.%m.%d'),
    version=version.next_version,
    author="Kevin Netherton",
    author_email="kevin.netherton@gov.bc.ca",
    description="Tool for moving data between CKAN instances using API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bcgov/bcdc2bcdc",
    packages=setuptools.find_packages(),
    python_requires='>=3.6.*, <4',
    install_requires=requires,
    include_package_data=True,
    scripts=['src/main.py'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development :: Testing",
        "Operating System :: OS Independent",
    ],
)
