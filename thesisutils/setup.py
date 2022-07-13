import setuptools

setuptools.setup(name='thesisutils',
version='0.3.7',
description='Add report dropped function',
url='#',
author='Theo',
install_requires=[
    "pandas",
    "boto3"
    ],
author_email='',
packages=setuptools.find_packages(),
include_package_data=True,
zip_safe=False)