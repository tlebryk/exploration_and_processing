import setuptools

setuptools.setup(name='thesisutils',
version='0.3.1',
description='Add timeit and justletters',
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