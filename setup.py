from setuptools import setup

setup(
    name='mdf_connect_client',
    version='0.3.2',
    packages=['mdf_connect_client'],
    description='Materials Data Facility Connect Client',
    long_description=("The MDF Connect Client is the Python client to easily submit"
                      " datasets to MDF Connect."),
    install_requires=[
        "mdf-toolbox>=0.2.6",
        "nameparser>=1.0.4",
        "requests>=2.18.4"
    ],
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
    ],
    keywords=[
        "MDF",
        "Materials Data Facility",
        "materials science",
        "utility",
        "Connect Client"
    ],
    license="Apache License, Version 2.0",
    url="https://github.com/materials-data-facility/connect_client"
)
