# Data Submission Codes

The direcotries contained in this folder are the scripts we use to submit data from external (i.e., non-MDF) repositories to the Materials Data Facility.

The procedure for each data source does vary slightly depending on the nature of the data source. 
Some data sources are downloaded and submitted via a Jupyter notebook, others rely on Python CLI tools. 
The steps needed to submit the data should be described in a README file in the individual direcotries.

The software required to submit certain data sources may require specialized environment. 
At present, we do not use automated tools for constructing the enviroments but the enviroments are described using the procedure defined by [`repo2docker`](https://repo2docker.readthedocs.io/en/latest/config_files.html).
