from datetime import datetime
import json

import globus_sdk
import mdf_toolbox
import requests


CONNECT_SERVICE_LOC = "https://api.materialsdatafacility.org"
CONNECT_DEV_LOC = "https://dev-api.materialsdatafacility.org"
CONNECT_CONVERT_ROUTE = "/submit"
CONNECT_STATUS_ROUTE = "/status/"
CONNECT_ALL_STATUS_ROUTE = "/submissions/"


class MDFConnectClient:
    """The MDF Connect Client is the Python client to easily submit datasets to MDF Connect."""
    __app_name = "MDF_Connect_Client"
    __login_services = ["connect"]
    __allowed_authorizers = [
        globus_sdk.RefreshTokenAuthorizer,
        globus_sdk.ClientCredentialsAuthorizer,
        globus_sdk.NullAuthorizer
    ]

    def __init__(self, test=False, service_instance=None, authorizer=None):
        """Create an MDF Connect Client.

        Arguments:
            test (bool): When ``False``, the dataset will be processed normally.
                    When ``True``, the dataset will be processed, but submitted to
                    test/sandbox/temporary resources instead of live resources.
                    This includes the ``mdf-test`` Search index and MDF Test Publish collection.
                    **Default:** ``False``
            service_instance (str): The instance of the MDF Connect API to use.
                    This value should not normally be changed from the default.
                    **Default:** ``None``, to use the default API instance.
            authorizer (globus_sdk.GlobusAuthorizer): The authorizer to use for authentication.
                    This value should not normally be changed from the default.
                    **Default:** ``None``, to run the standard authentication flow.

        Returns:
            *MDFConnectClient*: An initialized, authenticated MDF Connect Client.
        """
        self.test = test
        self.update = False
        '''
        self.dc = {}
        self.mdf = {}
        self.mrr = {}
        self.custom = {}
        self.projects = {}
        self.data_sources = []
        self.data_destinations = []
        self.index = {}
        self.conversion_config = {}
        self.services = {}
        self.tags = []
        self.update = False
        '''
        if service_instance == "prod" or service_instance is None:
            self.service_loc = CONNECT_SERVICE_LOC
        elif service_instance == "dev":
            self.service_loc = CONNECT_DEV_LOC
        else:
            self.service_loc = service_instance
        self.convert_route = CONNECT_CONVERT_ROUTE
        self.status_route = CONNECT_STATUS_ROUTE
        self.all_status_route = CONNECT_ALL_STATUS_ROUTE

        self.reset_submission()

        if any([isinstance(authorizer, allowed) for allowed in self.__allowed_authorizers]):
            self.__authorizer = authorizer
        else:
            self.__authorizer = mdf_toolbox.login({"app_name": self.__app_name,
                                                   "services": self.__login_services
                                                   }).get("connect")
        if not self.__authorizer:
            raise ValueError("Unable to authenticate")

    def logout(self):
        """Log out by removing cached tokens and discarding the client's authorizer"""
        self.__authorizer = None
        mdf_toolbox.logout()

    def create_dc_block(self, title, authors,
                        affiliations=None, publisher=None, publication_year=None,
                        resource_type=None, description=None, dataset_doi=None,
                        related_dois=None, subjects=None,
                        **kwargs):
        """Create your submission's dc block.
        This block is the DataCite block. Additional information on DataCite fields
        is available from the official DataCite website:
        https://schema.datacite.org/meta/kernel-4.1/

        Arguments:
            title (str or list of str): The title(s) of the dataset.
            authors (str or list of str): The author(s) of the dataset. Format must be one of:

                    * ``"Givenname Familyname"``
                    * ``"Familyname, Givenname"``
                    * ``"Familyname; Givenname"``

                    No additional commas or semicolons are permitted.
            publisher (str): The publisher of the dataset (not an associated paper).
                    **Default:** The Materials Data Facility.
            publication_year (int or str): The year of dataset publication.
                    **Default:** The current year.
            resource_type (str): The type of resource. Except in unusual cases, this should be
                                 ``"Dataset"``. **Default:** ``"Dataset"``
            affiliations (str or list of str or list of list of str):
                    The affiliations of the authors, in the same order.
                    If a different number of affiliations are given,
                    all affiliations will be applied to all authors.
                    Multiple affiliations can be given as a list.
                    **Default:** ``None`` for no affiliations for any author.

                    Examples::

                        authors = ["Fromnist, Alice", "Fromnist; Bob", "Cathy Multiples"]
                        # All authors are from NIST
                        affiliations = "NIST"
                        # All authors are from both NIST and UChicago
                        affiliations = ["NIST", "UChicago"]
                        # Alice and Bob are from NIST, Cathy is from NIST and UChicago
                        affliliations = ["NIST", "NIST", ["NIST", "UChicago"]]

                        # This is incorrect! If applying affiliations to all authors,
                        # lists must not be nested.
                        affiliations = ["NIST", ["NIST", "UChicago"], "Argonne", "Oak Ridge"]
            description (str): A description of the dataset.
                    **Default:** ``None`` for no description.
            dataset_doi (str): The DOI for this dataset (not an associated paper).
                    **Default:** ``None``
            related_dois (str or list of str): DOIs related to this dataset,
                    not including the dataset's own DOI (for example, an associated paper's DOI).
                    **Default:** ``None``
            subjects (str or list of str): Subjects (in Datacite terminology) or tags related
                    to the dataset. **Sefault:** ``None``
        Any further keyword arguments will be added to the DataCite metadata (the dc block).
        These arguments should be valid DataCite, as listed in the MDF Connect documentation.
        This is completely optional.
        """
        if not title and not authors:
            raise TypeError("'title' and 'authors' are required arguments.")
        if not title:
            raise TypeError("'title' is a required arguments.")
        if not authors:
            raise TypeError("'authors' is a required argument.")
        # titles
        if not isinstance(title, list):
            title = [title]
        titles = [{"title": t} for t in title]

        # creators
        if not isinstance(authors, list):
            authors = [authors]
        if not affiliations:
            affiliations = []
        elif not isinstance(affiliations, list):
            affiliations = [affiliations]
        if not len(authors) == len(affiliations):
            affiliations = [affiliations] * len(authors)
        creators = []
        for auth, affs in zip(authors, affiliations):
            if auth.find(",") >= 0:
                family, given = auth.split(",", 1)
            elif auth.find(";") >= 0:
                family, given = auth.split(";", 1)
            elif auth.find(" ") >= 0:
                given, family = auth.split(" ", 1)
            else:
                given = auth
                family = ""
            if not isinstance(affs, list):
                affs = [affs]

            family = family.strip()
            given = given.strip()
            creator = {
                "creatorName": "{}, {}".format(family, given),
                "familyName": family,
                "givenName": given
            }
            if affs:
                creator["affiliations"] = affs
            creators.append(creator)

        # publisher
        if not publisher:
            publisher = "Materials Data Facility"

        # publicationYear
        try:
            publication_year = str(int(publication_year))
        except (ValueError, TypeError):
            publication_year = str(datetime.now().year)

        # resourceType
        if not resource_type:
            resource_type = "Dataset"

        dc = {
            "titles": titles,
            "creators": creators,
            "publisher": publisher,
            "publicationYear": publication_year,
            "resourceType": {
                "resourceTypeGeneral": "Dataset",
                "resourceType": resource_type
            }
        }

        # descriptions
        if description:
            dc["descriptions"] = [{
                "description": description,
                "descriptionType": "Other"
            }]

        # identifier
        if dataset_doi:
            dc["identifier"] = {
                "identifier": dataset_doi,
                "identifierType": "DOI"
            }

        # relatedIdentifiers
        if related_dois:
            if not isinstance(related_dois, list):
                related_dois = [related_dois]
            dc["relatedIdentifiers"] = [{
                "relatedIdentifier": doi,
                "relatedIdentifierType": "DOI",
                "relationType": "IsPartOf"
            } for doi in related_dois]

        # subjects
        if subjects:
            if not isinstance(subjects, list):
                subjects = [subjects]
            dc["subjects"] = [{
                "subject": sub
            } for sub in subjects]

        # misc
        if kwargs:
            dc = mdf_toolbox.dict_merge(dc, kwargs)

        self.dc = dc

    def set_acl(self, acl):
        """Set the Access Control List for your dataset.

        Arguments:
            acl (str or list of str): The Globus UUIDs of users or groups that
                    should be granted access to the dataset.
                    **Default:** The special keyword ``"public"``, which makes the dataset
                    visible to everyone.
        """
        if not isinstance(acl, list):
            acl = [acl]
        self.mdf["acl"] = acl

    def clear_acl(self):
        """Reset the ACL of your dataset to the default value ``["public"]``."""
        self.mdf.pop("acl", None)

    def set_source_name(self, source_name):
        """Set the source name for your dataset.

        Arguments:
            source_name (str): The desired source name. Must be unique for new datasets.
                    Please note that your source name will be cleaned when submitted to Connect,
                    so the actual ``source_name`` may differ from this value.
                    Additionally, the ``source_id`` (which is the ``source_name`` plus
                    version information) is required to fetch the status of a submission.
                    ``check_status()`` can handle this for you.
        """
        self.mdf["source_name"] = source_name

    def clear_source_name(self):
        """Remove a previously set source_name."""
        self.mdf.pop("source_name", None)

    def add_organizations(self, organizations):
        """Set the organizations for your dataset.

        Arguments:
            organizations (str or list of str): The organization(s) to add.
                    If the organization is not registered with MDF, it will be discarded.
                    Additional organizations may be added automatically.
        """
        if not isinstance(organizations, list):
            organizations = [organizations]
        if not self.mdf.get("organizations"):
            self.mdf["organizations"] = organizations
        else:
            self.mdf["organizations"].extend(organizations)

    def clear_organizations(self):
        """Clear all added organizations from the submission."""
        self.mdf.pop("organizations", None)

    def set_project_block(self, project, data):
        """Set the project block for your dataset.
        Intended only for use by members of an approved project.
        To delete a project block, call this method with ``data=None``.

        Arguments:
            project (str): The name of the project block.
            data (dict): The data for the project block.
        """
        try:
            json.dumps(data, allow_nan=False)
        except Exception as e:
            return "Your project block is invalid: {}".format(repr(e))
        if data:
            self.projects[project] = data
        else:
            self.projects.pop(project, None)

    def create_mrr_block(self, mrr_data):
        """Create the mrr block for your dataset.
        This helper should be more helpful in the future.

        Arguments:
            mrr_data (dict): The MRR schema-compliant metadata.
        """
        self.mrr = mrr_data

    def set_custom_block(self, custom_fields):
        """Set the custom block for your dataset.

        Arguments:
            custom_fields (dict): Custom field-value pairs for your dataset.
                    You may add descriptions of your fields by creating a new field
                    called ``[field]_desc`` with the string description inside, or by
                    calling ``set_custom_descriptions()``.
        """
        try:
            json.dumps(custom_fields, allow_nan=False)
        except Exception as e:
            return "Error: Your custom block is invalid: {}".format(repr(e))
        self.custom = custom_fields

    def set_custom_descriptions(self, custom_descriptions):
        """Add descriptions to your custom block.

        Arguments:
            custom_descriptions (dict): Custom field-description pairs for your dataset.
                Field names in this argument must match field names added by
                calling ``set_custom_block()``.
        """
        try:
            json.dumps(custom_descriptions, allow_nan=False)
        except Exception as e:
            return "Error: Your custom descriptions are invalid: {}".format(repr(e))
        for field, desc in custom_descriptions.items():
            self.custom[field+"_desc"] = desc

    def add_data_source(self, data_source):
        """Add a data source to your submission.
        Note that this method is cumulative, so calls do not overwrite previous ones.

        Arguments:
            data_source (str or list of str): The location(s) of the data.
                    These should be formatted with protocol.

                    Examples:
                        ``"https://example.com/path/data.zip"``
                        ``"https://www.globus.org/app/transfer?..."``
                        ``"globus://endpoint123/path/data.out"``

        """
        if not isinstance(data_source, list):
            data_source = [data_source]
        self.data_sources.extend(data_source)

    def clear_data_sources(self):
        """Clear all data sources added so far to your dataset."""
        self.data_sources = []

    def add_data_destination(self, data_destination):
        """Add a data destination to your submission.
        Note that this method is cumulative, so calls do not overwrite previous ones.

        Arguments:
            data_destination (str or list of str): The destination for the data.
                    Destinations must be Globus Endpoints, and formatted with protocol.

                    Example:
                        ``"globus://endpoint123/path/data.out"``
        """
        if not isinstance(data_destination, list):
            data_destination = [data_destination]
        self.data_destinations.extend(data_destination)

    def clear_data_destinations(self):
        """Clear all data destinations added so far to your dataset."""
        self.data_destinations = []

    def add_index(self, data_type, mapping, delimiter=None, na_values=None):
        """Add indexing instructions for your dataset.
        This method can be called multiple times for multiple data types,
        but multiple calls with the same data type will overwrite each other.

        Arguments:
            data_type (str): The type of data to apply to. Supported types are: ``json``, ``csv``,
                    ``yaml``, ``xml``,  ``excel``, and ``filename``.
            mapping (dict): The mapping of MDF fields to your data type's fields.
                    It is strongly recommended that you use "dot notation",
                    where nested JSON objects are represented with a period.

                    Examples::

                        {
                            "material.composition": "my_json.data.stuff.comp",
                            "dft.converged": "my_json.data.dft.convgd"
                        }
                        {
                            "material.composition": "csv_header_1",
                            "crystal_structure.space_group_number": "csv_header_2"
                        }

            delimiter (str): The character that delimits cells in a table.
                    Only applicable to tabular data.
                    **Default:** comma.
            na_values (str or list of str): Values to treat as N/A (not applicable/available).
                    Applies to all values.
                    **Default:** For tabular data, blank and space.
                    For other data, ``None`` (no N/A values).

        """
        # TODO: Additional validation
        try:
            json.dumps(mapping, allow_nan=False)
        except Exception as e:
            return "Error: Your mapping is invalid: {}".format(repr(e))
        index = {
            "mapping": mapping
        }
        if delimiter is not None:
            index["delimiter"] = delimiter
        if na_values is not None:
            if not isinstance(na_values, list):
                na_values = [na_values]
            index["na_values"] = na_values

        self.index[data_type] = index

    def clear_index(self):
        """Clear all indexing instructions set so far."""
        self.index = {}

    def set_conversion_config(self, config):
        """Set advanced configuration parameters for dataset conversion.
        These parameters are intended for advanced users and/or special-case datasets.

        Arguments:
            config (dict): The conversion configuration parameters.
        """
        try:
            json.dumps(config, allow_nan=False)
        except Exception as e:
            return "Error: Your conversion config is invalid: {}".format(repr(e))
        self.conversion_config = config

    def add_service(self, service, parameters=None):
        """Add a service for data submission.

        Arguments:
            service (str): The integrated service to submit your dataset to.
                    Connected services include:

                    * ``globus_publish`` (publication with DOI minting)
                    * ``citrine`` (industry-partnered machine-learning specialists)
                    * ``mrr`` (NIST Materials Resource Registry)

            parameters (dict): Optional, service-specific parameters.

                    * For ``globus_publish``:

                        * **collection_id** (*int*) - The collection for submission.
                            Overwrites ``collection_name``.
                        * **collection_name** (*str*) - The collection for submission.
                    * For ``citrine``:
                        * **public** (*bool*) - When ``True``, will make data public.
                          Otherwise, it is inaccessible.
        """
        if parameters is None:
            parameters = True
        self.services[service] = parameters

    def clear_services(self):
        """Clear all services added so far."""
        self.services = {}

    def add_tag(self, tag):
        """Add a tag or keyword to your dataset.
        Note that this method is cumulative, so calls do not overwrite previous ones.

        Note:
            Setting tags here is equivalent to setting tags in ``create_dc_block(subjects=...)``.
            This method exists only for convenience.

        Arguments:
            tag (str or list of str): The tag(s) to add.
        """
        if not isinstance(tag, list):
            tag = [tag]
        self.tags.extend(tag)

    def clear_tags(self):
        """Clear all tags added so far to your dataset."""
        self.tags = []

    def set_curation(self, curation):
        """Set the curation flag for this submission.

        Note:
            Normally, this flag is set automatically by an organization, and is not set
            manually by the dataset submitter.

        Arguments:
            curation (bool): When ``False``, the dataset will be processed normally.
                    When ``True``, the dataset must be approved in curation
                    before it will be ingested to MDF Search or any other service.
                    **Default:** ``False``
        """
        self.curation = curation

    def set_test(self, test):
        """Set the test flag for this dataset.

        Arguments:
            test (bool): When ``False``, the dataset will be processed normally.
                    When ``True``, the dataset will be processed, but submitted to
                    test/sandbox/temporary resources instead of live resources.
                    This includes the ``mdf-test`` Search index and MDF Test Publish collection.
                    **Default:** ``False``
        """
        self.test = test

    def get_submission(self):
        """Fetch the current state of your submission.

        Returns:
            *dict*: Your submission.
        """
        submission = {
            "dc": self.dc,
            "data_sources": self.data_sources,
            "test": self.test,
            "update": self.update
        }
        if self.mdf:
            submission["mdf"] = self.mdf
        if self.mrr:
            submission["mrr"] = self.mrr
        if self.custom:
            submission["custom"] = self.custom
        if self.projects:
            submission["projects"] = self.projects
        if self.data_destinations:
            submission["data_destinations"] = self.data_destinations
        if self.index:
            submission["index"] = self.index
        if self.conversion_config:
            submission["conversion_config"] = self.conversion_config
        if self.services:
            submission["services"] = self.services
        if self.tags:
            submission["tags"] = self.tags
        if self.curation:
            submission["curation"] = self.curation
        return submission

    def reset_submission(self):
        """Reset and clear metadata from your submission.

        Warning:
            **This action cannot be undone.**

        The last submission's source_id will also be cleared. If you want to use ``check_status``,
        you will be required to input the ``source_id`` manually.

        Returns:
            *dict*: The variables that are NOT cleared, which includes:
                    * **test**: (*bool*) - If the submission is a test submission or not.
                    * **service_location** (*str*) - The URL of the MDF Connect server in use.
        """
        self.dc = {}
        self.mdf = {}
        self.mrr = {}

        self.projects = {}

        self.set_custom_block({})
        self.set_conversion_config({})
        self.set_curation(False)

        self.clear_data_sources()
        self.clear_data_destinations()
        self.clear_index()
        self.clear_services()
        self.clear_tags()

        self.source_id = None

        return {
            "test": self.test,
            "service_location": self.service_loc
        }

    def submit_dataset(self, resubmit=False, submission=None, reset=False):
        """Submit your dataset to MDF Connect for processing.

        Arguments:
            resubmit (bool): If you wish to submit this dataset again, set this to ``True``.
                    If this is the first submission, leave this ``False``.
                    **Default:** ``False``
            submission (dict): If you have assembled the Connect metadata yourself,
                    you can submit it here. This argument supersedes any data
                    set through other methods.
                    **Default:** ``None``, to use method-assembled data.
            reset (bool): If True, will clear the old submission. The test flag will be preserved.
                    **IMPORTANT**: The ``source_id`` of the submission will not be saved if
                    this argument is ``True``. ``check_status`` will require you to
                    pass the ``source_id`` as an argument.
                    If ``False``, the submission will be preserved.
                    **Default:** ``False``

        Returns:
            *dict*: The submission information.
                * **success** (*bool*) - Whether the submission was successful.
                * **source_id** (*string*) - The ``source_id`` of your dataset,  which is also saved
                    in ``self.source_id``. The ``source_id`` is the ``source_name``
                    plus version information. In other words, the ``source_name`` is unique
                    to your dataset, and the ``source_id`` is unique to
                    your submission of the dataset.
                * **error** (*string*) - Error message, if applicable.
        """
        # If submission not supplied, get from stored values
        if not submission:
            # Ensure resubmit set if known resubmission
            if not resubmit and self.source_id:
                return {
                    'source_id': None,
                    'success': False,
                    'error': ("You have already submitted this dataset."
                              " Set resubmit=True to resubmit it")
                }
            self.update = resubmit
            submission = self.get_submission()

        # Check for required data
        if not submission["dc"] or not submission["data_sources"]:
            return {
                'source_id': None,
                'success': False,
                'error': "You must populate the dc and data blocks before submission."
            }
        # Validate JSON
        try:
            json.dumps(submission, allow_nan=False)
        except Exception as e:
            return {
                'source_id': None,
                'success': False,
                'error': "The submission JSON is invalid: {}".format(repr(e))
            }

        # Make the request
        headers = {}
        self.__authorizer.set_authorization_header(headers)
        res = requests.post(self.service_loc+self.convert_route,
                            json=submission, headers=headers)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            self.__authorizer.set_authorization_header(headers)
            res = requests.post(self.service_loc+self.convert_route,
                                json=submission, headers=headers)

        # Check for success
        error = None
        try:
            json_res = res.json()
        except Exception:
            if res.status_code < 300:
                error = "Error decoding {} response: {}".format(res.status_code, res.content)
            else:
                error = ("Error {}. MDF Connect may be experiencing technical"
                         " difficulties.").format(res.status_code)
        else:
            if res.status_code < 300:
                self.source_id = json_res["source_id"]
            else:
                error = "Error {} submitting dataset: {}".format(res.status_code, json_res)

        # Prepare the output
        source_id = self.source_id
        if reset:
            self.reset_submission()

        # Return results
        return {
            "source_id": source_id,
            "success": error is None,
            "error": error,
            "status_code": res.status_code
        }

    def check_status(self, source_id=None, raw=False):
        """Check the status of your submission.
        You may only check the status of your own submissions.

        Arguments:
            source_id (str): The ``source_id`` (``source_name`` + version information) of the
                    submission to check.
                    **Default:** ``self.source_id``
            raw (bool): When ``False``, will print a nicely-formatted status summary.
                    When ``True``, will return the full status result.
                    For direct human consumption, ``False`` is recommended.
                    **Default:** ``False``

        Returns:
            If ``raw`` is ``True``, *dict*: The full status result.
        """
        if not source_id and not self.source_id:
            print("Error: No dataset submitted")
            return None
        headers = {}
        self.__authorizer.set_authorization_header(headers)
        res = requests.get(self.service_loc+self.status_route+(source_id or self.source_id),
                           headers=headers)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            self.__authorizer.set_authorization_header(headers)
            res = requests.get(self.service_loc+self.status_route+(source_id or self.source_id),
                               headers=headers)

        try:
            json_res = res.json()
        except Exception as e:
            if raw:
                return {
                    "success": False,
                    "error": "{}: {}".format(e, res.content),
                    "status_code": res.status_code
                }
            elif res.status_code < 300:
                print("Error decoding {} response: {}".format(res.status_code, res.content))
            else:
                print("Error {}. MDF Connect may be experiencing technical"
                      " difficulties.".format(res.status_code))
        else:
            if raw:
                json_res["status_code"] = res.status_code
                return json_res
            elif res.status_code >= 300:
                print("Error {} fetching status: {}".format(res.status_code, json_res))
            else:
                print("\n", json_res["status"]["status_message"], "\nThis submission is ",
                      ("active." if json_res["status"]["active"] else "inactive."), sep="")

    def check_all_submissions(self, verbose=False, active=False, raw=False,
                              _admin_code=None):
        """Check the status of all of your submissions.

        Arguments:
            verbose (bool): When ``False``, will print a basic summary of your submissions.
                    When ``True``, will print the full status summary of each submission,
                    as if you called ``check_status()`` on each. Has no effect if raw is ``True``.
                    **Default:** ``False``
            active (bool): When ``False``, will print the status for all of your submissions.
                    When ``True``, will only print active submissions.
                    **Default:** ``False``
            raw (bool): When ``False``, will print your submissions' summaries.
                    When ``True``, will return the full status results.
                    For direct human consumption, ``False`` is recommended.
                    **Default:** ``False``
            _admin_code (str): *For MDF Connect administrators only,* a special function code.
                    Valid codes:

                        * ``all``: All submission statuses
                        * ``active``: All active submission statuses

                    Only MDF Connect administrators are allowed to use these codes.
                    **Default:** ``None``, the only valid value for non-admins.

        Returns:
            if raw is ``True``, *dict*: The full status results.
        """
        headers = {}
        self.__authorizer.set_authorization_header(headers)
        res = requests.get("{}{}{}".format(self.service_loc, self.all_status_route,
                                           (_admin_code or "")), headers=headers)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            self.__authorizer.set_authorization_header(headers)
            res = requests.get("{}{}{}".format(self.service_loc, self.all_status_route,
                                               (_admin_code or "")), headers=headers)

        try:
            json_res = res.json()
        except Exception as e:
            if raw:
                return {
                    "success": False,
                    "error": "{}: {}".format(e, res.content),
                    "status_code": res.status_code
                }
            elif res.status_code < 300:
                print("Error decoding {} response: {}".format(res.status_code, res.content))
            else:
                print("Error {}. MDF Connect may be experiencing technical"
                      " difficulties.".format(res.status_code))
        else:
            if raw:
                json_res["status_code"] = res.status_code
                json_res["submissions"] = [sub for sub in json_res["submissions"]
                                           if (not active or sub["active"])]
                return json_res
            elif res.status_code >= 300:
                print("Error {} fetching status: {}".format(res.status_code, json_res))
            else:
                if not verbose:
                    print()  # Newline, because non-verbose won't include one
                for sub in json_res["submissions"]:
                    if not active or sub["active"]:
                        if verbose:
                            # Same message as check_status() with extra spacing
                            print("\n\n", sub["status_message"], "\nThis submission is ",
                                  ("active." if sub["active"] else "inactive."), sep="")
                        else:
                            # Decide if submission failed/succeeded/in processing/etc.
                            if "F" in sub["status_code"]:
                                status_word = "Failed"
                            elif "P" in sub["status_code"]:
                                status_word = "Processing"
                            elif sub["status_code"][-1] == "S":
                                status_word = "Succeeded"
                            elif sub["status_code"][-1] == "X":
                                status_word = "Cancelled"
                            elif sub["status_code"][0] == "z":
                                status_word = "Not started"
                            elif "R" in sub["status_code"]:
                                status_word = "Retrying error"
                            else:
                                status_word = "Unknown"
                            print("{}: {} - {}".format(sub["source_id"],
                                                       ("Active" if sub["active"] else "Inactive"),
                                                       status_word))
