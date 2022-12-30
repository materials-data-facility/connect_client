from datetime import datetime
from typing import Any, Tuple, Dict, List
from uuid import uuid4
import json
import os

import globus_sdk
import mdf_toolbox
from nameparser import HumanName
import requests
import urllib

from .version import __version__

CONNECT_SERVICE_LOC = "https://api.materialsdatafacility.org"
CONNECT_DEV_LOC = "https://f6avec0img.execute-api.us-east-1.amazonaws.com/test"
CONNECT_EXTRACT_ROUTE = "/submit"
CONNECT_STATUS_ROUTE = "/status/"
CONNECT_ALL_STATUS_ROUTE = "/submissions/"
CONNECT_CURATION_ROUTE = "/curate/"
CONNECT_ALL_CURATION_ROUTE = "/curation/"
CONNECT_MD_UPDATE_ROUTE = "/update/"
CURATION_SUMMARY_STR = ("{source_id} by {submitter}\nWaiting since {waiting_since}"
                        "\n{extraction_summary}\n")
DEFAULT_CURATION_REASONS = {
    "accept": "This submission has been accepted because it meets the appropriate standards",
    "reject": ("This submission has been rejected because it does not meet the "
               "appropriate standards")
}
FILE_UPLOAD_SERVICES = ["transfer", "openid",
    "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",  # funcx
    "https://auth.globus.org/scopes/f10a69a9-338c-4e5b-baa1-0dc92359ab47/https",  # Eagle HTTPS
    "https://auth.globus.org/scopes/82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/https",  # NCSA HTTPS
]


class MDFConnectClient:
    """The MDF Connect Client is the Python client to easily submit datasets to MDF Connect."""
    __app_name = "MDF_Connect_Client"
    __client_id = "fcb9bf5a-4492-4e25-970f-510b69abc964"
    __login_services = ["mdf_connect", "mdf_connect_dev"]
    __allowed_authorizers = [
        globus_sdk.AccessTokenAuthorizer,
        globus_sdk.RefreshTokenAuthorizer,
        globus_sdk.ClientCredentialsAuthorizer,
        globus_sdk.NullAuthorizer
    ]

    def __init__(self, test=False, service_instance=None, authorizer=None, confidential=False, client_secret=None):
        """Create an MDF Connect Client.

        Arguments:
            test (bool): When ``False``, the dataset will be processed normally.
                    When ``True``, the dataset will be processed, but submitted to
                    test/sandbox/temporary resources instead of live resources.
                    This includes the ``mdf-test`` Search index and test DOIs minted
                    with MDF Publish.
                    **Default:** ``False``
            service_instance (str): The instance of the MDF Connect API to use.
                    This value should not normally be changed from the default.
                    **Default:** ``None``, to use the default API instance.
            authorizer (globus_sdk.GlobusAuthorizer): The authorizer to use for authentication.
                    This value should not normally be changed from the default.
                    **Default:** ``None``, to run the standard authentication flow.
            confidential (bool): When ``True``, log in to Globus services as a confidential client
                    (a client with its own login information, i.e. NOT a human's account).
                    **Default:** ``False``, to run the standard authentication flow.
            client_secret (str): Client secret to use when performing a confidential login.
                    Required when performing a confidential login.
                    **Default:** ``None``, because it is unnecessary otherwise.

        Returns:
            *MDFConnectClient*: An initialized, authenticated MDF Connect Client.
        """
        self.test = test
        self.update = False
        if (service_instance == "prod" or service_instance == "production"
                or service_instance is None):
            self.service_loc = CONNECT_SERVICE_LOC
        elif service_instance == "dev" or service_instance == "development":
            self.service_loc = CONNECT_DEV_LOC
        else:
            raise ValueError("'service_instance' must be 'prod' or 'dev', not '{}'"
                             .format(service_instance))
        self.extract_route = CONNECT_EXTRACT_ROUTE
        self.status_route = CONNECT_STATUS_ROUTE
        self.all_status_route = CONNECT_ALL_STATUS_ROUTE
        self.curation_route = CONNECT_CURATION_ROUTE
        self.all_curation_route = CONNECT_ALL_CURATION_ROUTE
        self.curation_summary_template = CURATION_SUMMARY_STR
        self.default_curation_reasons = DEFAULT_CURATION_REASONS
        self.md_update_route = CONNECT_MD_UPDATE_ROUTE

        self.reset_submission()
        login_service = "mdf_connect" if self.service_loc == CONNECT_SERVICE_LOC else "mdf_connect_dev"

        if any([isinstance(authorizer, allowed) for allowed in self.__allowed_authorizers]):
            self.__authorizer = authorizer
        else:
            perform_login = mdf_toolbox.login
            login_kwargs = {"services": self.__login_services+FILE_UPLOAD_SERVICES,
                            "client_id": self.__client_id,
                            "app_name": self.__app_name}
            if confidential:
                if client_secret is None:
                    raise ValueError(f"Unable to perform confidential login without client_secret")
                perform_login = mdf_toolbox.confidential_login
                login_kwargs["client_secret"] = client_secret
                del login_kwargs["app_name"]
            self.__auths = perform_login(**login_kwargs)
            self.__authorizer = self.__auths.get(login_service)
        if not self.__authorizer:
            raise ValueError("Unable to authenticate")

    def logout(self):
        """Log out by removing cached tokens and discarding the client's authorizer.
        Also clear the current submission, as it cannot be interacted with.
        """
        self.reset_submission()
        self.__authorizer = None
        if self.__auths: self.__auths = None
        mdf_toolbox.logout(client_id=self.__client_id, app_name=self.__app_name)
        return "Logged out. You must create a new MDF Connect Client to log back in."

    @property
    def version(self):
        return __version__

    # ***********************************************
    # * Mandatory inputs
    # ***********************************************

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
            authors (str or list of str): The author(s) of the dataset.
                    The name will be automatically parsed into given name and family name.
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
            name = HumanName(auth)
            given = "{} {}".format(name.first, name.middle).strip()
            family = "{} {}".format(name.last, name.suffix).strip()
            creator = {
                "creatorName": "{}, {}".format(family, given).strip(" ,"),
                "familyName": family,
                "givenName": given
            }
            if not isinstance(affs, list):
                affs = [affs]
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

    # ***********************************************
    # * Recommended inputs
    # ***********************************************

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

    def add_service(self, service, parameters=None):
        """Add a service for data submission.

        Arguments:
            service (str): The integrated service to submit your dataset to.
                    Connected services include:

                    * ``mdf_publish`` (publication with DOI minting)
                    * ``citrine`` (industry-partnered machine-learning specialists)
                    * ``mrr`` (NIST Materials Resource Registry)

            parameters (dict): Optional, service-specific parameters.

                    * For ``mdf_publish``:
                        * **publication_location** (*str*) - The Globus Endpoint
                            and path on which to save the published files.
                            It is recommended to not specify this parameter,
                            which causes the dataset to be published on MDF resources.

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

    def set_test(self, test):
        """Set the test flag for this dataset.

        Arguments:
            test (bool): When ``False``, the dataset will be processed normally.
                    When ``True``, the dataset will be processed, but submitted to
                    test/sandbox/temporary resources instead of live resources.
                    This includes the ``mdf-test`` Search index and test DOIs minted
                    with MDF Publish.
                    **Default:** ``False``
        """
        self.test = test

    def add_organization(self, organization):
        """Add your dataset to an organization.

        Arguments:
            organization (str or list of str): The organization(s) to add.
                    If the organization is not registered with MDF, it will be discarded.
                    Parent organizations will be added automatically.
        """
        if not isinstance(organization, list):
            organization = [organization]
        if not self.mdf.get("organizations"):
            self.mdf["organizations"] = organization
        else:
            self.mdf["organizations"].extend(organization)

    def clear_organizations(self):
        """Clear all added organizations from the submission."""
        self.mdf.pop("organizations", None)

    def add_links(self, links):
        """Add links to a dataset.

        Arguments:
            link (str or list of str): The link(s) to add.
                   Should be of the form {"type":str, "doi":str, "url":str, "description":str, "bibtex":str}
        """
        if not isinstance(links, list):
            links = [links]
        if not self.mdf.get("links"):
            self.mdf["links"] = links

    def clear_links(self):
        """Clear all tags added so far to your dataset."""
        self.links = []

    # ***********************************************
    # * Optional inputs
    # ***********************************************

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

    def set_base_acl(self, acl):
        """Set the Access Control List for your entire dataset.

        Arguments:
            acl (str or list of str): The Globus UUIDs of users or groups that
                    should be granted full read access to the dataset, including records and files.
                    **Default:** The special keyword ``"public"``, which makes the dataset
                    visible to everyone.

        Warning:
            The identities listed in the `base_acl` of your submission can always see
            your submission, including dataset entry, even if they are not listed in
            the ``dataset_acl``. This means that **if you do not specify a ``base_acl``**,
            because it defaults to `"public"`, **your entire dataset will be public.**
            MDF encourages you to make your data public, but if you do not want it public
            you must specify this value.
        """
        if not isinstance(acl, list):
            acl = [acl]
        self.mdf["acl"] = acl

    def clear_base_acl(self):
        """Reset the base ACL of your dataset to the default value ``["public"]``."""
        self.mdf.pop("acl", None)

    def set_dataset_acl(self, acl):
        """Set the Access Control List for just the dataset entry of your dataset.

        Arguments:
            acl (str or list of str): The Globus UUIDs of users or groups that
                    should be granted read access only to the dataset entry for your dataset
                    in MDF Search (this includes the author list, title, etc. but
                    does not include extracted metadata in records or files).
                    Anyone listed in the base ACL already has this permission.
        """
        if not isinstance(acl, list):
            acl = [acl]
        self.dataset_acl = acl

    def clear_dataset_acl(self):
        """Remove all Globus UUIDs from the dataset ACL for your dataset."""
        self.dataset_acl = None

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

    def set_incremental_update(self, source_id):
        """Make this submission an incremental update of a previous submission.
        Incremental updates use the same submission metadata, except for whatever you
        specify in the new submission. For example, if you submit an incremental update
        and only include a ``data_source``, the submission will run as if you copied the
        DC block and other metadata into the submission, but with the new ``data_source``.

        Note:
            You must still set ``update=True`` when submitting an incremental update.

        Arguments:
            source_id (str): The ``source_id`` of the previous submission to update.
        """
        self.incremental_update = source_id

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

    def set_external_uri(self, uri):
        """Set an external URI for your dataset. This is used to point at
        a landing page outside of MDF that also hosts the dataset.

        Arguments:
            uri (str): The external URI.
        """
        self.external_uri = uri

    def clear_external_uri(self):
        """Remove any set external URI from your submission."""
        self.external_uri = None

    def create_mrr_block(self, mrr_data):
        """Create the mrr block for your dataset.
        This helper should be more helpful in the future.

        Arguments:
            mrr_data (dict): The MRR schema-compliant metadata.
        """
        self.mrr = mrr_data

    # ***********************************************
    # * Advanced inputs
    # ***********************************************

    def set_passthrough(self, passthrough):
        """Set the dataset pass-through flag for your submission.

        Caution:
            This flag will cause metadata from your dataset's files to not be extracted by
            MDF Connect, so only high-level dataset metadata will be available in MDF Search.
            *This flag is only intended for datasets that cannot be extracted.*

        Arguments:
            passthrough (bool): When ``False``, the dataset will be processed normally.
                    When ``True``, the metadata in the files will not be extracted.
                    **Default:** ``False``
        """
        self.no_extract = passthrough

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

    def set_extraction_config(self, config):
        """Set advanced configuration parameters for dataset extraction.
        These parameters are intended for advanced users and/or special-case datasets.

        Arguments:
            config (dict): The extraction configuration parameters.
        """
        try:
            json.dumps(config, allow_nan=False)
        except Exception as e:
            return "Error: Your extraction config is invalid: {}".format(repr(e))
        self.extraction_config = config

    # ***********************************************
    # * Dataset submission
    # ***********************************************

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
        if self.external_uri:
            submission["external_uri"] = self.external_uri
        if self.index:
            submission["index"] = self.index
        if self.extraction_config:
            submission["extraction_config"] = self.extraction_config
        if self.services:
            submission["services"] = self.services
        if self.tags:
            submission["tags"] = self.tags
        if self.links:
            submission["links"] = self.links
        if self.curation:
            submission["curation"] = self.curation
        if self.no_extract:
            submission["no_extract"] = self.no_extract
        if self.dataset_acl:
            submission["dataset_acl"] = self.dataset_acl
        if self.incremental_update:
            submission["incremental_update"] = self.incremental_update
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
        self.set_extraction_config({})
        self.set_curation(False)
        self.set_passthrough(False)
        self.set_incremental_update(False)

        self.clear_data_sources()
        self.clear_external_uri()
        self.clear_data_destinations()
        self.clear_index()
        self.clear_services()
        self.clear_tags()
        self.clear_links()
        self.clear_dataset_acl()

        self.source_id = None

        return {
            "test": self.test,
            "service_location": self.service_loc
        }

    def submit_dataset(self, update=False, submission=None, reset=False):
        """Submit your dataset to MDF Connect for processing.

        Arguments:
            update (bool): If you wish to submit this dataset again, set this to ``True``.
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
            # Ensure update set if known resubmission
            if not update and self.source_id:
                return {
                    'source_id': None,
                    'success': False,
                    'error': ("You have already submitted this dataset."
                              " Set update=True to resubmit it")
                }
            self.update = update
            submission = self.get_submission()

        # Check for required data
        if ((not submission["dc"] or not submission["data_sources"])
                and not submission["incremental_update"]):
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
        headers["Authorization"] = self.__authorizer.get_authorization_header()
        res = requests.post(self.service_loc+self.extract_route,
                            json=submission, headers=headers)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            headers["Authorization"] = self.__authorizer.get_authorization_header()
            res = requests.post(self.service_loc+self.extract_route,
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
                error = ("Error {} submitting dataset: {}"
                         .format(res.status_code, json_res.get("error", json_res)))

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

    def submit_dataset_metadata_update(self, source_id, metadata_update=None, reset=False):
        """Submit an update to a dataset entry (and NOT the data or record entries).

        Arguments:
            source_id (str): The ``source_id`` of the dataset you wish to update.
                    You must be the owner of the dataset.
            metadata_update (dict): If you have assembled the dataset metadata yourself,
                    you can submit it here. This argument supersedes any data
                    set through other methods.
                    **Default:** ``None``, to use method-assembled data.
            reset (bool): If True, will clear the old metadata from the client.
                    The test flag will be preserved.
                    If ``False``, the metadata will be preserved.
                    **Default:** ``False``
        """
        if not metadata_update:
            metadata_update = self.get_submission()
        # Strip off submission pieces not used in update
        metadata_update.pop("data_sources", None)
        metadata_update.pop("test", None)
        metadata_update.pop("update", None)
        metadata_update.pop("data_destinations", None)
        metadata_update.pop("index", None)
        metadata_update.pop("extraction_config", None)
        metadata_update.pop("services", None)
        metadata_update.pop("curation", None)
        metadata_update.pop("no_extract", None)
        metadata_update.pop("incremental_update", None)

        # Validate JSON
        try:
            json.dumps(metadata_update, allow_nan=False)
        except Exception as e:
            return {
                'source_id': None,
                'success': False,
                'error': "The metadata update JSON is invalid: {}".format(repr(e))
            }

        # Make the request
        headers = {}
        headers["Authorization"] = self.__authorizer.get_authorization_header()
        res = requests.post(self.service_loc+self.md_update_route+source_id,
                            json=metadata_update, headers=headers)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            headers["Authorization"] = self.__authorizer.get_authorization_header()
            res = requests.post(self.service_loc+self.md_update_route+source_id,
                                json=metadata_update, headers=headers)

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
            if res.status_code >= 300:
                error = ("Error {} submitting dataset: {}"
                         .format(res.status_code, json_res.get("error", json_res)))

        if reset:
            self.reset_submission()

        # Return results
        return {
            "success": error is None,
            "error": error,
            "status_code": res.status_code
        }

    # ***********************************************
    # * Status checking
    # ***********************************************

    def check_status(self, source_id=None, short=False, raw=False):
        """Check the status of your submission.
        You may only check the status of your own submissions.

        Arguments:
            source_id (str): The ``source_id`` (``source_name`` + version information) of the
                    submission to check.
                    **Default:** ``self.source_id``
            short (bool): When ``False``, will print a status summary containing
                    all of the status steps for the dataset.
                    When ``True``, will print a short finished/processing message,
                    useful for checking many datasets' status at once.
                    **Default:** ``False``
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
        headers["Authorization"] = self.__authorizer.get_authorization_header()
        res = requests.get(self.service_loc+self.status_route+(source_id or self.source_id),
                           headers=headers)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            headers["Authorization"] = self.__authorizer.get_authorization_header()
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
            if json_res.get("status", {}).get("active"):
                active_msg = "This submission is still processing."
            else:
                active_msg = "This submission is no longer processing."
            if raw:
                json_res["status_code"] = res.status_code
                return json_res
            elif res.status_code >= 300:
                print("Error {} fetching status: {}".format(res.status_code,
                                                            json_res.get("error", json_res)))
            elif short:
                print("{}: {}".format((source_id or self.source_id), active_msg))
            else:
                print("\n{}\n{}\n".format(json_res["status"]["status_message"], active_msg))

    def check_all_submissions(self, verbose=False, active_only=False, include_tests=True,
                              newer_than_date=None, older_than_date=None, raw=False,
                              filters=None, _admin_code=None):
        """Check the status of all of your submissions.

        Arguments:
            verbose (bool): When ``False``, will print a basic summary of your submissions.
                    When ``True``, will print the full status summary of each submission,
                    as if you called ``check_status()`` on each. Has no effect if raw is ``True``.
                    **Default:** ``False``
            active_only (bool): When ``True``, will only print active submissions.
                    **Default:** ``False``
            include_tests (bool): When ``False``, will only print non-test submissions.
                    **Default:** ``True``
            newer_than_date (datetime or tuple of ints): Exclude submissions made before
                    this date. Accepts a ``datetime`` object or ``(year, month, day)``
                    as integers. Comparisons are made in UTC.
                    **Default:**: ``None``, to set no maximum age.
            older_than_date: (datetime or tuple of ints): Exclude submissions made after
                    this date. Accepts a ``datetime`` object or ``(year, month, day)``
                    as integers. Comparisons are made in UTC.
                    **Default:**: ``None``, to set no minimum age.
            raw (bool): When ``False``, will print your submissions' summaries.
                    When ``True``, will return the full status results.
                    For direct human consumption, ``False`` is recommended.
                    **Default:** ``False``
            filters (list of tuples): **Advanced users only**
                    Filters to apply to the status database scan.
                    For a submission to be returned, all filters must match.
                    **Default:** ``None``.
                    Format: (field, operator, value)
                        field: The status field to filter on.
                        operator: The relation of field to value. Valid operators:
                                ^: Begins with
                                *: Contains
                                ==: Equal to (or field does not exist, if value is None)
                                !=: Not equal to (or field exists, if value is None)
                                >: Greater than
                                >=: Greater than or equal to
                                <: Less than
                                <=: Less than or equal to
                                []: Between, inclusive (requires a list of two values)
                                in: Is one of the values (requires a list of values)
                                    This operator effectively allows OR-ing '=='
                   value: The value of the field.
            _admin_code (str): *For MDF Connect administrators only,* a special function code.
                    Valid codes:

                        * ``all``: All submission statuses
                        * ``active``: All active submission statuses

                    Only MDF Connect administrators are allowed to use these codes.
                    **Default:** ``None``, the only valid value for non-admins.

        Note about date filtering:
                Days are compared in UTC, at exactly 0:00 (12:00am). This means that the two dates
                cannot be the same, as they would filter out all submissions not made at exactly
                0:00:00 on the chosen date. To see submissions made on a specific date, set the
                older_than filter one day away from the date in question.
                For example, to see submissions from Feb 11, 2020, use
                ``newer_than_date=(2020, 2, 11), older_than_date=(2020, 2, 12)``.

        Returns:
            if raw is ``True``, *dict*: The full status results.
        """
        if filters is None:
            filters = []
        if active_only:
            filters.append(("active", "==", True))
        if not include_tests:
            filters.append(("test", "==", False))

        # Date filters
        if newer_than_date is not None and not isinstance(newer_than_date, datetime):
            newer_than_date = datetime(*newer_than_date)
        if older_than_date is not None and not isinstance(older_than_date, datetime):
            older_than_date = datetime(*older_than_date)
        # Validate date filters if both present
        if newer_than_date is not None and older_than_date is not None:
            # Cannot be the same
            if newer_than_date == older_than_date:
                raise ValueError("Date filters cannot be the identical. To see submissions "
                                 "made on a specific date, set the older_than filter one day "
                                 "away from the date in question.\nFor example, to see "
                                 "submissions from Feb 11, 2020, use "
                                 "'newer_than_date=(2020, 2, 11), older_than_date=(2020, 2, 12)'.")
            elif newer_than_date > older_than_date:
                raise ValueError("newer_than_date must be before older_than_date")
        if newer_than_date:
            filters.append(("submission_time", ">=", newer_than_date.isoformat("T") + "Z"))
        if older_than_date:
            filters.append(("submission_time", "<=", older_than_date.isoformat("T") + "Z"))

        headers = {}
        headers["Authorization"] = self.__authorizer.get_authorization_header()
        body = {
            "filters": filters
        }
        url = self.service_loc + self.all_status_route + (_admin_code or "")
        res = requests.post(url, headers=headers, json=body)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            headers["Authorization"] = self.__authorizer.get_authorization_header()
            res = requests.post(url, headers=headers, json=body)

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
                print("Error {} fetching status: {}".format(res.status_code,
                                                            json_res.get("error", json_res)))
            else:
                if not verbose:
                    print()  # Newline, because non-verbose won't include one
                for sub in json_res["submissions"]:
                    if verbose:
                        # Same message as check_status() with extra spacing
                        if sub["active"]:
                            active_msg = "This submission is still processing."
                        else:
                            active_msg = "This submission is no longer processing."
                        print("\n\n", sub["status_message"], active_msg, sep="")
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
                                                   ("Processing" if sub["active"]
                                                    else "Not processing"), status_word))

    # ***********************************************
    # * Curation
    # ***********************************************

    def get_curation_task(self, source_id, summary=False, raw=False):
        """Get the content of a curation task.
        You must have curation permissions on the selected submission.

        Arguments:
            source_id (str): The ``source_id`` (``source_name`` + version information) of the
                    curation task. You can acquire this through
                    ``get_available_curation_tasks()``.
            summary (bool): When ``False``, will print the entire curation task,
                    including the verbose dataset entry and sample records.
                    When ``True``, will only print a summary of the task.
                    **Default:** ``False``
            raw (bool): When ``False``, will print the curation task.
                    When ``True``, will return a dictionary of the full result.
                    Overrides the value of ``summary``.
                    For direct human consumption, ``False`` is recommended.
                    **Default:** ``False``

        Returns:
            if raw is ``True``, *dict*: The full task results.
        """
        headers = {}
        headers["Authorization"] = self.__authorizer.get_authorization_header()
        res = requests.get(self.service_loc+self.curation_route+source_id, headers=headers)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            headers["Authorization"] = self.__authorizer.get_authorization_header()
            res = requests.get(self.service_loc+self.curation_route+source_id, headers=headers)

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
                print("Error {} fetching curation task: {}"
                      .format(res.status_code, json_res.get("error", json_res)))
            elif summary:
                task = json_res["curation_task"]
                print(self.curation_summary_template.format(
                    source_id=task["source_id"],
                    submitter=task["submission_info"]["submitter"],
                    waiting_since=task["curation_start_date"],
                    extraction_summary=task["extraction_summary"]))
            else:
                task = json_res["curation_task"]
                # TODO: Are the dataset and record entries human-useful?
                # task.pop("dataset")
                # task.pop("sample_records")
                print(json.dumps(task, indent=4, sort_keys=True))

    def get_available_curation_tasks(self, summary=True, raw=False, _admin_code=None):
        """Get all curation tasks available to you.

        Arguments:
            summary (bool): When ``False``, will print the entire curation task,
                    including dataset entry and sample records.
                    When ``True``, will only print a summary of the task.
                    Using the summary is recommended to find specific tasks to
                    get full task information on using ``get_curation_task()``.
                    **Default:** ``True``
            raw (bool): When ``False``, will print out summaries of your available
                    curation tasks. When ``True``, will return a dictionary containing
                    the results.
                    For direct human consumption, ``False`` is recommended.
                    **Default:** ``False``
            _admin_code (str): *For MDF Connect administrators only,* a special function code.
                    Valid codes:

                        * ``all``: All waiting curation tasks.

                    Only MDF Connect administrators are allowed to use these codes.
                    **Default:** ``None``, the only valid value for non-admins.

        Returns:
            if raw is ``True``, *dict*: The full task results.
        """
        headers = {}
        headers["Authorization"] = self.__authorizer.get_authorization_header()
        res = requests.get(self.service_loc+self.all_curation_route+(_admin_code or ""),
                           headers=headers)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            headers["Authorization"] = self.__authorizer.get_authorization_header()
            res = requests.get(self.service_loc+self.all_curation_route+(_admin_code or ""),
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
                print("Error {} fetching curation tasks: {}"
                      .format(res.status_code, json_res.get("error", json_res)))
            # Check that results were returned
            elif len(json_res["curation_tasks"]) < 1:
                print("You have no open curation tasks.")
            elif summary:
                print()  # Newline for spacing
                for task in json_res["curation_tasks"]:
                    print(self.curation_summary_template.format(
                        source_id=task["source_id"],
                        submitter=task["submission_info"]["submitter"],
                        waiting_since=task["curation_start_date"],
                        extraction_summary=task["extraction_summary"]))
            else:
                for task in json_res["curation_tasks"]:
                    # TODO: Are the dataset and record entries human-useful?
                    # task.pop("dataset")
                    # task.pop("sample_records")
                    print("========== {} ==========".format(task["source_id"]))
                    print(json.dumps(task, indent=4, sort_keys=True))
                    print("\n")  # Double newline

    def _complete_curation_task(self, source_id, verdict, reason, prompt=True, raw=False):
        """Complete a curation task by accepting or rejecting it.
        You must have curation permissions on the selected submission.

        Note:
            This method is intended to be used through ``accept_curation_submission()``
            and ``reject_curation_submission()``, as those methods are more explicit,
            although the internal logic is almost identical.

        Arguments:
            source_id (str): The ``source_id`` (``source_name`` + version information) of the
                    curation task. You can acquire this through
                    ``get_available_curation_tasks()``.
            verdict (str): "accept" or "reject" to accept or reject the submission.
            reason (str): The reason for accepting/rejecting this submission.
                    **Default:** ``None``, to use a generic reason.
            prompt (bool): When ``True``, will prompt the user to confirm action selection,
                    with a summary of the selected task.
                    When ``False``, will not require confirmation.
                    **Default:** ``True``.
            raw (bool): When ``False``, will print the result.
                    When ``True``, will return a dictionary of the full result.
                    For direct human consumption, ``False`` is recommended.
                    **Default:** ``False``

        Returns:
            if raw is ``True``, *dict*: The full task results.
        """
        # Validate verdict
        verdict = verdict.strip().lower()
        if verdict not in self.default_curation_reasons.keys():
            error = ("Verdict '{}' is invalid. Valid verdicts are: {}"
                     .format(verdict, self.default_curation_reasons.keys()))
            if raw:
                return {
                    "success": False,
                    "error": error
                }
            else:
                print(error)
                return
        # Check that curation task exists
        task_json = self.get_curation_task(source_id, raw=True)
        if task_json["status_code"] == 404:
            error = task_json.get("error", "Curation task not found")
            if raw:
                return {
                    "success": False,
                    "error": error
                }
            else:
                print(error)
                return
        elif task_json["status_code"] >= 300:
            default_error = "MDF Connect may be experiencing technical difficulties."
            error = ("Error {} fetching curation task: {}"
                     .format(task_json["status_code"], task_json.get("error", default_error)))
            if raw:
                return {
                    "success": False,
                    "error": error
                }
            else:
                print(error)
                return

        # Prompt user to confirm, if requested
        if prompt:
            print("Are you sure you want to {} the following submission?".format(verdict))
            self.get_curation_task(source_id, summary=True)
            prompt_response = input("\nConfirm {}ing submission [yes/no]: ".format(verdict))
            if prompt_response.strip().lower() != "yes":
                error = "Curation cancelled"
                if raw:
                    return {
                        "success": False,
                        "error": error
                    }
                else:
                    print(error)
                    return
            elif not reason:
                reason = input("\nWhat is the reason for {}ing this submission?\n\t"
                               .format(verdict)).strip()

        if not reason:
            reason = self.default_curation_reasons[verdict]

        # Submit verdict
        command = {
            "action": verdict,
            "reason": reason
        }
        headers = {}
        headers["Authorization"] = self.__authorizer.get_authorization_header()
        res = requests.post(self.service_loc+self.curation_route+source_id, headers=headers,
                            json=command)
        # Handle first 401/403 by regenerating auth headers
        if res.status_code == 401 or res.status_code == 403:
            self.__authorizer.handle_missing_authorization()
            headers["Authorization"] = self.__authorizer.get_authorization_header()
            res = requests.get(self.service_loc+self.curation_route+source_id, headers=headers,
                               json=command)

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
                print("Error {} fetching curation task: {}"
                      .format(res.status_code, json_res.get("error", json_res)))
            else:
                print("\n", json_res["message"], sep="")

    def accept_curation_submission(self, source_id, reason=None, prompt=True, raw=False):
        """Complete a curation task by accepting the submission.
        You must have curation permissions on the selected submission.

        Arguments:
            source_id (str): The ``source_id`` (``source_name`` + version information) of the
                    curation task. You can acquire this through
                    ``get_available_curation_tasks()``.
            reason (str): The reason for accepting this submission.
                    **Default:** ``None``, to use a generic acceptance reason.
            prompt (bool): When ``True``, will prompt the user to confirm action selection,
                    with a summary of the selected task.
                    When ``False``, will not require confirmation.
                    **Default:** ``True``.
            raw (bool): When ``False``, will print the result.
                    When ``True``, will return a dictionary of the full result.
                    For direct human consumption, ``False`` is recommended.
                    **Default:** ``False``

        Returns:
            if raw is ``True``, *dict*: The full task results.
        """
        return self._complete_curation_task(source_id, "accept", reason, prompt, raw)

    def reject_curation_submission(self, source_id, reason=None, prompt=True, raw=False):
        """Complete a curation task by rejecting the submission.
        You must have curation permissions on the selected submission.

        Arguments:
            source_id (str): The ``source_id`` (``source_name`` + version information) of the
                    curation task. You can acquire this through
                    ``get_available_curation_tasks()``.
            reason (str): The reason for rejecting this submission.
                    **Default:** ``None``, to use a generic rejection reason.
            prompt (bool): When ``True``, will prompt the user to confirm action selection,
                    with a summary of the selected task.
                    When ``False``, will not require confirmation.
                    **Default:** ``True``.
            raw (bool): When ``False``, will print the result.
                    When ``True``, will return a dictionary of the full result.
                    For direct human consumption, ``False`` is recommended.
                    **Default:** ``False``

        Returns:
            if raw is ``True``, *dict*: The full task results.
        """
        return self._complete_curation_task(source_id, "reject", reason, prompt, raw)

    def upload_to_endpoint(self, local_data_path: str, endpoint_id: str = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                           dest_parent: str = None, dest_child: str = None) -> Tuple[str, str]:
        """Upload local data to a Globus endpoint using HTTPS PUT requests. Data can be a folder or an individual file.
            Note that the ACL rule created in this method must later be deleted after the dataset is submitted to MDF.
        Args:
            local_data_path (str): Path to the local dataset to publish to Foundry via HTTPS. Creates an HTTPS PUT
                request to upload the data specified to a Globus endpoint (default is NCSA endpoint) before it is
                transferred to MDF.
            endpoint_id (str): Globus endpoint ID to upload the data to. Default is NCSA endpoint.

        Returns
        -------
        (str) Globus data source URL: URL pointing to the data on the Globus endpoint
        (str) rule_id: Globus ACL rule ID for the uploaded data. Used to delete the rule after the dataset is submitted
            to MDF.
        """
        # define upload destination
        dest_path = self._create_dest_folder(endpoint_id, parent_dir=dest_parent, child_dir=dest_child)
        # create new ACL rule (ie permission) for user to read/write to endpoint and path
        rule_id = ""  # self._create_access_rule(endpoint_id, dest_path)
        # upload data to endpoint
        globus_data_source = self._https_upload(local_data_path=local_data_path, dest_path=dest_path,
                                                endpoint_id=endpoint_id)
        return globus_data_source, rule_id

    def _create_dest_folder(self, endpoint_id: str, parent_dir: str = None, child_dir: str = None) -> str:
        """Create a destination folder for the data on a Globus endpoint
        Args:
            endpoint_id (str): A UUID designating the exact Globus endpoint. Can be obtained via the Globus Web UI or
                the SDK.
            parent_dir (str): Set to "/tmp" when default is None. The parent directory that all publications via HTTPS
                will be written to.
            child_dir (str): Set to a random UUID when default is None. The child directory that the data will be
                written to.
        Returns
        -------
            (str): Path on Globus endpoint to write to
        """
        transfer_client = self.__auths["transfer"]
        # use a random UUID for each dataset publication, unless specified otherwise
        if child_dir is None:
            child_dir = uuid4()  # the publication ID forms the name of the child directory
        if parent_dir is None:
            parent_dir = "/tmp"
        dest_path = os.path.join(parent_dir, str(child_dir))  # NOTE: must start and end with "/"

        try:
            transfer_client.operation_mkdir(endpoint_id=endpoint_id, path=dest_path)
        except globus_sdk.TransferAPIError as e:
            raise IOError(f"Error from Globus API while creating destination folder: {e.message}") from e
        return dest_path

    def _create_access_rule(self, endpoint_id: str, dest_path: str) -> str:
        """Create an ACL rule (ie permission) for the user to read/write to the given destination on a Globus endpoint
        Args:
            endpoint_id (str): A UUID designating the exact Globus endpoint. Can be obtained via the Globus Web UI or
                the SDK.
            dest_path (str): The path to the existing folder on the given Globus endpoint.
        Returns
        -------
            (str): The ID for the ACL rule (necessary to delete it in the future)
        """
        transfer_client = self.__auths["transfer"]
        auth_client = globus_sdk.AuthClient(authorizer=self.__auths["openid"])
        # get user info
        res = auth_client.oauth2_userinfo()
        user_id = res.data["sub"]  # get the user primary ID (based on primary email set in Globus)
        # create data blob needed to set new rule with Globus
        rule_data = {
            "DATA_TYPE": "access",
            "principal_type": "identity",
            "principal": user_id,
            "path": dest_path,
            "permissions": "rw",
        }
        # create new ACL rule (eg permission) for user to read/write to endpoint and path
        rule_id = None
        try:
            ret = transfer_client.add_endpoint_acl_rule(endpoint_id, rule_data)
            rule_id = ret["access_id"]  # rule_id is needed to delete the rule later
        except globus_sdk.TransferAPIError:
            pass  # NOTE: known issue where user can still write to endpoint if this fails
        return rule_id

    def _https_upload(self, local_data_path: str, dest_path: str = "/tmp",
                    endpoint_id: str = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec") -> str:
        """Upload a dataset via HTTPS to a Globus endpoint
        Args:
            local_data_path (str): The path to the local data to upload. Can be relative or absolute.
            dest_path (str): The path to the destination folder on the Globus endpoint. Default is "/tmp".
            endpoint_id (str): A UUID designating the exact Globus endpoint. Can be obtained via the Globus Web UI or
                the SDK. Default is the NCSA UUID "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec".
        Returns
        -------
            (str): Globus data source URL (ie the URL that points to the data on a Globus endpoint)
        """
        transfer_client = self.__auths["transfer"]
        # get URL for Globus endpoint location
        endpoint = transfer_client.get_endpoint(endpoint_id)  # gets info for NCSA endpoint
        https_base_url = endpoint["https_server"]

        # Submit data (folders of files or an independent file) to be written to endpoint
        if os.path.isdir(local_data_path):
            self._upload_folder(local_data_path, https_base_url, dest_path, endpoint_id)
        elif os.path.isfile(local_data_path):
            self._upload_file(local_data_path, https_base_url, dest_path, endpoint_id)
        else:
            raise IOError(f"Data path '{local_data_path}' is of unknown type")

        # return the data source URL for publication to MDF
        return self._make_globus_link(endpoint_id, dest_path)

    def _upload_folder(self, local_data_path: str, https_base_url: str, parent_dest_path: str, endpoint_id: str) \
            -> List[Dict[str, Any]]:
        """Upload a folder to a Globus endpoint using HTTPS
        Args:
            local_data_path (str): The path to the local data to upload. Can be relative or absolute.
            https_base_url (str): The URL for a given Globus endpoint.
            parent_dest_path (str): The path to the parent folder to be written to on the given endpoint. The contents
                of "local_data_path" will be written here, including subdirectories.
            endpoint_id (str): The UUID designating the exact Globus endpoint. Can be obtained via the Globus Web UI or
                the SDK. This must be the same endpoint pointed to by the https_base_url.
        Returns
        -------
            (list): A list of all the HTTPS PUT request results (dicts) from the uploads
        """
        transfer_client = self.__auths["transfer"]
        results = []
        # initialize destination path as the parent destination path
        dest_path = parent_dest_path

        # walk through each child directory in the designated local data folder
        for root, _, files in os.walk(local_data_path):
            # update destination path if we have walked into a child directory
            if root != local_data_path:
                # get the child directory relative path
                subpath = os.path.relpath(root, local_data_path)
                # update destination path to include child directories (ie subpaths)
                dest_path = os.path.join(parent_dest_path, subpath)
                # create child directories on endpoint
                try:
                    transfer_client.operation_mkdir(endpoint_id=endpoint_id, path=dest_path)
                except globus_sdk.TransferAPIError as e:
                    raise IOError(f"Error while creating child directory {dest_path}: {e.message}") from e
            # get local path to file to upload
            for filename in files:
                filepath = os.path.join(root, filename)
                # upload file to destination path on endpoint
                result = self._upload_file(filepath, https_base_url, dest_path, endpoint_id)
                results.append(result)
        return results

    def _upload_file(self, filepath: str, https_base_url: str, dest_path: str, endpoint_id: str) -> requests.Response:
        """Upload an individual file to a Globus endpoint using HTTPS PUT
        Args:
            filepath (str): The path to the local file to upload.
            https_base_url (str): The URL for a given Globus endpoint.
            dest_path (str): The path to the folder to be written to on the given endpoint.
            endpoint_id (str): The UUID designating the exact Globus endpoint. Can be obtained via the Globus Web UI or
                the SDK. This must be the same endpoint pointed to by the https_base_url.
        Returns
        -------
            (Response): The `requests` HTTPS response object from a PUT request
        """
        # lets you HTTPS to specific endpoint (NCSA endpoint by default)
        scope = f"https://auth.globus.org/scopes/{endpoint_id}/https"
        # Get the authorization header token (string for the headers dict) HTTPS upload
        auth_gcs = globus_sdk.AuthClient(authorizer=self.__auths[scope])
        header = auth_gcs.authorizer.get_authorization_header()

        # get Globus endpoint path to write to
        filename = os.path.split(filepath)[1]
        # need to strip out leading "/" in dest_path for join to work
        endpoint_dest = os.path.join(https_base_url, dest_path.lstrip("/"), filename)

        # upload via HTTPS as arbitrary binary content type
        with open(filepath, "rb") as f:
            reply = requests.put(
                endpoint_dest,
                data=f,
                headers={"Authorization": header, "Content-Type": "application/octet-stream"}
            )
        if reply.status_code != 200:
            raise IOError(f"Error on HTTPS PUT, got response {reply.status_code}: {reply.text}")
        # Return the response
        return reply

    def _make_globus_link(self, endpoint_id: str, path: str) -> str:
        """Create the Globus data source URL for a given datapath on an endpoint
        Args:
            endpoint_id (str): The UUID designating the exact Globus endpoint. Can be obtained via the Globus Web UI or
                the SDK.
            path (str): The path to the dataset folder on the given endpoint.
        Returns
        -------
            (str): The Globus data source URL (ie the URL that points to the data on a Globus endpoint)
        """
        # make sure the path has the "/"s encoded properly for a URL
        safe_path = urllib.parse.quote(path, safe="*")
        link = f"https://app.globus.org/file-manager?origin_id={endpoint_id}&origin_path={safe_path}"
        return link
