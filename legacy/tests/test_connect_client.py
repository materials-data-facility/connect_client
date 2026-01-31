from datetime import datetime

from mdf_toolbox import insensitive_comparison
import pytest

from mdf_connect_client import MDFConnectClient
from mdf_connect_client.mdfcc import CONNECT_SERVICE_LOC, CONNECT_DEV_LOC
from globus_sdk import NullAuthorizer

@pytest.fixture
def auths(mocker):
    return {"mdf_connect": NullAuthorizer(), "mdf_connect_dev": NullAuthorizer()}


def test_service_loc(auths):
    mdf1 = MDFConnectClient(authorizer=auths["mdf_connect"])
    assert mdf1.service_loc == CONNECT_SERVICE_LOC
    mdf2 = MDFConnectClient(service_instance="prod", authorizer=auths["mdf_connect"])
    assert mdf2.service_loc == CONNECT_SERVICE_LOC
    mdf3 = MDFConnectClient(service_instance="dev", authorizer=auths["mdf_connect_dev"])
    assert mdf3.service_loc == CONNECT_DEV_LOC

    with pytest.raises(ValueError):
        MDFConnectClient(service_instance="foobar", authorizer=auths["mdf_connect"])


def test_create_dc_block(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    # Full test, no lists
    mdf.create_dc_block(
        title="Connect Title",
        authors="Data Facility, Materials",
        affiliations="UChicago",
        publisher="Globus",
        publication_year="2017",
        resource_type="Dataset",
        description="This is a test",
        dataset_doi="10.555",
        related_dois="10.5555",
        subjects="Science",
        other=5,
    )
    assert mdf.dc == {
        "creators": [
            {
                "affiliations": ["UChicago"],
                "creatorName": "Data Facility, Materials",
                "familyName": "Data Facility",
                "givenName": "Materials",
            }
        ],
        "descriptions": [{"description": "This is a test", "descriptionType": "Other"}],
        "identifier": {"identifier": "10.555", "identifierType": "DOI"},
        "other": 5,
        "publicationYear": "2017",
        "publisher": "Globus",
        "relatedIdentifiers": [
            {
                "relatedIdentifier": "10.5555",
                "relatedIdentifierType": "DOI",
                "relationType": "IsPartOf",
            }
        ],
        "resourceType": {"resourceType": "Dataset", "resourceTypeGeneral": "Dataset"},
        "titles": [{"title": "Connect Title"}],
        "subjects": [{"subject": "Science"}],
    }
    # Full test, all lists
    mdf.create_dc_block(
        title=["Connect Title", "Other Title"],
        authors=["Data Facility, Materials", "Blaiszik, Ben", "Jonathon Gaff"],
        affiliations=["UChicago", "Argonne"],
        publisher="Globus",
        publication_year="2017",
        resource_type="Dataset",
        description="This is a test",
        dataset_doi="10.555",
        related_dois=["10.5555", "10.555-5555"],
        subjects=["Science", "Math"],
        other=5,
        list_other=["a", "b"],
    )
    assert mdf.dc == {
        "creators": [
            {
                "affiliations": ["UChicago", "Argonne"],
                "creatorName": "Data Facility, Materials",
                "familyName": "Data Facility",
                "givenName": "Materials",
            },
            {
                "affiliations": ["UChicago", "Argonne"],
                "creatorName": "Blaiszik, Ben",
                "familyName": "Blaiszik",
                "givenName": "Ben",
            },
            {
                "affiliations": ["UChicago", "Argonne"],
                "creatorName": "Gaff, Jonathon",
                "familyName": "Gaff",
                "givenName": "Jonathon",
            },
        ],
        "descriptions": [{"description": "This is a test", "descriptionType": "Other"}],
        "identifier": {"identifier": "10.555", "identifierType": "DOI"},
        "list_other": ["a", "b"],
        "other": 5,
        "publicationYear": "2017",
        "publisher": "Globus",
        "relatedIdentifiers": [
            {
                "relatedIdentifier": "10.5555",
                "relatedIdentifierType": "DOI",
                "relationType": "IsPartOf",
            },
            {
                "relatedIdentifier": "10.555-5555",
                "relatedIdentifierType": "DOI",
                "relationType": "IsPartOf",
            },
        ],
        "resourceType": {"resourceType": "Dataset", "resourceTypeGeneral": "Dataset"},
        "titles": [{"title": "Connect Title"}, {"title": "Other Title"}],
        "subjects": [{"subject": "Science"}, {"subject": "Math"}],
    }
    # Minimum test
    mdf.create_dc_block(
        title="Project One", authors=["Artemis Moonshot", "Landing, Apollo"]
    )
    assert mdf.dc == {
        "creators": [
            {
                "creatorName": "Moonshot, Artemis",
                "familyName": "Moonshot",
                "givenName": "Artemis",
            },
            {
                "creatorName": "Landing, Apollo",
                "familyName": "Landing",
                "givenName": "Apollo",
            },
        ],
        "publicationYear": str(datetime.now().year),
        "publisher": "Materials Data Facility",
        "resourceType": {"resourceType": "Dataset", "resourceTypeGeneral": "Dataset"},
        "titles": [{"title": "Project One"}],
    }


def test_acl(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    mdf.set_base_acl("12345abc")
    assert mdf.mdf == {"acl": ["12345abc"]}
    mdf.set_base_acl(["12345abc", "6789def"])
    assert mdf.mdf == {"acl": ["12345abc", "6789def"]}
    mdf.set_base_acl("public")
    assert mdf.mdf == {"acl": ["public"]}
    mdf.clear_base_acl()
    assert mdf.mdf.get("acl", None) is None

    mdf.set_dataset_acl("12345abc")
    assert mdf.dataset_acl == ["12345abc"]
    mdf.set_dataset_acl(["12345abc", "6789def"])
    assert mdf.dataset_acl == ["12345abc", "6789def"]
    mdf.set_dataset_acl("public")
    assert mdf.dataset_acl == ["public"]
    mdf.clear_dataset_acl()
    assert mdf.dataset_acl is None


def test_source_name(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    mdf.set_source_name("foo")
    assert mdf.mdf == {"source_name": "foo"}
    mdf.clear_source_name()
    assert mdf.mdf.get("source_name", None) is None


def test_organizations(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    mdf.set_organization("ANL")
    assert mdf.mdf["organization"] == "ANL"


def test_create_mrr_block(auths):
    # TODO: Update after helper is helpful
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    mdf.create_mrr_block({"a": "b"})
    assert mdf.mrr == {"a": "b"}


def test_set_custom_block(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    mdf.set_custom_block({"foo": "bar"})
    assert mdf.custom == {"foo": "bar"}
    # OOR floats not allowed
    res = mdf.set_custom_block({"foo": float("nan")})
    assert "Out of range float values are not JSON compliant" in res
    assert mdf.custom == {"foo": "bar"}
    # Clear block
    mdf.set_custom_block({})
    assert mdf.custom == {}


def test_set_custom_descriptions(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    mdf.set_custom_block({"foo": "bar"})
    mdf.set_custom_descriptions({"foo": "This is a foo"})
    assert mdf.custom == {"foo": "bar", "foo_desc": "This is a foo"}
    # OOR floats not allowed
    res = mdf.set_custom_descriptions({"foo": float("nan")})
    assert "Out of range float values are not JSON compliant" in res
    assert mdf.custom == {"foo": "bar", "foo_desc": "This is a foo"}


def test_set_project_block(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    mdf.set_project_block("proj1", {"foo": "bar"})
    assert mdf.projects == {"proj1": {"foo": "bar"}}
    # OOR floats not allowed
    res = mdf.set_project_block("proj2", {"foo": float("nan")})
    assert "Out of range float values are not JSON compliant" in res
    assert mdf.projects == {"proj1": {"foo": "bar"}}
    # Pop project
    mdf.set_project_block("proj1", None)
    assert mdf.projects == {}


def test_data(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    # data_sources
    mdf.add_data_source("https://example.com/path/data.zip")
    assert mdf.data_sources == ["https://example.com/path/data.zip"]
    mdf.add_data_source(
        [
            "https://www.globus.org/app/transfer?123",
            "globus://endpoint123/path/data.out",
        ]
    )
    assert mdf.data_sources == [
        "https://example.com/path/data.zip",
        "https://www.globus.org/app/transfer?123",
        "globus://endpoint123/path/data.out",
    ]
    mdf.clear_data_sources()
    assert mdf.data_sources == []

    # data_destinations
    mdf.add_data_destination("https://example.com/path/data.zip")
    assert mdf.data_destinations == ["https://example.com/path/data.zip"]
    mdf.add_data_destination(
        [
            "https://www.globus.org/app/transfer?123",
            "globus://endpoint123/path/data.out",
        ]
    )
    assert mdf.data_destinations == [
        "https://example.com/path/data.zip",
        "https://www.globus.org/app/transfer?123",
        "globus://endpoint123/path/data.out",
    ]
    mdf.clear_data_destinations()
    assert mdf.data_destinations == []


def test_index(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    # Mapping only
    mdf.add_index("json", mapping={"materials.composition": "my_json.data.stuff.comp"})
    assert mdf.index == {
        "json": {"mapping": {"materials.composition": "my_json.data.stuff.comp"}}
    }
    # With delim/na
    mdf.add_index(
        "csv",
        mapping={"materials.composition": "header1"},
        delimiter="#",
        na_values="zero",
    )
    assert mdf.index == {
        "json": {"mapping": {"materials.composition": "my_json.data.stuff.comp"}},
        "csv": {
            "mapping": {"materials.composition": "header1"},
            "delimiter": "#",
            "na_values": ["zero"],
        },
    }
    # Overwrite
    mdf.add_index(
        "csv", mapping={"crystal_structure.space_group_number": "csv_header_2"}
    )
    assert mdf.index == {
        "json": {"mapping": {"materials.composition": "my_json.data.stuff.comp"}},
        "csv": {"mapping": {"crystal_structure.space_group_number": "csv_header_2"}},
    }
    # Bad input
    res = mdf.add_index(
        "json", mapping={"crystal_structure.space_group_number": float("nan")}
    )
    assert "Out of range float values are not JSON compliant" in res
    assert mdf.index == {
        "json": {"mapping": {"materials.composition": "my_json.data.stuff.comp"}},
        "csv": {"mapping": {"crystal_structure.space_group_number": "csv_header_2"}},
    }
    # Clear
    mdf.clear_index()
    assert mdf.index == {}


def test_extraction_config(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    mdf.set_extraction_config({"group_by_dir": True})
    assert mdf.extraction_config == {"group_by_dir": True}
    # OOR floats not allowed
    res = mdf.set_extraction_config({"dirs": float("nan")})
    assert "Error: Your extraction config is invalid" in res
    assert mdf.extraction_config == {"group_by_dir": True}
    # Clear block
    mdf.set_extraction_config({})
    assert mdf.extraction_config == {}


def test_services(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    # No parameters
    mdf.add_service("citrine")
    assert mdf.services == {"citrine": True}
    # With parameters
    mdf.add_service("globus_publish", parameters={"collection_id": 5555})
    assert mdf.services == {"citrine": True, "globus_publish": {"collection_id": 5555}}
    # Cancelling
    mdf.add_service("citrine", False)
    assert mdf.services == {"citrine": False, "globus_publish": {"collection_id": 5555}}
    # Removing
    mdf.clear_services()
    assert mdf.services == {}


def test_tags(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    mdf.add_tag("foo")
    assert mdf.tags == ["foo"]
    mdf.add_tag(["bar", "baz"])
    assert mdf.tags == ["foo", "bar", "baz"]
    mdf.clear_tags()
    assert mdf.tags == []


def test_curation(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    assert mdf.curation is False
    mdf.set_curation(True)
    assert mdf.curation is True
    mdf.set_curation(False)
    assert mdf.curation is False


def test_set_test(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    assert mdf.test is False
    mdf.set_test(True)
    assert mdf.test is True
    mdf.set_test(False)
    assert mdf.test is False
    mdf2 = MDFConnectClient(test=True, authorizer=auths["mdf_connect"])
    assert mdf2.test is True


def test_passthrough(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    assert mdf.no_extract is False
    mdf.set_passthrough(True)
    assert mdf.no_extract is True
    mdf.set_passthrough(False)
    assert mdf.no_extract is False


def test_submission(auths):
    mdf = MDFConnectClient(authorizer=auths["mdf_connect"])
    assert insensitive_comparison(
        mdf.get_submission(),
        {
            "dc": {},
            "data_sources": [],
            "mdf": {},
            "test": False,
            "update": False,
            "update_metadata_only": False,
        },
    )
    mdf.dc = {"a": "a"}
    mdf.mdf = {"b": "b"}
    mdf.services = {"c": "c"}
    mdf.projects = {"foo": {"bar": "baz"}}
    assert insensitive_comparison(
        mdf.get_submission(),
        {
            "dc": {"a": "a"},
            "mdf": {"b": "b"},
            "projects": {"foo": {"bar": "baz"}},
            "services": {"c": "c"},
            "data_sources": [],
            "test": False,
            "update": False,
            "update_metadata_only": False,
        },
    )

    mdf.reset_submission()
    assert insensitive_comparison(
        mdf.get_submission(),
        {"dc": {}, "mdf": {}, "data_sources": [], "test": False, "update": False, "update_metadata_only": False},
    )
    mdf.set_test(True)
    mdf.reset_submission()
    assert insensitive_comparison(
        mdf.get_submission(),
        {"dc": {}, "mdf": {},"data_sources": [], "test": True, "update": False, "update_metadata_only": False},
    )


# def test_submit_dataset():
#     # TODO
#     pass


# def test_check_status():
#     # TODO
#     pass


# def test_check_all_submissions():
#     # TODO
#     pass
