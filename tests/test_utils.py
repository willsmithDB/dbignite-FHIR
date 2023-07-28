from pyspark.sql import SparkSession
import os, re, pytest
from shutil import *

from chispa import *
from chispa.schema_comparer import *

from dbignite.omop.data_model import *
from dbignite.omop.utils import *
from dbignite.omop.schemas import *
from dbignite.fhir_mapping_model import FhirSchemaModel

REPO = os.environ.get("REPO", "dbignite")
BRANCH = re.sub(r"\W+", "", os.environ.get("BRANCH", "local_test"))

TEST_BUNDLE_PATH = "./sampledata/"
TEST_DATABASE = f"test_{REPO}_{BRANCH}"


@pytest.fixture
def get_entries_inline_json_df(spark_session):
    return FhirBundles(path=TEST_BUNDLE_PATH).loadEntries()


@pytest.fixture
def get_entries_df(spark_session):
    return FhirBundles(path=TEST_BUNDLE_PATH).loadEntries()


@pytest.fixture
def fhir_model():
    fhir_model = FhirBundles(path=TEST_BUNDLE_PATH)
    return fhir_model


@pytest.fixture
def cdm_model():
    cdm_model = OmopCdm(TEST_DATABASE)
    return cdm_model


class TestMappings:

    def test_default_fhir_schema_model_count(self):
        fhir_mapped_model = FhirSchemaModel()
        assert (
            len(fhir_mapped_model.list_keys()) >= 157
        ), f"Resource Count Error: Expected at least 157 FHIR resources saved in the mapping, actual number mapped: {len(fhir_mapped_model.list_keys())}"
        return True
    
    def test_us_core_fhir_schema_model_count(self):
        us_core_fhir_mapped_model = FhirSchemaModel.us_core_fhir_resource_mapping()
        assert (
            len(us_core_fhir_mapped_model.list_keys()) == 26
        ), f"Resource Count Error: Expected 26 FHIR resources saved in the mapping, actual number mapped: {len(us_core_fhir_mapped_model.list_keys())}"
        return True
      
    def test_custom_fhir_schema_model_count(self, resource_list: list[str]):
        assert(type(resource_list) == list), f"Resource List Error: Expected a list and received: {type(resource_list).__name__}"
        for resource in resource_list:
          assert(type(resource) == str),  f"Resource Error: Expected a list of strings and received an item of type: {type(resource).__name__}"
        custom_fhir_mapped_model = FhirSchemaModel.custom_fhir_resource_mapping(resource_list)
        assert (
            len(custom_fhir_mapped_model.list_keys()) == len(resource_list)
        ), f"Expected %i FHIR resources saved in the mapping, actual number mapped: {len(custom_fhir_mapped_model.list_keys())}"
        return True
    
    def test_fhir_schema_model_resource_types(self, mapping: dict[str, StructType]):
        for key in mapping.keys():
          assert (
              type(mapping[key]) == StructType
          ), f"Resource Type Error: Expected StructType and received a resource of type: {type(mapping[key]).__name__}"
        for resource in mapping:
          assert (
              type(resource) == str
          ), f"Resource Type Error: Expected string and received a resource of type: {type(resource).__name__}"
        return True

    # Pending dependency within CICD 
    # def test_save_FHIR_schemas(self):
    #     schema_path_name = "TEST_SCHEMAS"
    #     records_saved = save_FHIR_schemas(schema_path_name)

    #     print("Schemas saved successfully. Number of schemas = " + str(records_saved))
    #     assert (
    #         records_saved >= 157
    #     ), f"Expected at least 157 FHIR resources, actual number saved: {records_saved}"
        
    #     print("All records saved successfully. Removing test directory.")
    #     print(subprocess.run(["rm", "-rf", schema_path_name], capture_output=True))


class TestUtils:
    def test_setup(self):
        rmtree("./spark-warehouse/", ignore_errors=True)

    def test_entries_to_person(self, get_entries_df) -> None:
        person_df = entries_to_person(get_entries_df)
        assert person_df.count() == 3
        assert_schema_equality(person_df.schema, PERSON_SCHEMA, ignore_nullable=True)

    def test_entries_inline_json(self, spark_session):
        fhir = FhirBundles(
            defaultResource=FhirBundles().asInlineJsonSingleton,
            path="./sampledata/inline_records/",
        )
        assert fhir.loadEntries().count() == 29
        import json

        x = json.loads(fhir.loadEntries().take(1)[0]["entry_json"])
        assert x["resourceType"] == "Patient"
        assert x["gender"] == "male"
        assert x["birthDate"] == "1963-06-09"

    def test_entries_to_condition(self, get_entries_df) -> None:
        condition_df = entries_to_condition(get_entries_df)
        assert condition_df.count() == 103
        assert_schema_equality(
            condition_df.schema, CONDITION_SCHEMA, ignore_nullable=True
        )

    def test_entries_to_procedure_occurrence(self, get_entries_df) -> None:
        procedure_occurrence_df = entries_to_procedure_occurrence(get_entries_df)
        assert procedure_occurrence_df.count() == 119
        assert_schema_equality(
            procedure_occurrence_df.schema,
            PROCEDURE_OCCURRENCE_SCHEMA,
            ignore_nullable=True,
        )

    def test_entries_to_encounter(self, get_entries_df) -> None:
        encounter_df = entries_to_encounter(get_entries_df)
        assert encounter_df.count() == 128
        assert_schema_equality(
            encounter_df.schema, ENCOUNTER_SCHEMA, ignore_nullable=True
        )


class TestTransformers:
    def test_loadEntries(self, get_entries_df) -> None:
        assert get_entries_df.count() == 1872
        assert_schema_equality(
            get_entries_df.schema, JSON_ENTRY_SCHEMA, ignore_nullable=True
        )

    def test_fhir_bundles_to_omop_cdm(
        self, spark_session, fhir_model, cdm_model
    ) -> None:
        FhirBundlesToCdm().transform(fhir_model, cdm_model, True)
        tables = [
            t.tableName
            for t in spark_session.sql(f"SHOW TABLES FROM {TEST_DATABASE}").collect()
        ]

        assert TEST_DATABASE in cdm_model.listDatabases()
        assert PERSON_TABLE in tables
        assert CONDITION_TABLE in tables
        assert PROCEDURE_OCCURRENCE_TABLE in tables
        assert ENCOUNTER_TABLE in tables

        assert spark_session.table(f"{TEST_DATABASE}.person").count() == 3

    def test_omop_cdm_to_person_dashboard(
        self, spark_session, fhir_model, cdm_model
    ) -> None:
        transformer = CdmToPersonDashboard()
        person_dash_model = PersonDashboard()

        FhirBundlesToCdm().transform(fhir_model, cdm_model, True)
        CdmToPersonDashboard().transform(cdm_model, person_dash_model)
        person_dashboard_df = person_dash_model.summary()
        assert_schema_equality(
            CONDITION_SUMMARY_SCHEMA,
            person_dashboard_df.select("conditions").schema,
            ignore_nullable=True,
        )
