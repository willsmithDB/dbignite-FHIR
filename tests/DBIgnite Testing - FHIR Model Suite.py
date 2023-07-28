# Databricks notebook source
pip install git+https://github.com/willsmithDB/dbignite-FHIR@feature-FHIR-schema-dbignite-HLSSA-296

# COMMAND ----------

pip install pytest chispa

# COMMAND ----------

from tests.test_utils import *

testing = TestMappings()

custom_mapping = [
            "Goal",
            "Immunization",
            "Location",
            "Medication",
            "MedicationDispense",
            "MedicationRequest",
            "Observation",
            "Organization",
            "Patient",
            "Practitioner",
            "Procedure"
]


# COMMAND ----------

testing.test_default_fhir_schema_model_count()

# COMMAND ----------

testing.test_us_core_fhir_schema_model_count()

# COMMAND ----------

testing.test_custom_fhir_schema_model_count(custom_mapping)

# COMMAND ----------

default_mapping = FhirSchemaModel().fhir_resource_map
us_core_mapping = FhirSchemaModel.us_core_fhir_resource_mapping().fhir_resource_map
custom_mapping = FhirSchemaModel.custom_fhir_resource_mapping(custom_mapping).fhir_resource_map

# COMMAND ----------

testing.test_fhir_schema_model_resource_types(default_mapping)

# COMMAND ----------

testing.test_fhir_schema_model_resource_types(us_core_mapping)

# COMMAND ----------

testing.test_fhir_schema_model_resource_types(custom_mapping)
