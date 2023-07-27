# Databricks notebook source
# MAGIC %pip install pytest chispa
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

pip install git+https://github.com/willsmithDB/dbignite-FHIR.git@feature-FHIR-schema-dbignite-HLSSA-296

# COMMAND ----------

test = TestMappings()

# COMMAND ----------

test.test_save_FHIR_schemas()

# COMMAND ----------


import json
from pyspark.sql.types import *
import subprocess

def save_FHIR_schemas(schema_path_name: str = "schema"):
  schema_path_name = schema_path_name
  subprocess.run(["mkdir", "-p", schema_path_name], capture_output=True, text=True, input="y")

  count = 0

  for item in dbutils.fs.ls("dbfs:/user/hive/warehouse/json2spark-schema/spark_schemas/"):
      patient_schema = None
      file_location = "/tmp/" + item.name
      dbutils.fs.cp(
          "dbfs:/user/hive/warehouse/json2spark-schema/spark_schemas/" + item.name,
          "file://" + file_location,
      )
      with open(file_location) as new_file:
          patient_schema = json.load(new_file)
      with open(schema_path_name + "/" + item.name, "w") as new_file:
          new_file.write(json.dumps(patient_schema, indent = 4) )
      dbutils.fs.rm("./" + item.name)
      count += 1
      if count % 10 == 0:
        print("Progress: Successfully saved " + str(count) + " records.")
        
  return count

# COMMAND ----------

def test_save_FHIR_schemas():
  schema_path_name = "TEST_SCHEMAS"
  records_saved = save_FHIR_schemas(schema_path_name)

  print("Schemas saved successfully. Number of schemas = " + str(records_saved))  
  assert records_saved >= 157, f"Expected at least 157 FHIR resources, actual number saved: {records_saved}"
  print("All records saved successfully. Removing test directory.")
  print(subprocess.run(["rm", "-rf", schema_path_name], capture_output=True))
  


# COMMAND ----------

test_save_FHIR_schemas()

# COMMAND ----------

output = subprocess.run(["ls", "-p"], capture_output=True)
print(output.stdout.split())

# COMMAND ----------



# COMMAND ----------


