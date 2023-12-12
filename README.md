# 🔥 dbignite
__Health Data Interoperability__

This library is designed to provide a low friction entry to performing analytics on 
[FHIR](https://hl7.org/fhir/bundle.html) bundles by extracting resources and flattening. 

# Usage

## Installation
```
pip install git+https://github.com/databrickslabs/dbignite.git
```
For a more detailed Demo, clone repo into Databricks and refer to the notebook [dbignite_patient_sample.py](./notebooks/dbignite_patient_sample.py)

## Usage: Read & Analyze a FHIR Bundle


This functionality exists in two components 

1. Representation of a FHIR schema and resources (see dbiginte/schemas, dbignite/fhir_mapping_model.py)
2. Interpretation of a FHIR bundle for consumable analtyics (see dbiginte/*py)

### 1. FHIR representations

``` python 
from  dbignite.fhir_mapping_model import FhirSchemaModel
fhir_schema = FhirSchemaModel()

#list all supported FHIR resources
sorted(fhir_schema.list_keys()) # ['Account', 'ActivityDefinition', 'ActorDefinition'...

#only use a subset of FHIR resources (built in CORE list)
fhir_core = FhirSchemaModel().us_core_fhir_resource_mapping()
sorted(fhir_core.list_keys()) # ['AllergyIntolerance', 'CarePlan', 'CareTeam', 'Condition', ...

#create your own custom resource list
fhir_custom = FhirSchemaModel().custom_fhir_resource_mapping(['Patient', 'Claim', 'Condition'])
sorted(fhir_custom.list_keys()) # ['Claim', 'Condition', 'Patient']

#create your own custom schema mapping (advanced usage, not recommended)
# ... FhirSchemaModel(fhir_resource_map = <your dictionary of resource to spark schema>)
```

### 2. FHIR interpretation for analytics

#### Summary Level FHIR Bundle Information

``` python
from pyspark.sql.functions import size,col, sum
from dbignite.readers import read_from_directory

#Read sample data from a public s3 bucket
sample_data = "s3://hls-eng-data-public/data/synthea/fhir/fhir/*json"
bundle = read_from_directory(sample_data)

#Read all the bundles and parse
bundle.entry

#Show the total number of patient resources in all bundles
bundle.count_resource_type("Patient").show() 
#+------------+                                                                  
#|resource_sum|
#+------------+
#|           3|
#+------------+
#

#Show number of patient resources within each bundle 
bundle.count_within_bundle_resource_type("Patient").show()
#+-------------------+
#|resource_bundle_sum|
#+-------------------+
#|                  1|
#|                  1|
#|                  1|
#+-------------------+

```

#### FHIR Bundle Representation in DBIgnite

The core of a  FHIR bundle is the list of entry resources. This information is flattened into individual columns grouped by resourceType in DBIgnite. The following examples depict common uses and interactions. 

![logo](/img/FhirBundleSchemaClass.png?raw=true)

#### Detailed Mapping Level FHIR Bundle Information (SQL API)

``` python
%python
#Save Claim and Patient data to a table
spark.sql("""DROP TABLE IF EXISTS hls_dev.default.claim""")
spark.sql("""DROP TABLE IF EXISTS hls_dev.default.patient""")

df = bundle.entry.withColumn("bundleUUID", expr("uuid()"))
( df
	.select(col("bundleUUID"), col("Claim"))
	.write.mode("overwrite")
	.saveAsTable("hls_dev.default.claim")
)

( df
	.select(col("bundleUUID"), col("Patient"))
	.write.mode("overwrite")
	.saveAsTable("hls_dev.default.patient")
)
```
``` SQL
%sql
-- Select claim line detailed information
select p.bundleUUID as UNIQUE_FHIR_ID, 
  p.Patient.id as patient_id,
  p.patient.birthDate,
  c.claim.patient as claim_patient_id, 
  c.claim.id as claim_id,
  c.claim.type.coding.code[0] as claim_type_cd, --837I = Institutional, 837P = Professional
  c.claim.insurance.coverage[0],
  c.claim.total.value as claim_billed_amount,
  c.claim.item.productOrService.coding.display as procedure_description,
  c.claim.item.productOrService.coding.code as procedure_code,
  c.claim.item.productOrService.coding.system as procedure_coding_system
from (select bundleUUID, explode(Patient) as patient from hls_dev.default.patient) p --all patient information
  inner join (select bundleUUID, explode(claim) as claim from hls_dev.default.claim) c --all claims from that patient 
    on p.bundleUUID = c.bundleUUID --Only show records that were bundled together
limit 100
```

#### Detailed Mapping Level FHIR Bundle Information (DataFrame API)

Perform same functionality above, except using Dataframe only

``` python
df = bundle.entry.withColumn("bundleUUID", expr("uuid()"))

df.select(explode("Patient").alias("Patient"), col("bundleUUID"), col("Claim")).select(col("Patient"), col("bundleUUID"), explode("Claim").alias("Claim")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("claim.patient").alias("claim_patient_id"),
  col("claim.id").alias("claim_id"),
  col("patient.birthDate").alias("Birth_date"),
  col("claim.type.coding.code")[0].alias("claim_type_cd"),
  col("claim.insurance.coverage")[0].alias("insurer"),
  col("claim.total.value").alias("claim_billed_amount"),
  col("claim.item.productOrService.coding.display").alias("prcdr_description"),
  col("claim.item.productOrService.coding.code").alias("prcdr_cd"),
  col("claim.item.productOrService.coding.system").alias("prcdr_coding_system")
).show()

```

#### Reading in non-comopliant FHIR Data

Medication details are saved in a non-compliant FHIR schema. Update the schema to read this information under "medicationCodeableConcept"

``` python
%python
med_schema = df.select(explode("MedicationRequest").alias("MedicationRequest")).schema
#Add the medicationCodeableConcept schema in
medCodeableConcept = StructField("medicationCodeableConcept", StructType([
              StructField("text",StringType()),
              StructField("coding", ArrayType(
                StructType([
                    StructField("code", StringType()),
                    StructField("display", StringType()),
                    StructField("system", StringType()),
                ])
              ))
    ]))

med_schema.fields[0].dataType.add(medCodeableConcept) #Add StructField one level below MedicationRequest

#create new schema for reading FHIR bundles
old_schemas = {k:v for (k,v) in FhirSchemaModel().fhir_resource_map.items() if k != 'MedicationRequest'}
new_schemas = {**old_schemas, **{'MedicationRequest': med_schema.fields[0].dataType} }

df = bundle.read_data( FhirSchemaModel(fhir_resource_map = new_schemas))

#select and show medication data
df = df.withColumn("bundleUUID", expr("uuid()"))

df.select(explode("Patient").alias("Patient"), col("bundleUUID"), col("MedicationRequest")).select(col("Patient"), col("bundleUUID"), explode(col("MedicationRequest")).alias("MedicationRequest")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("MedicationRequest.status"),
  col("MedicationRequest.intent"),
  col("MedicationRequest.authoredOn"),
  col("MedicationRequest.medicationCodeableConcept.text").alias("rx_text"),
  col("MedicationRequest.medicationCodeableConcept.coding.code")[0].alias("rx_code"),
  col("MedicationRequest.medicationCodeableConcept.coding.system")[0].alias("code_type")
).show()

```

#### Usage: Writing Data as a FHIR Bundle 

>  **Warning** 
> This section is under construction

#### Usage: Seeing a Patient in a Hospital in Real Time  

Patient flow is driven by ADT message headers (Admission, Discharge, & Transfers). We parse sample [data](./sampledata/adt_records) from this repo and associate "actions" associated with each message type.

Import ADT package & read FHIR data

``` python
import os, uuid
from dbignite.readers import read_from_directory
from dbignite.hosp_feeds.adt import ADTActions

#Side effect of creating the UDF to see actions from ADT messages 
#SELECT get_action("ADT") -> action : "discharge" , description : "transfer an inpatient to outpatient"
ADTActions()

sample_data = "file:///" + os.getcwd() + "/../sampledata/adt_records/"
bundle = read_from_directory(sample_data)
df = bundle.read_data()
```

Create relational tables for Patient and MessageHeader data types
 
``` sql
spark.sql("""DROP TABLE IF EXISTS hls_healthcare.hls_dev.patient""")
spark.sql("""DROP TABLE IF EXISTS hls_healthcare.hls_dev.adt_message""")
df.select(col("bundleUUID"), col("Patient")).write.mode("overwrite").saveAsTable("hls_healthcare.hls_dev.patient")
df.select(col("bundleUUID"), col("timestamp"), col("MessageHeader")).write.mode("overwrite").saveAsTable("hls_healthcare.hls_dev.adt_message")
```

Sample results and query used against relational tables above that shows patient flow in the hospital setting (fake data shown) 

[img](./img/patient_adt.png)

``` sql
Select 
--SSN value for patient matching
filter(patient.identifier, x -> x.system == 'http://hl7.org/fhir/sid/us-ssn')[0].value as ssn
,adt.timestamp as event_timestamp

--ADT action
,adt.messageheader.eventCoding.code as adt_type
,get_action(adt.messageheader.eventCoding.code).action as action
,get_action(adt.messageheader.eventCoding.code).description as description
,adt.messageheader.eventCoding.code
,adt.messageheader.eventCoding.system 

--Patient Resource Details 
,patient.name[0].given[0] as first_name
,patient.name[0].family as last_name
,patient.birthDate
,patient.gender
--Selecting Driver's license number identifier code='DL'
,filter(patient.identifier, x -> x.type.coding[0].code == 'DL')[0].value as drivers_license_id
--Master Patient Index Value for patient matching
,filter(patient.identifier, x -> x.type.text == 'EMPI')[0].value as empi_id

from (select timestamp, bundleUUID, explode(MessageHeader) as messageheader from hls_healthcare.hls_dev.adt_message) adt
  inner join (select bundleUUID, explode(Patient) as patient from hls_healthcare.hls_dev.patient) patient 
    on patient.bundleUUID = adt.bundleUUID
  order by ssn desc, timestamp desc
```

#### Usage: OMOP Common Data Model 

See [DBIgnite OMOP](dbignite/omop) for details 
