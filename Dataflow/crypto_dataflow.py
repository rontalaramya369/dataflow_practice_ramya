import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

import json



project_id = "project-8a611ce5-dc75-4904-b12"

subscription = "crypto-sub"

dataset = "crypto_dataset"

table = "crypto_table"



class ParseMessage(beam.DoFn):

    def process(self, element):

        record = json.loads(element.decode("utf-8"))

        yield {

            "id": record.get("id"),

            "name": record.get("name"),

            "username": record.get("username"),

            "email": record.get("email"),

            "street": record.get("street"),

            "suite": record.get("suite"),

            "city": record.get("city"),

            "zipcode": record.get("zipcode"),

            "latitude": record.get("latitude"),

            "longitude": record.get("longitude"),

            "phone": record.get("phone"),

            "website": record.get("website"),

            "company_name": record.get("company_name"),

            "company_phrase": record.get("company_phrase"),

            "company_bs": record.get("company_bs"),

            "ingestion_time": record.get("ingestion_time")

        }



pipeline_options = PipelineOptions(streaming=True)



with beam.Pipeline(options=pipeline_options) as p:



    (

        p

        | "Read PubSub"

        >> beam.io.ReadFromPubSub(

            subscription=f"projects/{project_id}/subscriptions/{subscription}"

        )



        | "Parse JSON"

        >> beam.ParDo(ParseMessage())



        | "Write BigQuery"

        >> beam.io.WriteToBigQuery(

            table=f"{project_id}:{dataset}.{table}",



            schema="""

            id:INTEGER,

            name:STRING,

            username:STRING,

            email:STRING,

            street:STRING,

            suite:STRING,

            city:STRING,

            zipcode:STRING,

            latitude:STRING,

            longitude:STRING,

            phone:STRING,

            website:STRING,

            company_name:STRING,

            company_phrase:STRING,

            company_bs:STRING,

            ingestion_time:TIMESTAMP

            """,



            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED

        )

    )