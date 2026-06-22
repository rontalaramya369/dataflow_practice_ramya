import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io.gcp.pubsub import ReadFromPubSub

from apache_beam.io.gcp.bigquery import WriteToBigQuery

import json



project_id = "project-8a611ce5-dc75-4904-b12"

subscription = f"projects/{project_id}/subscriptions/crypto-sub"

table_id = f"{project_id}:crypto_dataset.crypto_table"



class ParseJson(beam.DoFn):

    def process(self, element):

        record = json.loads(
            element.decode("utf-8")
        )

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



table_schema = {

    "fields":[

        {"name":"id","type":"INTEGER"},

        {"name":"name","type":"STRING"},

        {"name":"username","type":"STRING"},

        {"name":"email","type":"STRING"},

        {"name":"street","type":"STRING"},

        {"name":"suite","type":"STRING"},

        {"name":"city","type":"STRING"},

        {"name":"zipcode","type":"STRING"},

        {"name":"latitude","type":"STRING"},

        {"name":"longitude","type":"STRING"},

        {"name":"phone","type":"STRING"},

        {"name":"website","type":"STRING"},

        {"name":"company_name","type":"STRING"},

        {"name":"company_phrase","type":"STRING"},

        {"name":"company_bs","type":"STRING"},

        {"name":"ingestion_time","type":"TIMESTAMP"}

    ]

}



options = PipelineOptions(
    streaming=True
)


with beam.Pipeline(options=options) as p:


    (

        p

        | "Read From PubSub"

        >> ReadFromPubSub(
            subscription=subscription
        )

        | "Parse Json"

        >> beam.ParDo(ParseJson())

        | "Write To BigQuery"

        >> WriteToBigQuery(

            table=table_id,

            schema=table_schema,

            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED

        )

    )