import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io.gcp.pubsub import ReadFromPubSub

from apache_beam.io.gcp.bigquery import WriteToBigQuery

import json


project_id = "project-8a611ce5-dc75-4904-b12"

subscription = f"projects/{project_id}/subscriptions/crypto-sub"

table_id = f"{project_id}:crypto_dataset.crypto_table"


class ParseMessage(beam.DoFn):

    def process(self, element):

        record = json.loads(element.decode("utf-8"))

        yield {

            "name": record.get("name"),

            "official_name": record.get("official_name"),

            "capital": record.get("capital"),

            "region": record.get("region"),

            "subregion": record.get("subregion"),

            "population": record.get("population"),

            "area": record.get("area"),

            "independent": record.get("independent"),

            "cca2": record.get("cca2"),

            "cca3": record.get("cca3"),

            "status": record.get("status"),

            "un_member": record.get("un_member"),

            "ingestion_time": record.get("ingestion_time")

        }


schema = """

name:STRING,

official_name:STRING,

capital:STRING,

region:STRING,

subregion:STRING,

population:INTEGER,

area:FLOAT,

independent:BOOLEAN,

cca2:STRING,

cca3:STRING,

status:STRING,

un_member:BOOLEAN,

ingestion_time:TIMESTAMP

"""


pipeline_options = PipelineOptions(

    streaming=True,

    project=project_id,

    save_main_session=True

)


with beam.Pipeline(options=pipeline_options) as pipeline:

    (

        pipeline

        | "Read From PubSub"

        >> ReadFromPubSub(subscription=subscription)

        | "Parse Json"

        >> beam.ParDo(ParseMessage())

        | "Write To BigQuery"

        >> WriteToBigQuery(

            table=table_id,

            schema=schema,

            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED

        )

    )