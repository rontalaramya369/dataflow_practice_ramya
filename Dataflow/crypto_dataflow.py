import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

import json

from datetime import datetime



PROJECT_ID='project-8a611ce5-dc75-4904-b12'

TOPIC='projects/project-8a611ce5-dc75-4904-b12/topics/crypto-topic'

TABLE='project-8a611ce5-dc75-4904-b12:realtime_demo.crypto_prices'



class ParseJson(beam.DoFn):

    def process(self, element):

        row=json.loads(element.decode('utf-8'))

        row['ingestion_time']=datetime.utcnow().isoformat()

        yield row



options=PipelineOptions(

    streaming=True,

    save_main_session=True

)



with beam.Pipeline(options=options) as p:


    (

        p

        | "ReadFromPubSub"

        >> beam.io.ReadFromPubSub(

            topic=TOPIC

        )


        | "ParseJson"

        >> beam.ParDo(ParseJson())


        | "WriteToBQ"

        >> beam.io.WriteToBigQuery(

            TABLE,


            schema='''

            coin:STRING,

            price_usd:FLOAT,

            last_updated:TIMESTAMP,

            ingestion_time:TIMESTAMP

            ''',


            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,


            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER

        )

    )