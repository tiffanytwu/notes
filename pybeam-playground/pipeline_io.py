import apache_beam as beam
import logging

from apache_beam.io import ReadFromBigQueryRequest
from apache_beam.io.gcp.bigquery import ReadAllFromBigQuery
from datetime import datetime, timedelta
from typing import Tuple

logger = logging.getLogger(__name__)


def dates_between_range_inclusive(element):
    (start_date, end_date) = element
    date_list = []
    start_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")

    diff = abs((start_date - end_date).days)

    for n in range(0, diff + 1):
        date_list.append(start_date.strftime("%Y%m%d"))
        start_date += timedelta(days=1)
    return date_list

  
class ReadFromBQDailyRange(beam.PTransform):
    def __init__(self, query: str, input_dates: Tuple):
        self.query = query
        self.input_dates = input_dates

    @staticmethod
    def bq_data_request(element, query):
        return ReadFromBigQueryRequest(query=query.format(partition_date=element))

    def expand(self, p):
        dates_requests = p | beam.Create([self.input_dates]) | beam.FlatMap(dates_between_range_inclusive)

        (dates_requests | beam.Map(lambda e: logging.info(f"partition date {e}")))

        return (
            dates_requests
            | "generate queries" >> beam.Map(ReadFromBQDailyRange.bq_data_request, self.query)
            | "load data" >> ReadAllFromBigQuery()
        )

