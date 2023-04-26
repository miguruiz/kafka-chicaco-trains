"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass, asdict

import faust
import json


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

topic = app.topic("chicago.rail.stations", value_type=Station)

out_topic = app.topic("chicago.rail.stations.clean", partitions=1)

table = app.Table(
   "stations-table",
   default=str,
   partitions=1,
   changelog_topic=out_topic,
)

def get_line_color(station):
    if station.red:
        return 'red'
    if station.green:
        return 'green'
    if station.blue:
        return 'blue'


@app.agent(topic)
async def process(stations_stream):

    async for station in stations_stream:
        print(json.dumps(asdict(station), indent=2))

        transformed_station = TransformedStation(
            station_id = station.station_id,
            station_name = station.station_name,
            order = station.order,
            line = get_line_color(station)
        )
        print(json.dumps(asdict(transformed_station), indent=2))
        await out_topic.send(key=transformed_station.station_name, value=transformed_station)


if __name__ == "__main__":
    app.main()
