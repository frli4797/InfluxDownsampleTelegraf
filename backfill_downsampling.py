import time
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dateutil.parser
from influxdb_client import InfluxDBClient

config = ConfigParser()
config.read('config.ini')
INTERVAL = config.getint("position", "interval_mins", fallback=5)
MAX_1_Y = datetime.today().replace(tzinfo=timezone(timedelta(hours=2))) - timedelta(days=365)

source_bucket = config.get("main", "source_bucket")
destination_bucket = config.get("main", "destination_bucket")

client = InfluxDBClient(url=config.get("main", "url"), token=config.get("main", "token"),
                        org=config.get("main", "org"), timeout=config.getint("main", "timeout"))

downsample_query = Path("query.flux").read_text()

query_api = client.query_api()

position_str = config.get("position", "time")
position = datetime.today()
position = position.replace(tzinfo=timezone(timedelta(hours=2)))

try:
    position = dateutil.parser.isoparse(position_str)
except ValueError:
    print("Could not parse date. Using now().")

# now = datetime.today()
start = position - timedelta(minutes=INTERVAL)
print("From {} to {}.".format(start.isoformat(), position.isoformat()))

while MAX_1_Y < start:
    t_start = time.perf_counter()
    downsample = downsample_query.format(
        source_bucket=source_bucket,
        rel_start=start.isoformat(),
        rel_stop=position.isoformat(),
        aggregate_interval="1m",
        destination_bucket=destination_bucket
    )

    # print(downsample)
    print("Downsampling from {} to {}... ".format(start.isoformat(), position.isoformat()), end='')
    result = query_api.query_raw(downsample)

    # There's probably no more data if the result string is of length 0.
    result_length = len(result.data.decode("utf-8").strip())

    # if MAX_1_Y > start or result_length <= 0:
    if MAX_1_Y > start:
        print("Finished on length {} or {} > start {}.".format(result_length, MAX_1_Y, start))
        break

    position = position - timedelta(minutes=INTERVAL)
    start = start - timedelta(minutes=INTERVAL)

    config.set('position', 'time', position.isoformat())
    with open('config.ini', 'w') as f:
        config.write(f)

    t_end = time.perf_counter()
    t_elapsed = t_end - t_start
    print(f" done in {t_elapsed:.03f} secs.")
    # End of loop

print("\nFinished.")
