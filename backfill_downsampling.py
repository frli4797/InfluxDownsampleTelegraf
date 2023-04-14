import time
from pathlib import Path

from influxdb_client import InfluxDBClient, Point
from configparser import ConfigParser


config = ConfigParser()
config.read('config.ini')
INTERVAL = config.getint("position", "interval_mins", fallback=5)
MAX = -525960  # 1 year

source_bucket = config.get("main", "source_bucket")
destination_bucket = config.get("main", "destination_bucket")

client = InfluxDBClient(url=config.get("main", "url"), token=config.get("main", "token"),
                        org=config.get("main", "org"), timeout=config.getint("main", "timeout"))

downsample_query = Path("query.flux").read_text()
# print(downsample_query)

query_api = client.query_api()

# We query Influxdb by relative times, we work from -5 minutes to -0 (now)
# we then decrease both of them by 5 minutes and do this until we reach 'MAX'
cur_stop = config.getint('position', 'start', fallback=0)
cur_start = cur_stop-INTERVAL

print("Starting to process position {} to {}...".format(cur_start, cur_stop))
while MAX < cur_start:
    t_start = time.perf_counter()
    downsample_1m = downsample_query.format(
        source_bucket=source_bucket,
        rel_start=cur_start,
        rel_stop=cur_stop,
        aggregate_interval='1m',
        destination_bucket=destination_bucket
    )

    # print(downsample_1m)
    print("Downsampling from {} to {}... ".format(cur_start, cur_stop), end='')

    result = query_api.query_raw(downsample_1m)

    # There's probably no more data if the result string is of length 0.
    result_length = len(result.data.decode("utf-8").strip())

    if MAX > cur_start or result_length <= 0:
        break

    cur_start += -INTERVAL
    cur_stop += -INTERVAL

    config.set('position', 'start', str(cur_stop))
    with open('config.ini', 'w') as f:
        config.write(f)

    t_end = time.perf_counter()
    t_elapsed = t_end - t_start
    print(f" done in {t_elapsed:.03f} secs.")
    # End of loop

print("\nFinished.")
