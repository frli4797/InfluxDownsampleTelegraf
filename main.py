from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# from influxdb-client import InfluxDBClient, Point

MAX = -525960  # 1 year

source_bucket = 'telegraf'
bucket_1m = 'telegraf_experiment'

client = InfluxDBClient(url="http://unraid:8087", token="heUB_k87iPwAPZSOb79JXgzkCBFiOSbmKCmZIGZaxBpfIBoVzqes6tzeqJGDgfyK1Rn7wJUlKqZXZi3T0xsiig==", org="Lilja", timeout=30000)

downsample_query = '''import "types"
from(bucket: "{source_bucket}")
    |> range(start: {rel_start}m, stop: {rel_stop}m)
    |> filter(fn: (r) => r._measurement =~ /^cpu|disk|diskio|mem|processes|swap|system$/)
    |> filter(
        fn: (r) =>
            types.isType(v: r._value, type: "int") or types.isType(v: r._value, type: "uint") or types.isType(
                    v: r._value,
                    type: "float",
                ),
    )
    |> aggregateWindow(every: {aggregate_interval}, fn: mean, createEmpty: false)
    |> set(key: "aggregate", value: "mean")
    |> set(key: "rollup_interval", value: string(v: {aggregate_interval}))
    |> to(bucket: "{destination_bucket}")
'''

query_api = client.query_api()

# We query Influxdb by relative times, we work from -5 minutes to -0 (now)
# we then decrease both of them by 5 minutes and do this until we reach 'MAX'
cur_start = -5
cur_stop = -0
while True:
    downsample_1m = downsample_query.format(
        source_bucket=source_bucket,
        rel_start=cur_start,
        rel_stop=cur_stop,
        aggregate_interval='1m',
        destination_bucket=bucket_1m
    )

    print(downsample_1m)
    query_api.query_raw(downsample_1m)

    if MAX > cur_start:
        break

    cur_start += -5
    cur_stop += -5