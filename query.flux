import "types"


fromBucket = "{source_bucket}"
toBucket = "{destination_bucket}"
every=1m

//fromBucket = "telegraf"
//toBucket = "telegraf_" + string(v: task.every)

all_data =
    from(bucket: fromBucket)
        |> range(start: {rel_start}, stop: {rel_stop})
        |> filter(fn: (r) => r._measurement =~ /^cpu|disk|diskio|mem|processes|swap|system$/)
numeric_data =
    all_data
        |> filter(fn: (r) => types.isType(v: r._value, type: "float") or types.isType(v: r._value, type: "int"))
        |> filter(fn: (r) => r._field !~ /^uptime_format$/)


numeric_data
    |> aggregateWindow(every: every, fn: mean)
    |> set(key: "aggregate", value: "mean")
    |> set(key: "rollup_interval", value: string(v: every))
    |> to(bucket: toBucket)

numeric_data
    |> aggregateWindow(every: every, fn: min)
    |> set(key: "aggregate", value: "min")
    |> set(key: "rollup_interval", value: string(v: every))
    |> to(bucket: toBucket)

numeric_data
    |> aggregateWindow(every: every, fn: max)
    |> set(key: "aggregate", value: "max")
    |> set(key: "rollup_interval", value: string(v: every))
    |> to(bucket: toBucket)

numeric_data
    |> aggregateWindow(every: every, fn: sum)
    |> set(key: "aggregate", value: "sum")
    |> set(key: "rollup_interval", value: string(v: every))
    |> to(bucket: toBucket)