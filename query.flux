import "types"
fromBucket = "{source_bucket}"
toBucket = "{destination_bucket}"
//toBucket = "telegraf_" + string(v: task.every)

numeric_data =
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

numeric_data
    |> aggregateWindow(every: {aggregate_interval}, fn: mean, createEmpty: false)
    |> set(key: "aggregate", value: "mean")
    |> set(key: "rollup_interval", value: string(v: {aggregate_interval}))
    |> to(bucket: toBucket)
numeric_data
    |> aggregateWindow(every: {aggregate_interval}, fn: min, createEmpty: false)
    |> set(key: "aggregate", value: "min")
    |> set(key: "rollup_interval", value: string(v: {aggregate_interval}))
    |> to(bucket: toBucket)
numeric_data
    |> aggregateWindow(every: {aggregate_interval}, fn: max, createEmpty: false)
    |> set(key: "aggregate", value: "max")
    |> set(key: "rollup_interval", value: string(v: {aggregate_interval}))
    |> to(bucket: toBucket)
numeric_data
    |> aggregateWindow(every: {aggregate_interval}, fn: sum, createEmpty: false)
    |> set(key: "aggregate", value: "sum")
    |> set(key: "rollup_interval", value: string(v: {aggregate_interval}))
    |> to(bucket: toBucket)

//    |> set(key: "aggregate", value: "mean")
//    |> set(key: "rollup_interval", value: string(v: {aggregate_interval}))
//    |> to(bucket: "{destination_bucket}")