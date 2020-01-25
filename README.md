# Introduction

Nibbledb is a simple, light-weight time series database which uses [Irmin](https://github.com/mirage/irmin) to store data within Git. The goal is to run over a unikernel using [MirageOS](https://mirage.io/).

# Tutorial

## starting server

```bash
docker run -p 8000:8000 -it jptmoore/nibbledb /home/nibble/nibbledb --enable-tls
```

## adding data

Values are floats which can optionally be tagged with extra information. If no timestamp is provided it will be automatically generated as an epoch in millisecond precision.

```bash
curl -k --request POST --data '[{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}, {"value": 5}]' https://localhost:8000/ts/foo
curl -k --request POST --data '[{"value": 10}, {"value": 20}, {"value": 30}, {"value": 40}, {"value": 50}]' https://localhost:8000/ts/bar
curl -k --request POST --data '[{"colour":"red", "value": 1}, {"colour":"blue", "value": 2}, {"colour":"green", "value": 3}, {"colour":"red", "value": 4}, {"colour":"red", "value": 5}]' https://localhost:8000/ts/baz
curl -k --request POST --data '[{"timestamp":1, "value": 100}, {"timestamp":2, "value": 200}, {"timestamp":3, "value": 300}, {"timestamp":4, "value": 400}, {"timestamp":5, "value": 500}]' https://localhost:8000/ts/boz
```



## retrieving data

A typical request to obtain the last n values

```bash
curl -k https://localhost:8000/ts/foo/last/3
```

```json
[{"timestamp":1545232878575320,"data":{"value":5}},{"timestamp":1545232878575311,"data":{"value":4}},{"timestamp":1545232878575302,"data":{"value":3}}]
```

Finding the combined length of two time series

```bash
curl -k https://localhost:8000/ts/foo,bar/length
```

```json
{"length":10}
```

A range query from a specific point in time

```bash
curl -k https://localhost:8000/ts/boz/since/4
```

```json
[{"timestamp":5,"data":{"value":500}},{"timestamp":4,"data":{"value":400}}]
```

A query which filters the result based on matching tagged values and then applies an aggregation function

```bash
curl -k https://localhost:8000/ts/baz/last/5/filter/colour/equals/red/count
```

```json
{"count":3}
```

A query across a time range

```bash
curl -k https://localhost:8000/ts/boz/range/3/5
```

```json
[{"timestamp":5,"data":{"value":500}},{"timestamp":4,"data":{"value":400}},{"timestamp":3,"data":{"value":300}}]
```

Statistical analysis across different time series

```bash
curl -k https://localhost:8000/ts/foo,bar,baz,boz/last/5/sd
```

```json
{"sd":147.6999091329942}
```

Other functions that can be used in above example are: **sum**,**max**,**min**,**mean**,**median**,**count**.

## deleting data

The delete API supports range querying across multiple time series with support for filtering

```bash
curl -k --request DELETE https://localhost:8000/ts/baz/since/0/filter/colour/contains/re
curl -k https://localhost:8000/ts/baz/since/0
```

```json
[{"timestamp":1545235536095244,"data":{"colour":"blue","value":2}}]
```

## performance data

Finding the number of values of a time series held in memory

```bash
curl -k https://localhost:8000/ts/foo/memory/length
```

```json
{"length":5}
```

Finding the length of an index

```bash
curl -k https://localhost:8000/ts/foo/index/length
```

```json
{"length":0}
```

## maintenance

Flushing the memory of all time series to disk

```bash
curl -k https://localhost:8000/ts/sync
```

```bash
curl -k https://localhost:8000/ts/foo,bar,baz,boz/memory/length
```

```json
{"length":0}
```

