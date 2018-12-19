# nibbledb tutorial

## starting server

```bash
docker run -p 8000:8000 -v /tmp/nibble:/tmp/nibble -it jptmoore/nibbledb /home/nibble/nibbledb --enable-tls
```

## adding data

```bash
curl -k --request POST --data '[{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}, {"value": 5}]' https://localhost:8000/ts/foo
curl -k --request POST --data '[{"value": 10}, {"value": 20}, {"value": 30}, {"value": 40}, {"value": 50}]' https://localhost:8000/ts/bar
curl -k --request POST --data '[{"colour":"red", "value": 1}, {"colour":"blue", "value": 2}, {"colour":"green", "value": 3}, {"colour":"red", "value": 4}, {"colour":"red", "value": 5}]' https://localhost:8000/ts/baz
curl -k --request POST --data '[{"timestamp":1, "value": 100}, {"timestamp":2, "value": 200}, {"timestamp":3, "value": 300}, {"timestamp":4, "value": 400}, {"timestamp":5, "value": 500}]' https://localhost:8000/ts/boz
```



## retrieving data

```bash
curl -k https://localhost:8000/ts/foo,bar/length
```


```json
{"length":10}
```


```bash
curl -k https://localhost:8000/ts/foo/last/3
```


```json
[{"timestamp":1545232878575320,"data":{"value":5}},{"timestamp":1545232878575311,"data":{"value":4}},{"timestamp":1545232878575302,"data":{"value":3}}]
```


```bash
curl -k https://localhost:8000/ts/foo/since/1545232878575311
```


```json
[{"timestamp":1545232878575320,"data":{"value":5}},{"timestamp":1545232878575311,"data":{"value":4}}]
```

```bash
curl -k https://localhost:8000/ts/baz/last/5/filter/colour/equals/red/count
```

```json
{"count":3}
```

```bash
curl -k https://localhost:8000/ts/boz/range/3/5
```

```json
[{"timestamp":5,"data":{"value":500}},{"timestamp":4,"data":{"value":400}},{"timestamp":3,"data":{"value":300}}]
```

```bash
curl -k https://localhost:8000/ts/foo,bar,baz,boz/last/5/sd
```

```json
{"sd":147.6999091329942}
```

## deleting data

```bash
curl -k --request DELETE https://localhost:8000/ts/baz/since/0/filter/colour/contains/re
curl -k https://localhost:8000/ts/baz/since/0
```

```json
[{"timestamp":1545235536095244,"data":{"colour":"blue","value":2}}]
```