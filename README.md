# nibbledb tutorial

## starting server

```bash
$ docker run -p 8000:8000 -v /tmp/nibble:/tmp/nibble -it jptmoore/nibbledb /home/nibble/nibbledb --enable-tls
```

## adding data

```bash
$ curl -k --request POST --data '[{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}, {"value": 5}]' https://localhost:8000/ts/foo
```


```bash
$ curl -k --request POST --data '[{"value": 10}, {"value": 20}, {"value": 30}, {"value": 40}, {"value": 50}]' https://localhost:8000/ts/bar
```

## retrieving data

```bash
$ curl -k https://localhost:8000/ts/foo,bar/length
```


```json
{"length":10}
```


```bash
$ curl -k https://localhost:8000/ts/foo/last/3
```


```json
[{"timestamp":1545232878575320,"data":{"value":5}},{"timestamp":1545232878575311,"data":{"value":4}},{"timestamp":1545232878575302,"data":{"value":3}}]
```


```bash
$ curl -k https://localhost:8000/ts/foo/since/1545232878575311
```


```json
[{"timestamp":1545232878575320,"data":{"value":5}},{"timestamp":1545232878575311,"data":{"value":4}}]
```
