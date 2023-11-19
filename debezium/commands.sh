curl -X POST --location "http://localhost:8083/connectors" -H "Content-Type: application/json" -H "Accept: application/json" -d @debezium.json
curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" -d '{"name": "dwh_group", "format": "binary", "auto.offset.reset": "latest"}' http://localhost:8082/consumers/dwh_group

#curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" 127.0.0.1:8083/connector-plugins/PostgresConnector/config/validate --data "@debezium.json"
#curl http://localhost:8082/v3/clusters -o clusters.json
#curl http://localhost:8082/v3/clusters/hKYwT5GoSlapEIkACEV1Zw/topics -o topics.json

