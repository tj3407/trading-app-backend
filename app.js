const express = require("express");
const { Kafka } = require('kafkajs')
const bodyParser = require("body-parser");

const config = {
    kafka: {
      TOPIC: 'clicks',
      BROKERS: ['localhost:9092'],
      GROUPID: 'bills-consumer-group',
      CLIENTID: 'sample-kafka-client'
    }
}

const client = new Kafka({
    brokers: config.kafka.BROKERS,
    clientId: config.kafka.CLIENTID
})

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(function(req, res, next) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});
const PORT = process.env.PORT || 9000;

const topic = config.kafka.TOPIC

const producer = client.producer()

// producer.on("ready", function() {
//     console.log("Kafka producer is ready")
// })

// producer.on("error", function(error) {
//     console.log("Producer is on error state")
//     console.error(error)
// })

app.get("/", function(req, res) {
    res.send("Service is running")
})

app.post("/api/click", function(req, res) {
    console.log('getting message', req.body.message)
    let message = JSON.stringify({ message: req.body.message });

    const payload = {
        topic: topic, messages: [{ key: "click", value: message }], partition: 0
    }
    producer.send(payload)
    res.send("Success")
})

app.listen(PORT, () =>
    console.log("Express server is running on localhost:" + PORT)
);