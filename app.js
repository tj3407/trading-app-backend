const express = require("express");
const kafka = require('kafka-node')
const bodyParser = require("body-parser");


const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(function(req, res, next) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});
const PORT = process.env.PORT || 9000;

// const client = new kafka.Client();

// const topics = [
//     {
//         topic: "clicks",
//         offset: 0
//     }
// ]

// const options = {
//     autoCommit: true
// }

// const consumer = new kafka.HighLevelConsumer(client, topics, options);
// consumer.on("message", (message) => {
//     console.log(message)
// })

const Producer = kafka.Producer,
    client = new kafka.KafkaClient({ kafkaHost: "localhost:9092" }),
    producer = new Producer(client)

producer.on("ready", function() {
    console.log("Kafka producer is ready")
})

producer.on("error", function(error) {
    console.log("Producer is on error state")
    console.error(error)
})

app.get("/", function(req, res) {
    res.send("Service is running")
})

app.post("/api/click", function(req, res) {
    console.log('getting message')
    let message = JSON.stringify(req.body.message);
    console.log(message)
    const payload = [
        { topic: "clicks", messages: message, partition: 0 }
    ]
    producer.send(payload, (err, data) => {
        console.log("data sent:", data);
        res.json(data);
    })
})

app.listen(PORT, () =>
    console.log("Express server is running on localhost:" + PORT)
);