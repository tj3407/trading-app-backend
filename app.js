require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
const Kafka = require("node-rdkafka");

const kafkaConf = {
  "group.id": "cloudkarafka-clicks",
  "metadata.broker.list": process.env.CLOUDKARAFKA_BROKERS,
  "socket.keepalive.enable": true,
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "SCRAM-SHA-256",
  "sasl.username": process.env.CLOUDKARAFKA_USERNAME,
  "sasl.password": process.env.CLOUDKARAFKA_PASSWORD,
  debug: "generic,broker,security",
};

const prefix = process.env.CLOUDKARAFKA_USERNAME;
const topic = `${prefix}-clicks`;
const producer = new Kafka.Producer(kafkaConf);

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(function (req, res, next) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  next();
});
const PORT = process.env.PORT || 9000;

app.get("/", function (req, res) {
  console.log(process.env.CLOUDKARAFKA_BROKERS);
  res.send("Service is running");
});

app.post("/api/click", function (req, res) {
  console.log("getting message", req.body.message);
  let message = Buffer.from(JSON.stringify({ message: req.body.message }));

  producer.produce(topic, -1, message, Date.now());
  res.send("Success");
});

app.listen(PORT, () =>
  console.log("Express server is running on localhost:" + PORT)
);

producer.on("ready", function(arg) {
    console.log(`producer ${arg.name} ready.`);
});

producer.on("disconnected", function (arg) {
  process.exit();
});

producer.on("event.error", function (err) {
  console.error(err);
  process.exit(1);
});
producer.on("event.log", function (log) {
  console.log(log);
});
producer.connect();
