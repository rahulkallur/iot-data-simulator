import pkg from "kafkajs";
const { Kafka } = pkg;
import dotenv from "dotenv";
dotenv.config();

const kafka = new Kafka({
  clientId: "pacemaker-data-" + Date.now(), // Append Current Epoch milliseconds for Random Id
  brokers: [
    process.env.KAFKA_BOOTSTRAP_SERVER_URL ||
      "my-cluster-kafka-bootstrap.kafka:9092",
  ],
    sasl: {
      mechanism: "scram-sha-512",
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
    },
});
const imeiList = ["354982074563217", "867493027564321", "356789012345678"];

// Function to generate random location (latitude, longitude)
const getRandomLocation = () => {
  const lat = (Math.random() * 180 - 90).toFixed(6); // Range: -90 to 90
  const lon = (Math.random() * 360 - 180).toFixed(6); // Range: -180 to 180
  return { latitude: parseFloat(lat), longitude: parseFloat(lon) };
};

// Function to generate a single IoT data packet
const generateIoTData = () => {
  return imeiList.map((imei) => {
    const location = getRandomLocation();
    return {
      imei,
      timestamp: Math.floor(Date.now() / 1000),
      latitude: location.latitude,
      longitude: location.longitude,
      speed: parseInt((Math.random() * 100).toFixed(2)), // Speed in km/h
      battery: parseInt((Math.random() * 100).toFixed(0)), // Battery percentage
    };
  });
};

async function publishData(data) {
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: process.env.PUBLISH_TOPIC,
    messages: [{ value: data }],
  });
  await producer.disconnect();
}

// Function to stream data every 5 seconds
const streamData = () => {
  setInterval(async () => {
    const data = generateIoTData();
    console.log(JSON.stringify(data, null, 2)); // Print to console
    for(let i=0;i<data.length;i++) {
      await publishData(JSON.stringify(data[i]));
    }
  }, 5000);
};

// Start streaming
streamData();
