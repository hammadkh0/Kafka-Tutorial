const { Kafka, Partitioners } = require("kafkajs");
const msg = process.argv[2];

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const producer = kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
    await producer.connect();
    console.log("producer connected...");

    const result = await producer.send({
      topic: "Users",
      messages: [
        {
          value: msg,
          partition: msg[0] < "N" ? 0 : 1, // msg[0] is the first character of message
        },
      ],
    });
    console.log("producer sent message successfully", result);
    await producer.disconnect();
  } catch (error) {
    console.error(error);
  } finally {
    process.exit(0);
  }
}

run();
