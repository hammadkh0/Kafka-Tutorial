const { Kafka } = require("kafkajs");

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const consumer = kafka.consumer({
      groupId: "Test",
    });
    await consumer.connect();
    console.log("consumer connected...");

    await consumer.subscribe({
      topic: "Users",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async (result) => {
        console.log(
          `Received message: ${result.message.value} on partition: ${result.partition}`
        );
      },
    });
  } catch (error) {
    console.error(error);
  }
}

run();
