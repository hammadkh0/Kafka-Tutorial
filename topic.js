const { Kafka } = require("kafkajs");

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const admin = kafka.admin();
    await admin.connect();
    console.log("connected...");

    // A-M, N-Z
    await admin.createTopics({
      topics: [
        {
          topic: "Users",
          numPartitions: 2,
        },
      ],
    });
    console.log("created successfully");
    await admin.disconnect();
  } catch (error) {
    console.error(error);
  } finally {
    process.exit(0);
  }
}

run();
