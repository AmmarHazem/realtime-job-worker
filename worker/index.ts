import express from "express";
import { createClient } from "redis";

const app = express();

const port = process.env.PORT || 3001;
const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";

const redisClient = createClient({ url: redisUrl });

app.use(express.json());

const REDIS_JOBS_STREAM_NAME = "jobs";
const REDIS_CONSUMER_GROUP_NAME = "job_consumer";

async function ensureConsumerGroup() {
  try {
    const createConsumerGroupOk = await redisClient.xGroupCreate(
      REDIS_JOBS_STREAM_NAME,
      REDIS_CONSUMER_GROUP_NAME,
      "$",
      {
        MKSTREAM: true,
      },
    );
    if (createConsumerGroupOk !== "OK") {
      throw new Error("failed to create redis consumer group");
    }
  } catch (e) {
    if (e instanceof Error && e.message?.includes("BUSYGROUP")) {
      console.log("consumer group already exists");
    } else {
      throw e;
    }
  }
}

async function main() {
  try {
    await redisClient.connect();
    await ensureConsumerGroup();
    app.listen(port, async () => {
      console.log(`listening on port ${port}`);
    });
  } catch (e) {
    console.log("main error", e);
  } finally {
    console.log("finally");
  }
}

main();
