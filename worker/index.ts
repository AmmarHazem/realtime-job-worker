import express from "express";
import os from "node:os";
import { createClient } from "redis";

const app = express();

const port = process.env.PORT || 3001;
const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";

const redisClient = createClient({ url: redisUrl });

app.use(express.json());

const REDIS_JOBS_STREAM_NAME = "jobs";
const REDIS_CONSUMER_GROUP_NAME = "job_consumer";
const CONSUMER_NAME = `${os.hostname()}-${process.pid}`;

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

async function startProcessingJobs() {
  while (true) {
    try {
      const redisResponse = await redisClient.xReadGroup(
        REDIS_CONSUMER_GROUP_NAME,
        CONSUMER_NAME,
        [{ key: REDIS_JOBS_STREAM_NAME, id: ">" }],
        {
          COUNT: 1,
          BLOCK: 0,
        },
      );
      if (!redisResponse?.length) continue;
      for (const stream of redisResponse) {
        for (const message of stream.messages) {
          console.log(`[${CONSUMER_NAME}] Received:`, message);
          await redisClient.xAck(
            REDIS_JOBS_STREAM_NAME,
            REDIS_CONSUMER_GROUP_NAME,
            message.id,
          );
        }
      }
    } catch (e) {
      console.log("startProcessingJobs error", e);
    }
  }
}

async function main() {
  try {
    await redisClient.connect();
    await ensureConsumerGroup();
    app.listen(port, async () => {
      console.log(`listening on port ${port}`);
      startProcessingJobs();
    });
  } catch (e) {
    console.log("main error", e);
  } finally {
    console.log("--- finally");
  }
}

main();
