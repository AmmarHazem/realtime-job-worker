import express from "express";
import os from "node:os";
import { createClient } from "redis";

const app = express();

const port = process.env.PORT || 3000;
const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";

const redisClient = createClient({ url: redisUrl });

app.use(express.json());

type JobModel = { fn: string };

const REDIS_JOBS_STREAM_NAME = "jobs";
const REDIS_CONSUMER_GROUP_NAME = "job_producer";
const REDIS_RESULTS_STREAM_NAME = "jobs_results";
const CONSUMER_NAME = `${os.hostname()}-${process.pid}`;

app.post("/api/jobs", async (req, res) => {
  const job: JobModel = req.body?.job;
  if (!job) {
    return res.status(400).json({ message: "invalid job" });
  }
  const addJobResult = await redisClient.xAdd(
    REDIS_JOBS_STREAM_NAME,
    "*",
    job ?? {},
  );
  res.json({ jobId: addJobResult });
});

app.get("/api/jobs", async (req, res) => {
  const jobs = await redisClient.xRange(REDIS_JOBS_STREAM_NAME, "-", "+", {
    COUNT: 9999,
  });
  res.json({ jobs });
});

async function ensureConsumerGroup({
  streamName,
  groupName,
}: {
  streamName: string;
  groupName: string;
}) {
  try {
    const createConsumerGroupOk = await redisClient.xGroupCreate(
      streamName,
      groupName,
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
      console.log(`consumer group ${groupName} already exists`);
    } else {
      throw e;
    }
  }
}

async function listenToJobResults() {
  try {
    const newClient = redisClient.duplicate();
    await newClient.connect();
    while (true) {
      const redisResponse = await newClient.xReadGroup(
        REDIS_CONSUMER_GROUP_NAME,
        CONSUMER_NAME,
        [{ key: REDIS_RESULTS_STREAM_NAME, id: ">" }],
        {
          COUNT: 1,
          BLOCK: 0,
        },
      );
      console.log("core results", redisResponse);
      if (!redisResponse?.length) continue;
      for (const stream of redisResponse) {
        for (const job of stream.messages) {
          console.log(`Core [${CONSUMER_NAME}] Received:`, typeof job, job);
          await redisClient.xAck(
            REDIS_RESULTS_STREAM_NAME,
            REDIS_CONSUMER_GROUP_NAME,
            job.id,
          );
        }
      }
    }
  } catch (e) {
    console.log("error listening to job results", e);
  }
}

async function main() {
  try {
    await redisClient.connect();
    await Promise.all([
      ensureConsumerGroup({
        streamName: REDIS_JOBS_STREAM_NAME,
        groupName: REDIS_CONSUMER_GROUP_NAME,
      }),
      ensureConsumerGroup({
        streamName: REDIS_RESULTS_STREAM_NAME,
        groupName: REDIS_CONSUMER_GROUP_NAME,
      }),
    ]);
    app.listen(port, async () => {
      console.log(`listening on port ${port}`);
      listenToJobResults();
    });
  } catch (e) {
    console.log("main error", e);
  } finally {
    console.log("finally");
  }
}

main();
