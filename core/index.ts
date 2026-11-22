import express from "express";
import { createClient } from "redis";

const app = express();

const port = process.env.PORT || 3000;
const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";

const redisClient = createClient({ url: redisUrl });

app.use(express.json());

type JobModel = Record<string, string>;

const REDIS_JOBS_STREAM_NAME = "jobs";
const REDIS_CONSUMER_GROUP_NAME = "job_producer";

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
