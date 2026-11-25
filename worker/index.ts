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
const REDIS_RESULTS_STREAM_NAME = "jobs_results";
const CONSUMER_NAME = `${os.hostname()}-${process.pid}`;

async function ensureJobsConsumerGroup() {
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

async function reportJobResult({
  id,
  status,
  result,
}: {
  id: string;
  result: never;
  status: "done" | "failed";
}) {
  try {
    await redisClient.xAdd(REDIS_RESULTS_STREAM_NAME, id, {
      id,
      status,
      result: result ?? "",
    });
  } catch (e) {
    console.log("error reporting job", e);
  }
}

async function executeJob({ job, id }: { job: string; id: string }) {
  let result = null;
  try {
    result = await eval(job);
  } catch (e) {
    console.log(`failed to process job ${id}: ${job}`);
    result = null;
  } finally {
    await reportJobResult({
      id,
      result: result as never,
      status: result ? "done" : "failed",
    });
    return result;
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
        for (const job of stream.messages) {
          console.log(`Worker [${CONSUMER_NAME}] Received:`, typeof job, job);
          await executeJob({
            job: job.message.fn,
            id: job.id,
          });
          await redisClient.xAck(
            REDIS_JOBS_STREAM_NAME,
            REDIS_CONSUMER_GROUP_NAME,
            job.id,
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
    await ensureJobsConsumerGroup();
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
