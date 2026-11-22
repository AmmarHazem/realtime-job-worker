import express from "express";
import { createClient } from "redis";

const app = express();

const port = process.env.PORT || 3000;
const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";

const redisClient = createClient({ url: redisUrl });

app.use(express.json());

const REDIS_JOBS_STREAM_NAME = "jobs";
const REDIS_CONSUMER_GROUP_NAME = "job_producer";
