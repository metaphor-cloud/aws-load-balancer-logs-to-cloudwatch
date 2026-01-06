"use strict";

const { Readable } = require("stream");
const {
  CloudWatchLogsClient,
  PutLogEventsCommand,
  DescribeLogStreamsCommand,
  CreateLogStreamCommand,
} = require("@aws-sdk/client-cloudwatch-logs");
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const zlib = require("zlib");
const readline = require("readline");
const { fields, fieldFunctions } = require("./logFields");

const MAX_BATCH_SIZE = 1000000;
const MAX_BATCH_COUNT = 10000;
const LOG_EVENT_OVERHEAD = 26;

const cloudWatchLogs = new CloudWatchLogsClient();
const s3 = new S3Client();

const loadBalancerTypeEnvKey = "LOAD_BALANCER_TYPE";
const logGroupNameEnvKey = "LOG_GROUP_NAME";
const plaintextLogsEnvKey = "PLAINTEXT_LOGS";

function getEnvVar(name) {
  return process.env[name];
}

function parseLine(logFields, line) {
  const loadBalancerType = getEnvVar(loadBalancerTypeEnvKey);
  const parsed = {};
  let x = 0;
  let end = false;
  let withinQuotes = false;
  let element = "";
  for (const c of line + " ") {
    if (end) {
      if (element) {
        const fieldName = logFields[x];
        if (element.match(/^\d+.?\d*$/)) element = Number(element);
        if (fieldFunctions[loadBalancerType]?.[fieldName])
          fieldFunctions[loadBalancerType][fieldName](element, parsed);
        parsed[fieldName] = element;
        element = "";
        x++;
      }
      end = false;
    }
    if (c.match(/^\s$/) && !withinQuotes) end = true;
    if (c === '"') withinQuotes = !withinQuotes;
    else if (!end) element += c;
  }
  return parsed;
}

async function sendBatch(logGroupName, logStreamName, logEvents, seqToken) {
  try {
    console.log(`Sending batch to ${logStreamName}`);
    const putLogEventParams = {
      logEvents,
      logGroupName,
      logStreamName,
      sequenceToken: seqToken,
    };
    putLogEventParams.logEvents.sort((a, b) => a.timestamp - b.timestamp);
    console.log("Calling PutLogEvents");
    const cwPutLogEvents = await cloudWatchLogs.send(
      new PutLogEventsCommand(putLogEventParams)
    );
    console.log(
      `Success in putting ${putLogEventParams.logEvents.length} events`
    );
    return cwPutLogEvents.nextSequenceToken;
  } catch (err) {
    console.log("Error during put log events: ", err, err.stack);
    return seqToken;
  }
}

async function getS3Object(Bucket, Key) {
  console.log(`Retrieving ${Bucket}${Key}`);
  const getObjectCommand = new GetObjectCommand({ Bucket, Key });
  const response = await s3.send(getObjectCommand);
  return await response.Body.transformToByteArray("utf-8");
}

async function unpackLogData(s3object) {
  console.log("Unzipping log file");
  let result = "";
  return new Promise((resolve, reject) => {
    const bufferStream = new Readable();
    const gunzip = zlib.createGunzip();
    bufferStream.push(s3object);
    bufferStream.push(null);
    bufferStream.pipe(gunzip);
    gunzip.on("data", (data) => {
      result += data.toString();
    });
    gunzip.on("end", () => {
      console.log("Unzipped log file");
      resolve(result);
    });
    gunzip.on("error", (err) => {
      console.log("Error unzipping log file: ", err, err.stack);
      reject(err);
    });
  });
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function describeLogStreamWithRetry(
  describeLogStreamsCommand,
  maxRetries = 5,
  baseDelayMs = 100
) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const response = await cloudWatchLogs.send(describeLogStreamsCommand);
    if (response.logStreams && response.logStreams.length > 0) {
      return response;
    }
    if (attempt < maxRetries - 1) {
      const delayMs = baseDelayMs * (2 ** attempt);
      console.log(
        `Log stream not found, retrying in ${delayMs}ms (attempt ${
          attempt + 1
        }/${maxRetries})`
      );
      await sleep(delayMs);
    }
  }
  throw new Error("Log stream not found after retries");
}

async function getLogStreamSequenceToken(logGroupName, logStreamName) {
  console.log(`Checking Log Streams ${logGroupName}/${logStreamName}`);
  let currentStream;
  const describeLogStreamsCommand = new DescribeLogStreamsCommand({
    logGroupName,
    logStreamNamePrefix: logStreamName,
  });
  const cwlDescribeStreams = await cloudWatchLogs.send(
    describeLogStreamsCommand
  );
  if (cwlDescribeStreams.logStreams[0])
    currentStream = cwlDescribeStreams.logStreams[0];
  else {
    console.log(`Creating Log Stream ${logGroupName}/${logStreamName}`);
    const createLogStreamCommand = new CreateLogStreamCommand({
      logGroupName,
      logStreamName,
    });
    await cloudWatchLogs.send(createLogStreamCommand);
    const cwlDescribeCreatedStream = await describeLogStreamWithRetry(
      describeLogStreamsCommand
    );
    currentStream = cwlDescribeCreatedStream.logStreams[0];
  }
  return currentStream.uploadSequenceToken;
}

function finishBatch(batcher) {
  console.log(`Finished batch of size ${batcher.batch_size}`);
  batcher.batches.push(new Array(...batcher.batch));
  batcher.batch = [];
  batcher.batch_size = 0;
  return batcher;
}

function batchEvent(batcher, event) {
  const event_size = JSON.stringify(event).length + LOG_EVENT_OVERHEAD;
  let newBatchSize = batcher.batch_size + event_size;
  if (newBatchSize >= MAX_BATCH_SIZE) {
    batcher = finishBatch(batcher);
    newBatchSize = event_size;
  } else if (batcher.batch.length >= MAX_BATCH_COUNT) {
    batcher = finishBatch(batcher);
    newBatchSize = event_size;
  }
  batcher.batch.push(event);
  batcher.batch_size = newBatchSize;
  batcher.batcher_size += event_size;
  return batcher;
}

function readLogLine(logType, batcher, line) {
  const loadBalancerType = getEnvVar(loadBalancerTypeEnvKey);
  let fieldNames = fields[loadBalancerType][logType];
  if (!fieldNames) return console.log(`Unknown log type: ${logType}`);
  let timeIndex;
  for (let timeFieldName of ["time", "timestamp"]) {
    if (fieldNames.includes(timeFieldName)) {
      timeIndex = fieldNames.indexOf(timeFieldName);
      break;
    }
  }
  if (typeof timeIndex !== "number")
    return console.log(`No time field found in log type: ${logType}`);
  try {
    const parsed = parseLine(fieldNames, line);
    let ts = line.split(" ", timeIndex + 1)[timeIndex];
    if (!ts.endsWith("Z")) ts += "Z";
    const tval = Date.parse(ts);
    const plaintextLogs = getEnvVar(plaintextLogsEnvKey);
    if (!plaintextLogs) line = JSON.stringify(parsed);
    const event = { message: line, timestamp: tval };
    batcher = batchEvent(batcher, event);
  } catch (err) {
    console.log("Error parsing line: ", err, err.stack);
  }
}

async function readLogClose(
  batcher,
  logGroupName,
  logStreamName,
  sequenceToken
) {
  batcher.batches.push(batcher.batch);
  console.log(
    `Finished batching, pushing ${batcher.batches.length} batches (${batcher.batcher_size} bytes) to CloudWatch`
  );
  let seqToken = sequenceToken;
  let count = 0;
  let batch_count = 0;
  for (let i = 0; i < batcher.batches.length; i++) {
    const logEvents = batcher.batches[i];
    try {
      seqToken = await sendBatch(
        logGroupName,
        logStreamName,
        logEvents,
        seqToken
      );
      ++batch_count;
      count += logEvents.length;
    } catch (err) {
      console.log("Error sending batch: ", err, err.stack);
      continue;
    }
  }
  console.log(`Successfully put ${count} events in ${batch_count} batches`);
}

async function processS3Record(record, logGroupName, loadBalancerType) {
  const bucket = record?.s3?.bucket?.name;
  if (!bucket) throw new Error("No bucket found in record");

  const key = decodeURIComponent(record?.s3?.object?.key?.replace(/\+/g, " "));
  if (!key) throw new Error("No key found in record");
  if (key.endsWith("TestFile")) {
    console.log("Ignoring test file");
    return;
  }

  const eventName = record?.eventName;
  if (!eventName) throw new Error("No event name found in record");
  if (!eventName.includes("ObjectCreated:")) {
    console.log(`Ignoring event: ${eventName}`);
    return;
  }

  const logStreamName = key;
  const s3object = await getS3Object(bucket, key);
  const logData = await unpackLogData(s3object);
  if (!logData) {
    console.warn("Unable to unpack log data");
    return;
  }

  let logType;
  switch (true) {
    case key.includes("conn_log"):
      logType = "connection";
      break;
    default:
      logType = "access";
  }

  console.log(`${loadBalancerType} load balancer: ${logType} log`);

  let sequenceToken = await getLogStreamSequenceToken(
    logGroupName,
    logStreamName
  );

  console.log("Parsing log lines");
  var batcher = {
    batches: [],
    batch: [],
    batch_size: 0,
    batcher_size: 0,
  };
  var bufferStream = new Readable();
  bufferStream.push(logData);
  bufferStream.push(null);

  await new Promise((resolve, reject) => {
    try {
      let rl = readline.createInterface({ input: bufferStream });
      let logLines = [];
      rl.on("line", (line) => logLines.push(line));
      rl.on("close", async () => {
        console.log("Finished reading log lines");
        logLines.sort();
        for (let line of logLines) {
          readLogLine(logType, batcher, line);
        }
        console.log("Finished parsing log lines");
        await readLogClose(batcher, logGroupName, logStreamName, sequenceToken);
        console.log("Finished sending log lines");
        resolve();
      });
    } catch (err) {
      reject(err);
    }
  });
}

exports.handler = async (event) => {
  console.log(JSON.stringify(event));
  const logGroupName = getEnvVar(logGroupNameEnvKey);
  const loadBalancerType = getEnvVar(loadBalancerTypeEnvKey);
  let records = event?.Records;
  if (!records) return;

  let batchItemFailures;
  for (let record of records) {
    switch (record?.eventSource) {
      case "aws:s3":
        await processS3Record(record, logGroupName, loadBalancerType);
        break;
      case "aws:sqs":
        if (!batchItemFailures) batchItemFailures = [];
        try {
          const sqsRecordBody = JSON.parse(record?.body);
          if (!sqsRecordBody?.Records) {
            console.warn("No records found in SQS record body");
            break;
          }
          for (let s3Event of sqsRecordBody?.Records) {
            await processS3Record(s3Event, logGroupName, loadBalancerType);
          }
        } catch (err) {
          console.log("Error processing SQS record: ", err, err.stack);
          batchItemFailures.push({ itemIdentifier: record?.messageId });
        }
        break;
      default:
        console.warn(`Unknown event source: ${record?.eventSource}`);
    }
  }
  console.log("Finished processing records");
  if (batchItemFailures !== undefined) {
    console.log("Batch item failures: ", batchItemFailures);
    return { batchItemFailures };
  }
};
