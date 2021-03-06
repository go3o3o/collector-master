const cluster = require("cluster");
const express = require("express");
const morgan = require("morgan");

const config = require("./config");
const logger = require("./lib/logger");
const slack = require("./lib/slack");

const PRODUCER_DONE = "done";
const CONSUMER = "consumer";
const PRODUCER = "producer";

// producer 한대, consumer 3대
if (cluster.isMaster) {
  const groupCount = config.server.group_count;

  let workerCnt = 0;

  // 프로듀서 생성
  const workerProducer = cluster.fork();
  let workerProducerBusy = false;
  workerCnt++;

  // 메시지 대기
  workerProducer.on("message", function(msg) {
    if (msg === PRODUCER_DONE) {
      workerProducerBusy = false;
    }
  });

  // 운영 컨슈머 생성 * group count
  for (let idx = 0; idx < groupCount; idx++) {
    const workerConsumer = cluster.fork();
    workerCnt++;
    workerConsumer.send({
      mode: CONSUMER
    });
  }

  const app = express();
  app.set("port", config.server.port);
  app.use(
    morgan("combine", {
      stream: logger.stream
    })
  );

  app.get("/produce", function(req, res) {
    try {
      let data = {};
      data.Result = "OK";
      if (!workerProducerBusy) {
        workerProducer.send({
          mode: PRODUCER
        });
        workerProducerBusy = true;
      } else {
        data.Message = "producer is busy!";
      }
      res.send(data);
    } catch (err) {
      res.send({ msg: "produce error", error: err });
    }
  });

  app.get("/health", function(req, res) {
    try {
      let data = {};
      if (workerCnt > 2) {
        data.Result = "OK";
        data.Message = "WorkerCnt: " + workerCnt;
      } else {
        data.Result = "ERROR";
        data.Message = "WorkerCnt: " + workerCnt;
      }
      res.send(data);
    } catch (err) {
      res.send({ msg: "health error", error: err });
    }
  });

  const server = app.listen(app.get("port"), function() {
    const host = server.address().address;
    const port = server.address().port;

    logger.info(`[app][master] Server Listening on port ${port}`);
  });

  cluster.on("exit", function(worker, code, signal) {
    workerCnt--;
    logger.error(
      `[app][master] worker ${worker.process.pid} died - code ${code}, signal ${signal}`
    );
  });
} else {
  const producer = require("./sqs/producer");
  const consumer = require("./sqs/consumer");
  const SQSProducer = producer({ mode: "test" });
  const SQSConsumerTest = consumer({ mode: "test" });

  process.on("message", function(msg) {
    if (msg.mode === PRODUCER) {
      logger.debug("[app][worker] SQS Producer 시작");
      SQSProducer.produce({}, function(err) {
        if (err) {
          logger.error(err);
        } else {
          logger.info("[app][worker] SQS 메시지 생성 완료");
          process.send(PRODUCER_DONE);
        }
      });
    } else if (msg.mode === CONSUMER) {
      logger.debug("[app][worker] SQS Consumer 시작");
      SQSConsumerTest.consumer();
    }
  });
}
