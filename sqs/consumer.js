const async = require("async");
const aws = require("aws-sdk");
const moment = require("moment");
const path = require("path");

const config = require("../config");
const common = require("../lib/common");
const logger = require("../lib/logger");
const slack = require("../lib/slack");
const fsUtil = require("../lib/fsUtil");
const workerDao = require("../dao/workerDao");
const crawlRequestDao = require("../dao/crawlRequestDao");
const crawlProgressDao = require("../dao/crawlProgressDao");

module.exports = function(params) {
  const regions = config.sqs.regions;
  const region = config.sqs.region;

  const modes = config.sqs.modes;
  const mode = params.mode;

  const sqsNames = config.sqs.progress.names;
  const queryUrls = config.sqs.base_query_urls;
  const accessKeyId = config.sqs.access_key_id;
  const secretAccessKey = config.sqs.secret_access_key;

  const sqsName = sqsNames[modes.indexOf(mode)];
  const queryUrl = queryUrls[regions.indexOf(region)];

  // AWS 설정 세팅
  aws.config.update({
    region,
    accessKeyId,
    secretAccessKey
  });

  const Sqs = new aws.SQS();

  const consumer = function() {
    logger.debug(
      `[SQS/consumer] Consumer 시작 [region: ${region}, queue: ${sqsName}]`
    );

    const payload = {
      MaxNumberOfMessage: 10,
      QueueUrl: queryUrl + sqsName,
      VisibilityTimeout: 300,
      WaitTimeSeconds: 0
    };
    async.forever(
      function(next) {
        logger.info(`[SQS/consumer] SQS 메시지 수신`);
        Sqs.receiveMessage(payload, function(err, data) {
          if (err) {
            logger.error(err);
            next(null);
          } else {
            if (data.Messages === undefined) {
              logger.info(`[SQS/consumer] 수집 Request 메시지 없음`);
              logger.info(`[SQS/consumer] sleep until 2 seconds`);
              common.timeout(2, next);
            } else {
              let deletePayload = {
                QueueUrl: payload.QueueUrl,
                Entries: []
              };

              let payloadIdx = 0;

              async.eachSeries(
                data.Messages,
                function(msg, callback) {
                  let message = JSON.parse(msg.Body);
                  async.waterfall(
                    [
                      function(callback) {
                        logger.debug(
                          `[SQS/consumer] Step #1. 워커 상태 업데이트`
                        );

                        const worker = {
                          ip: message.ip,
                          host: message.host,
                          status: message.status,
                          errorMsg: message.errorMsg
                        };

                        logger.debug(worker);
                        workerDao.upsertWokerStatus(worker, callback);
                      },
                      function(callback) {
                        logger.debug(
                          `[SQS/consumer] Step #2. 수집 현황 상태 업데이트`
                        );

                        const crawlProgress = {
                          requestSeq: message.requestSeq,
                          period: message.period,
                          startDt: message.startDt,
                          endDt: message.endDt,
                          status: message.status,
                          errorMsg: message.errorMsg
                        };
                        logger.debug(crawlProgress);
                        crawlProgressDao.updateCrawlProgress(
                          crawlProgress,
                          callback
                        );
                      },
                      function(callback) {
                        logger.debug(
                          `[SQS/consumer] Step #3. 수집 MD5 업데이트`
                        );
                        if (message.md5 !== undefined) {
                          const collectCheckPath = `${__dirname}/../collect_check/${message.requestSeq}`;
                          fsUtil.makeDir(collectCheckPath, function(err) {
                            if (err) {
                              callback(err);
                            }
                          });

                          const collectCheckFile = `${__dirname}/../collect_check/${message.requestSeq}/duplication.md5`;
                          logger.debug(collectCheckFile);
                          fsUtil.writeFile(
                            collectCheckFile,
                            message.md5,
                            callback
                          );
                        } else {
                          callback(null);
                        }
                      },
                      function(callback) {
                        logger.debug(`[SQS/consumer] Step #4. 수집 에러 처리`);

                        if (message.status.includes("Error")) {
                          const crawlReq = {
                            seq: message.requestSeq,
                            status: "CRS005"
                          };
                          logger.debug(crawlReq);
                          crawlRequestDao.updateCrawlRequest(
                            crawlReq,
                            callback
                          );
                        } else {
                          callback(null);
                        }
                      },
                      function(callback) {
                        logger.debug(
                          `[SQS/consumer] Step #5. 수집 완료 여부 확인`
                        );

                        if (
                          message.status === "docFinished" ||
                          message.status === "linkFinished"
                        ) {
                          __checkCrawlRequestIsFinish(message, callback);
                        } else {
                          callback(null);
                        }
                      },
                      function(callback) {
                        logger.debug(
                          `[SQS/consumer] Step #6. SQS 메시지 삭제 파라미터 생성`
                        );

                        let entriesValue = {
                          Id: data.ResponseMetadata.RequestId + payloadIdx++,
                          ReceiptHandle: msg.ReceiptHandle
                        };

                        deletePayload.Entries.push(entriesValue);
                        callback(null);
                      }
                    ],
                    callback
                  );
                },
                function(err) {
                  if (err) {
                    logger.error(err.toString());
                    slack.sendMessage(
                      {
                        color: "danger",
                        title: "collector-master",
                        value: err.toString()
                      },
                      function(err) {
                        if (err) {
                          logger.error(err);
                          next(null);
                        } else {
                          logger.info(`[SQS/consumer] sleep until 10 seconds`);
                          common.timeout(10, next);
                        }
                      }
                    );
                  } else {
                    async.waterfall(
                      [
                        function(callback) {
                          logger.debug("Step #7. SQS 메시지 삭제");
                          logger.debug(deletePayload);
                          Sqs.deleteMessageBatch(deletePayload, callback);
                        },
                        function(result, callback) {
                          logger.debug(result);
                          logger.info(
                            "[sqs/consumer] all queue messages have been processed"
                          );
                          logger.info("[SQS/consumer] sleep until 1 seconds");
                          common.timeout(1, callback);
                        }
                      ],
                      next
                    ); // waterfall
                  }
                }
              );
            }
          }
        });
      },
      function(err) {
        if (err) {
          logger.error(err.toString());
          slack.sendMessage(
            {
              color: "danger",
              title: "collector-master",
              value: err.toString()
            },
            function(err) {
              if (err) {
                logger.error(err);
              } else {
                logger.info(
                  "[SQS/consumer] Successfully push message to Slack"
                );
              }
            }
          ); //sendMessage
        } else {
          logger.error("Module unexpectedly finished.");
          slack.sendMessage(
            {
              color: "danger",
              title: "collector-master",
              value: "Module unexpectedly finished."
            },
            function(err) {
              if (err) {
                logger.error(err);
              } else {
                logger.info(
                  "[SQS/consumer] Successfully push message to Slack"
                );
              }
            }
          ); //sendMessage
        }
      }
    );
  };

  /**
   * 수집 완료 여부 확인
   * @param {jsonObj} message
   * @param {Function} callback
   */
  const __checkCrawlRequestIsFinish = (message, callback) => {
    async.waterfall(
      [
        function(callback) {
          logger.debug(`Step #5-1. crawlRequest의 startDt, endDt 가져오기`);
          const crawlReq = {
            seq: message.requestSeq
          };

          crawlRequestDao.selectCrawlRequest(crawlReq, function(err, results) {
            if (err) {
              callback(err);
            } else {
              if (results.length === 0) {
                callback("NO_MATCHED_CRAWL_REQUEST");
              } else {
                logger.debug(results[0]);
                callback(null, results[0]);
              }
            }
          });
        },
        function(crawlReq, callback) {
          logger.debug(`Step #5-2. startDt ~ endDt 까지 dates 배열 만들기`);

          let dates = [];

          let startDt;
          let endDt;

          if (crawlReq.period === "DD") {
            startDt = moment(crawlReq.startDt);
            endDt = moment(crawlReq.endDt);
          } else if (crawlReq.period === "MM") {
            startDt = moment(crawlReq.startDt, "YYYY-MM");
            endDt = moment(crawlReq.endDt, "YYYY-MM");
          } else if (crawlReq.period === "YY") {
            startDt = moment(crawlReq.startDt, "YYYY");
            endDt = moment(crawlReq.endDt, "YYYY");
          } else if (crawlReq.period === "QQ") {
            startDt = moment(crawlReq.startDt, "YYYY-Q");
            endDt = moment(crawlReq.endDt, "YYYY-Q");
          }

          async.whilst(
            function() {
              return startDt.isSameOfBefore(endDt);
            },
            function(callback) {
              const date = {
                startDt: moment(startDt).format("YYYY-MM-DD"),
                endDt: moment(startDt).format("YYYY-MM-DD")
              };

              dates.push(date);

              if (crawlReq.period === "DD") {
                startDt.add(1, "d");
              } else if (crawlReq.period === "MM") {
                startDt.add(1, "M");
              } else if (crawlReq.period === "YY") {
                startDt.add(1, "y");
              } else if (crawlReq.period === "QQ") {
                startDt.add(1, "Q");
              }

              callback(null);
            },
            function(err) {
              if (err) {
                callback(err);
              } else {
                callback(null, dates);
              }
            }
          );
        },
        function(dates, callback) {
          logger.debug(`Step #5-3. 해당 기간의 progress 확인하기`);

          let isAllFinished = true;
          const crawlProgress = {
            requestSeq: message.requestSeq,
            period: message.period,
            startDt: date[0].startDt,
            endDt: dates[dates.length - 1].endDt
          };

          async.waterfall(
            [
              function(callback) {
                logger.debug(
                  `Step #5-3-1. 해당 기간의 status 별 progress 확인`
                );
                logger.debug(crawlProgress);
                crawlProgressDao.selectCrawlProgressByStatus(
                  crawlProgress,
                  function(err, results) {
                    if (err) {
                      callback(err);
                    } else {
                      for (result of results) {
                        logger.debug(result);
                        if (
                          result.status !== "docFinished" &&
                          result.status !== "linkDocFinished"
                        ) {
                          isAllFinished = false;
                        } else {
                          if (dates.length !== result.cnt) {
                            isAllFinished = false;
                          }
                        }
                      }
                      callback(null);
                    }
                  }
                );
              },
              function(callback) {
                logger.debug(
                  `Step #5-3-2. 해당 기간의 on_going_flag 별 progress 확인`
                );
                logger.debug(crawlProgress);
                crawlProgressDao.selectCrawlProgressByOnGoingFlag(
                  crawlProgress,
                  function(err, results) {
                    if (err) {
                      callback(err);
                    } else {
                      for (result of results) {
                        logger.debug(result);
                        if (result.onGoingFlag !== "N") {
                          isAllFinished = false;
                        } else {
                          if (dates.length !== result.cnt) {
                            isAllFinished = false;
                          }
                        }
                      }
                      callback(null);
                    }
                  }
                );
              }
            ],
            function(err) {
              if (err) {
                callback(err);
              } else {
                callback(null, isAllFinished);
              }
            }
          );
        },
        function(isAllFinished, callback) {
          logger.debug(
            `Step #5-4. 모든 날짜의 수집이 완료된 경우 crawlRequest의 상태 변경하기 ::: ${isAllFinished}`
          );

          if (isAllFinished) {
            const crawlReq = {
              seq: message.requestSeq,
              status: "CRS003"
            };

            crawlRequestDao.updateCrawlRequest(crawlReq, callback);
          } else {
            callback(null);
          }
        }
      ],
      callback
    );
  };

  return {
    consumer: consumer
  };
};
