const async = require("async");
const aws = require("aws-sdk");
const moment = require("moment");
const path = require("path");

const config = require("../config");
const logger = require("../lib/logger");
const fsUtil = require("../lib/fsUtil");
const userAgentDao = require("../dao/userAgentDao");
const channelDao = require("../dao/channelDao");
const crawlRequestDao = require("../dao/crawlRequestDao");
const crawlProgressDao = require("../dao/crawlProgressDao");

module.exports = function(params) {
  const mode = params.mode;
  const modes = config.sqs.modes;
  const region = config.sqs.region;
  const regions = config.sqs.regions;
  const groupCount = config.server.group_count;
  const sqsNames = config.sqs.request.casper.names;
  const baseUrls = config.sqs.base_query_urls;
  const accessKeyId = config.sqs.access_key_id;
  const secretAccessKey = config.sqs.secret_access_key;

  // SQS 세팅
  let sqsName = sqsNames[modes.indexOf(mode)];
  const queryUrl = baseUrls[regions.indexOf(region)];

  // AWS 설정 세팅
  aws.config.update({
    region,
    accessKeyId,
    secretAccessKey
  });

  const Sqs = new aws.SQS();

  const produce = (crawlReqParams, callback) => {
    logger.debug(
      `[SQS/producer] Producer 시작 [mode : ${mode}, region : ${region}]`
    );
    logger.debug(
      `[SQS/producer] Request Parameters [${JSON.stringify(crawlReqParams)}]`
    );

    async.waterfall(
      [
        function(callback) {
          logger.debug(
            `[SQS/producer] Step #1. 수집 요청 건 가져오기 - mode : ${mode}`
          );
          if (mode === "manual" || mode === "test") {
            crawlRequestDao.selectCrawlRequestList(
              crawlReqParams,
              null,
              null,
              null,
              null,
              mode,
              callback
            );
          } else {
            var s_year = moment().format("YYYY");
            var s_month = moment().format("MM");
            var s_day = moment().format("DD");
            var hours = moment().format("HH");

            var now = new Date();
            var lastDate = new Date(
              now.getYear(),
              now.getMonth() + 1,
              0
            ).getDate(); // 현재 월의 마지막 날짜 계산

            if (s_day == lastDate) {
              // 현재 날짜가 마지막 날이면 월별 수집 할 채널들의 리스트 가져옴.
              s_day = "32"; // db에서 day_schedules = 32인 애들은 월의 마지막날 수집이 돌아감
            }

            logger.debug(
              `[SQS/producer] 현재 시각: ${s_year}-${s_month}-${s_day}:${hours}시`
            );

            crawlRequestDao.selectCrawlRequestList2(
              crawlReqParams,
              s_year,
              s_month,
              s_day,
              hours,
              mode,
              callback
            );
          }
        },
        function(crawlReqs, callback) {
          logger.debug(`[SQS/producer] Step #2. userAgent 정보 가져오기`);

          if (crawlReqs.length === 0) {
            callback("NO_MESSAGE");
          } else {
            userAgentDao.selectUserAgentList(function(err, userAgents) {
              if (err) {
                callback(err);
              } else {
                callback(null, crawlreqs, userAgents);
              }
            });
          }
        },
        function(crawlReqs, userAgents, callback) {
          logger.debug(`[SQS/producer] Step #3. 각 수집 요청 건별로 처리하기`);

          async.eachSeries(
            crqwlReqs,
            function(crawlReq, callback) {
              logger.debug(crawlReq);
              crawlReq.userAgents = userAgents;

              let channelInfo = { seq: crawlReq.channelSeq };

              async.waterfall(
                [
                  function(callback) {
                    logger.deubg(
                      `[SQS/producer] Step #3-1. 수집 요청 건 채널 및 로그인 정보 가져오기`
                    );
                    channelDao.selectChannelInfo(channelInfo, function(
                      err,
                      results
                    ) {
                      if (err) {
                        callback(err);
                      } else {
                        if (results[0].chromeUseYn === "Y") {
                          logger.debug(
                            `[SQS/producer] config.sqs.request.names :: ${config.sqs.request.names}`
                          );
                          sqsName =
                            config.sqs.request.name[modes.indexOf(mode)];
                        } else {
                          sqsName = sqsNames[modes.indexOf(mode)];
                        }

                        // 채널 기본 정보 입력
                        channelInfo.name = results[0].name;
                        channelInfo.url = results[0].url;
                        channelInfo.rule = {
                          fileName: results[0].fileName,
                          filePath: results[0].filePath
                        };

                        // 채널 로그인 정보 입력
                        if (results[0].loginSeq !== null) {
                          channelInfo.loginRule = {};
                          channelInfo.loginRule.seq = results[0].loginSeq;
                          channelInfo.loginRule.id = results[0].id;
                          channelInfo.loginRule.password = results[0].password;
                        }

                        callback(null);
                      }
                    });
                  },
                  function(callback) {
                    logger.debug(
                      `[SQS/producer] Step #3-2. 수집 요청 건 채널의 engine 정보 가져오기`
                    );
                    channelDao.selectChannelEngineList(channelInfo, function(
                      err,
                      results
                    ) {
                      if (err) {
                        callback(err);
                      } else {
                        async.each(
                          results,
                          function(result, callback) {
                            if (result.typeCd === "CET001") {
                              crawlReq.role = "linkCollector";
                              channelInfo.linkEngine = result;
                            } else if (result.typeCd === "CET002") {
                              // 문서 수집 엔진
                              channelInfo.docEngine = result;
                            } else if (result.typeCd === "CET003") {
                              // 링크-문서 수집 엔진
                              crawlReq.role = "linkDocCollector";
                              channelInfo.linkDocEngine = result;
                            } else if (result.typeCd === "CET004") {
                              // API 수집 엔진
                              crawlReq.role = "apiCollector";
                              channelInfo.apiEngine = result;
                            }
                            callback(null);
                          },
                          callback
                        );
                      }
                    });
                  },
                  function(callback) {
                    logger.debug(
                      `[SQS/producer] Step #3-3. 수집 요청 건 채널의 injectScript 정보 가져오기`
                    );
                    logger.debug(
                      `[SQS/producer] channelInfo :: ${channelInfo}`
                    );
                    channelDao.selectChannelInjectScriptList(
                      channelInfo,
                      function(err, results) {
                        if (err) {
                          callback(err);
                        } else {
                          channelInfo.injectScript = results;
                          crawlReq.channel = channelInfo;
                          callback(null);
                        }
                      }
                    );
                  },
                  function(callback) {
                    logger.debug(
                      `[SQS/producer] Step #3-4. 수집 요청 건의 수집 대상 날짜 리스트 생성`
                    );

                    let dates = [];

                    let startDt = moment(crawlReq.startDt);
                    let endDt = moment(crawlReq.endDt);

                    const today = moment();

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

                    if (
                      crawlReqParams.startDt !== undefined &&
                      crawlReqParams.startDt !== ""
                    ) {
                      if (crawlReqParams.period === "DD") {
                        startDt = moment(crawlReqParams.startDt);
                      } else if (crawlReqParams.period === "MM") {
                        startDt = moment(crawlReqParams.startDt, "YYYY-MM");
                      } else if (crawlReqParams.period === "YY") {
                        startDt = moment(crawlReqParams.startDt, "YYYY");
                      } else if (crawlReqParams.period === "QQ") {
                        startDt = moment(crawlReqParams.startDt, "YYYY-Q");
                      }
                    }

                    if (
                      crawlReqParams.endDt !== undefined &&
                      crawlReqParams.endDt !== ""
                    ) {
                      if (crawlReqParams.period === "DD") {
                        endDt = moment(crawlReqParams.endDt);
                      } else if (crawlReqParams.period === "MM") {
                        endDt = moment(crawlReqParams.endDt, "YYYY-MM");
                      } else if (crawlReqParams.period === "YY") {
                        endDt = moment(crawlReqParams.endDt, "YYYY");
                      } else if (crawlReqParams.period === "QQ") {
                        endDt = moment(crawlReqParams.endDt, "YYYY-Q");
                      }
                    }

                    if (startDt.isAfter(today)) {
                      callback("DATE_TOO_EARLY");
                    } else {
                      if (endDt.isAfter(today)) {
                        endDt = today;
                      }

                      if (mode === "retroactive") {
                        try {
                          const date = {
                            startDt: moment(startDt).format("YYYY-MM-DD"),
                            endDt: moment(endDt).format("YYYY-MM-DD"),
                            period: crawlReq.period
                          };
                          dates.push(date);
                          callback(null, dates);
                        } catch (err) {
                          callback(err);
                        }
                      } else {
                        async.whilst(
                          function() {
                            return startDt.isSameOrBefore(endDt);
                          },
                          function(callback) {
                            if (crawlReq.period === "DD") {
                              const date = {
                                startDt: moment(startDt).format("YYYY-MM-DD"),
                                endDt: moment(startDt).format("YYYY-MM-DD"),
                                period: crawlReq.period
                              };

                              dates.push(date);
                              startDt.add(1, "d");
                              callback(null);
                            } else if (crawlReq.period === "MM") {
                              const date = {
                                startDt: moment(startDt).format("YYYY-MM"),
                                endDt: moment(startDt).format("YYYY-MM"),
                                period: crawlReq.period
                              };

                              dates.push(date);
                              startDt.add(1, "M");

                              callback(null);
                            } else if (crawlReq.period === "YY") {
                              const date = {
                                startDt: moment(startDt).format("YYYY"),
                                endDt: moment(startDt).format("YYYY"),
                                period: crawlReq.period
                              };

                              dates.push(date);
                              startDt.add(1, "y");

                              callback(null);
                            } else if (crawlReq.period === "QQ") {
                              const date = {
                                startDt: moment(startDt).format("YYYY-Q"),
                                endDt: moment(startDt).format("YYYY-Q"),
                                period: crawlReq.period
                              };

                              dates.push(date);
                              startDt.add(1, "Q");

                              callback(null);
                            }
                          },
                          function(err) {
                            if (err) {
                              callback(err);
                            } else {
                              callback(null, dates);
                            }
                          }
                        );
                      }
                    }
                  },
                  function(dates, callback) {
                    logger.debug(
                      `[SQS/producer] Step #3-5. 수집 대상 날짜의 메시지 생성 & MD5 생성`
                    );
                    let messages = [];

                    async.eachSeries(
                      dates,
                      function(date, callback) {
                        const collectCheckFile =
                          __dirname +
                          path.sep +
                          ".." +
                          path.sep +
                          "collect_check" +
                          path.sep +
                          crawlReq.seq +
                          path.sep +
                          "duplication.md5";
                        const collectCheckPath =
                          __dirname +
                          path.sep +
                          ".." +
                          path.sep +
                          "collect_check" +
                          path.sep +
                          crawlReq.seq;

                        async.waterfall(
                          [
                            function(callback) {
                              logger.debug(
                                `[SQS/producer] crawlReq.seq :: ${crawlReq.seq}`
                              );
                              if (crawlReq.check_md5 === "N") {
                                crawlReq.md5 = "";
                                callback(null, false);
                              } else {
                                logger.debug(
                                  `[SQS/producer] Step #3-5-1. MD5 파일 확인`
                                );
                                logger.debug(
                                  `[SQS/producer] collectCheckFile :: ${collectCheckFile}`
                                );
                                fsUtil.getStats(collectCheckFile, function(
                                  err,
                                  exist,
                                  stats
                                ) {
                                  if (err) {
                                    callback(err);
                                  } else {
                                    logger.debug(
                                      `[SQS/producer] exist ? ${exist}`
                                    );
                                    if (exist) {
                                      if (stats.isFile()) {
                                        callback(null, true);
                                      } else {
                                        callback(null, false);
                                      }
                                    } else {
                                      fsUtil.makeDir(
                                        collectCheckPath,
                                        callback
                                      );
                                      callback(null, false);
                                    }
                                  }
                                });
                              }
                            },
                            function(exist, callback) {
                              if (exist) {
                                logger.debug(
                                  `[SQS/producer] Step #3-5-2. MD5 값 읽기`
                                );
                                fsUtil.readFile(collectCheckFile, function(
                                  err,
                                  data
                                ) {
                                  if (err) {
                                    callback(err);
                                  } else {
                                    logger.debug(data);
                                    crawlReq.md5 = data;
                                    callback(null);
                                  }
                                });
                              } else {
                                crawlReq.md5 = "";
                                callback(null);
                              }
                            },
                            function(callback) {
                              logger.debug(
                                `[SQS/producer] Step #3-5-3. Request 메시지 생성`
                              );
                              let progressDt = "";

                              if (mode === "retroactive") {
                                progressDt = date.endDt;
                              }
                              if (date.period === "MM") {
                                progressDt = date.startDt + "-01";
                              } else if (date.period === "QQ") {
                                let spl = date.startDt.split("-");
                                if (spl[1] === "1") {
                                  progressDt = spl[0] + "-01-01";
                                } else if (spl[1] === "2") {
                                  progressDt = spl[0] + "-04-01";
                                } else if (spl[1] === "3") {
                                  progressDt = spl[0] + "07-01";
                                } else {
                                  progressDt = spl[0] + "-10-01";
                                }
                              }

                              const crawlProgress = {
                                requestSeq: crawlReq.seq,
                                startDt: date.startDt,
                                endDt: date.endDt,
                                progressDt: progressDt
                              };
                              crawlProgressDao.selectCrawlProgress(
                                crawlProgress,
                                function(err, results) {
                                  if (err) {
                                    callback(err);
                                  } else {
                                    logger.info(crawlProgress);
                                    let messageFlag = false;

                                    if (results.length === 0) {
                                      messageFlag = true;
                                    } else {
                                      if (results[0].onGoingFlag === "Y") {
                                        messageFlag = true;
                                      } else if (
                                        results[0].status.includes("Error")
                                      ) {
                                        messageFlag = true;
                                      } else if (
                                        results[0].errorMsg !== null &&
                                        results[0].errorMsg !== undefined
                                      ) {
                                        messageFlag = true;
                                      }
                                    }

                                    if (messageFlag) {
                                      const message = {
                                        requestSeq: crawlReq.seq,
                                        customer: crawlReq.customer,
                                        period: date.period,
                                        md5: crawlReq.md5,
                                        startDt: date.startDt,
                                        endDt: date.endDt,
                                        role: crawlReq.role,
                                        channel: crawlReq.channel,
                                        userAgents: crawlReq.userAgents
                                      };
                                      const today = moment();
                                      let endDt;
                                      if (message.period === "DD") {
                                        endDt = moment(message.endDt);
                                      } else if (message.period === "MM") {
                                        endDt = moment(
                                          message.endDt,
                                          "YYYY-MM"
                                        );
                                      } else if (message.period === "YY") {
                                        endDt = moment(message.endDt, "YYYY");
                                      } else if (message.period === "QQ") {
                                        endDt = moment(message.endDt, "YYYY-Q");
                                      }

                                      // ONGOING 여부 확인
                                      if (
                                        endDt.format("YYYYMMDD") ===
                                        today.format("YYYYMMDD")
                                      ) {
                                        message.onGoingFlag = "Y";
                                      } else {
                                        message.onGoingFlag = "N";
                                      }

                                      // 키워드 존재 여부 확인
                                      if (crawlReq.typeCd === "CRT002") {
                                        message.keyword = crawlReq.keyword;
                                      }

                                      // 큐 설정
                                      message.groupCount = groupCount;

                                      messages.push(
                                        JSON.parse(JSON.stringify(message))
                                      );
                                      callback(null);
                                    } else {
                                      callback(null);
                                    }
                                  }
                                }
                              );
                            }
                          ],
                          callback
                        );
                      },
                      function(err) {
                        if (err) {
                          callback(err);
                        } else {
                          callback(null, messages);
                        }
                      }
                    );
                  },
                  function(messages, callback) {
                    logger.debug(
                      `[SQS/producer] Step #3-6. SQS 메시지 처리 및 상태 업데이트`
                    );
                    if (messages.length === 0) {
                      callback("NO_MESSAGES");
                    } else {
                      async.eachSeries(
                        messages,
                        function(message, callback) {
                          const payload = {
                            DelaySecond: 2,
                            MessageBody: JSON.stringify(message),
                            QueueUrl: queryUrl + sqsName
                          };

                          logger.debug(payload);
                          Sqs.sendMessage(payload, function(err, result) {
                            if (err) {
                              callback(err);
                            } else {
                              logger.debug(
                                `[SQS/producer] 발송 결과: ${JSON.stringify(
                                  result
                                )}`
                              );

                              const crawlProgress = {
                                requestSeq: message.requestSeq,
                                period: message.period,
                                startDt: message.startDt,
                                endDt: message.endDt,
                                onGoingFlag: message.onGoingFlag,
                                errorMsg: message.errorMsg
                              };

                              if (message.role === "linkCollector") {
                                crawlProgress.status = "linkRequest";
                              } else if (message.role === "linkDocCollector") {
                                crawlProgress.status = "linkDocRequest";
                              } else if (message.role === "apiCollector") {
                                crawlProgress.status = "apiRequest";
                              }

                              if (mode === "retroactive") {
                                crawlProgress.progressDt = message.endDt;
                              }

                              logger.debug(crawlProgress);
                              crawlProgressDao.upsertCrawlProgress(
                                crawlProgress,
                                callback
                              );
                            }
                          });
                        },
                        callback
                      );
                    }
                  },
                  function(callback) {
                    logger.debug(
                      `[SQS/producer] Step #3-7. 수집 요청 건 상태 업데이트`
                    );
                    logger.debug(
                      `[SQS/producer] crawlReq.status ::: ${crawlReq.status}`
                    );

                    if (
                      crawlReq.status !== "CRS005" &&
                      crawlReq.status !== "CRS006"
                    ) {
                      crawlReq.status = "CRS002";
                      logger.debug(crawlReq);
                      crawlRequestDao.updateCrawlRequest(crawlReq, callback);
                    } else {
                      callback(null);
                    }
                  }
                ],
                callback
              );
            },
            callback
          );
        }
      ],
      function(err) {
        if (err) {
          if (err === "DATE_TOO_EARLY") {
            logger.debug("[SQS/producer] 수집 시작일 미도래 건");
            callback(null);
          } else if (err === "NO_MESSAGES") {
            logger.debug("[SQS/producer] 수집 요청 건 없음");
            callback(null);
          } else {
            callback(err);
          }
        } else {
          callback(null);
        }
      }
    );
  };

  return {
    produce: produce
  };
};
