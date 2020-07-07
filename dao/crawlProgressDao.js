var camelize = require("camelize");
var moment = require("moment");
var mysqlPool = require("../lib/mysqlPool");

var crawlProgressDao = (function() {
  /**
   * 수집 상태 조회
   * @param  {jsonObj}   crawlProgress 수집 상태 객체
   * @param  {Function} callback       콜백 함수
   */
  var selectCrawlProgress = function(crawlProgress, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        if (
          crawlProgress.progressDt === null ||
          crawlProgress.progressDt === undefined ||
          crawlProgress.progressDt.length < 1
        ) {
          crawlProgress.progressDt = crawlProgress.startDt;
        }
        var query =
          "select start_dt, end_dt, status, on_going_flag, error_msg from tb_crawl_progress where progress_dt=? and request_seq=? and start_dt=? and end_dt=?";

        conn.query(
          query,
          [
            crawlProgress.progressDt,
            crawlProgress.requestSeq,
            crawlProgress.startDt,
            crawlProgress.endDt
          ],
          function(err, results, fields) {
            if (err) {
              callback(err);
            } else {
              conn.release();
              callback(null, camelize(results));
            }
          }
        ); // query
      }
    }); // getConnection
  }; // selectCrawlProgress

  // 상태값에 의한 수집 현황 조회
  var selectCrawlProgressByStatus = function(crawlProgress, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "select status, count(seq) as cnt from tb_crawl_progress where request_seq=? and progress_dt between ? and ? group by status";

        conn.query(
          query,
          [
            crawlProgress.requestSeq,
            crawlProgress.startDt,
            crawlProgress.endDt
          ],
          function(err, results, fields) {
            if (err) {
              callback(err);
            } else {
              conn.release();
              callback(null, camelize(results));
            }
          }
        ); // query
      }
    }); // getConnection
  }; // selectCrawlProgressByStatus

  // 온고잉플레그에 의한 수집 현황 조회
  var selectCrawlProgressByOnGoingFlag = function(crawlProgress, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "select on_going_flag, count(seq) as cnt from tb_crawl_progress where request_seq=? and progress_dt between ? and ? group by on_going_flag";

        conn.query(
          query,
          [
            crawlProgress.requestSeq,
            crawlProgress.startDt,
            crawlProgress.endDt
          ],
          function(err, results, fields) {
            if (err) {
              callback(err);
            } else {
              conn.release();
              callback(null, camelize(results));
            }
          }
        ); // query
      }
    }); // getConnection
  }; // selectCrawlProgressByOnGoingFlag

  // 수집 현황 추가
  var insertCrawlProgress = function(crawlProgress, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "insert into tb_crawl_progress (progress_dt, request_seq, start_dt, end_dt, status, on_going_flag) values (?, ?, ?, ?, ?, ?)";

        conn.query(
          query,
          [
            crawlProgress.startDt,
            crawlProgress.requestSeq,
            crawlProgress.startDt,
            crawlProgress.endDt,
            crawlProgress.status,
            crawlProgress.onGoingFlag
          ],
          function(err, results, fields) {
            if (err) {
              callback(err);
            } else {
              conn.release();
              callback(null);
            }
          }
        );
      }
    });
  };

  /**
   * 수집 상태 업데이트
   * @param  {jsonObj}   crawlProgress 수집 상태 객체
   * @param  {Function} callback       콜백 함수
   */
  var updateCrawlProgress = function(crawlProgress, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "update tb_crawl_progress set status=?, error_msg=?, upd_dt=now() where progress_dt=? and request_seq=? and start_dt=? and end_dt=?";

        if (
          crawlProgress.progressDt === null ||
          crawlProgress.progressDt === undefined ||
          crawlProgress.progressDt.length < 1
        ) {
          crawlProgress.progressDt = crawlProgress.startDt;
        }
        if (crawlProgress.period === "MM") {
          crawlProgress.progressDt = moment(
            crawlProgress.startDt,
            "YYYY-MM"
          ).format("YYYY-MM-DD");
        } else if (crawlProgress.period === "YY") {
          crawlProgress.progressDt = moment(
            crawlProgress.startDt,
            "YYYY"
          ).format("YYYY-MM-DD");
        } else if (crawlProgress.period === "QQ") {
          crawlProgress.progressDt = moment(
            crawlProgress.startDt,
            "YYYY-Q"
          ).format("YYYY-MM-DD");
        }

        conn.query(
          query,
          [
            crawlProgress.status,
            crawlProgress.errorMsg,
            crawlProgress.progressDt,
            crawlProgress.requestSeq,
            crawlProgress.startDt,
            crawlProgress.endDt
          ],
          function(err, results) {
            if (err) {
              callback(err);
            } else {
              conn.release();
              callback(null);
            }
          }
        ); // query
      }
    }); // getConnection
  }; // updateCrawlProgress

  /**
   * 수집 상태 업설트
   * @param  {jsonObj}   crawlProgress 수집 상태 객체
   * @param  {Function} callback      콜백 함수
   */
  var upsertCrawlProgress = function(crawlProgress, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "insert into tb_crawl_progress (progress_dt, request_seq, start_dt, end_dt, status, on_going_flag, error_msg) values (?, ?, ?, ?, ?, ?, ?) " +
          "on duplicate key update status=?, on_going_flag=?, error_msg=?, upd_dt=NOW()";

        if (
          crawlProgress.progressDt === null ||
          crawlProgress.progressDt === undefined ||
          crawlProgress.progressDt.length < 1
        ) {
          crawlProgress.progressDt = crawlProgress.startDt;
        }
        if (crawlProgress.period === "MM") {
          crawlProgress.progressDt = moment(
            crawlProgress.startDt,
            "YYYY-MM"
          ).format("YYYY-MM-DD");
        } else if (crawlProgress.period === "YY") {
          crawlProgress.progressDt = moment(
            crawlProgress.startDt,
            "YYYY"
          ).format("YYYY-MM-DD");
        } else if (crawlProgress.period === "QQ") {
          crawlProgress.progressDt = moment(
            crawlProgress.startDt,
            "YYYY-Q"
          ).format("YYYY-MM-DD");
        }

        conn.query(
          query,
          [
            crawlProgress.progressDt,
            crawlProgress.requestSeq,
            crawlProgress.startDt,
            crawlProgress.endDt,
            crawlProgress.status,
            crawlProgress.onGoingFlag,
            crawlProgress.errorMsg,
            crawlProgress.status,
            crawlProgress.onGoingFlag,
            crawlProgress.errorMsg
          ],
          function(err, results) {
            if (err) {
              callback(err);
            } else {
              conn.release();
              callback(null);
            }
          }
        ); // query
      }
    }); // getConnection
  }; // upsertCrawlProgress

  /**
   * onginFlag 업데이트
   * @param  {jsonObj}   crawlProgress 수집 상태 객체
   * @param  {Function} callback       콜백 함수
   */
  var updateOngoingFlags = function(crawlProgress, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "update tb_crawl_progress set on_going_flag=? where request_seq=? and start_dt=? and end_dt=?";

        conn.query(
          query,
          [
            crawlProgress.onGoingFlag,
            crawlProgress.requestSeq,
            crawlProgress.startDt,
            crawlProgress.endDt
          ],
          function(err, results) {
            if (err) {
              callback(err);
            } else {
              conn.release();
              callback(null);
            }
          }
        ); // query
      }
    }); // getConnection
  }; // updateOngoingFlags

  return {
    selectCrawlProgress: selectCrawlProgress,
    selectCrawlProgressByStatus: selectCrawlProgressByStatus,
    selectCrawlProgressByOnGoingFlag: selectCrawlProgressByOnGoingFlag,
    insertCrawlProgress: insertCrawlProgress,
    updateCrawlProgress: updateCrawlProgress,
    upsertCrawlProgress: upsertCrawlProgress,
    updateOngoingFlags: updateOngoingFlags
  };
})();

if (exports) {
  module.exports = crawlProgressDao;
}
