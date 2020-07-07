var mysqlPool = require("../lib/mysqlPool");
var camelize = require("camelize");

var crawlRequestDao = (function() {
  // 수집 요청 리스트 조회
  var selectCrawlRequestList = function(
    crawlReq,
    s_year,
    s_month,
    s_day,
    hours,
    mode,
    callback
  ) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var params = [];
        var query =
          "select " +
          "  cr.seq, " +
          "  cr.channel_seq, " +
          "  cr.type_cd, " +
          "  cr.status, " +
          "  cr.period, " +
          "  cr.start_dt, " +
          "  cr.end_dt, " +
          "  cr.keyword, " +
          "  c.name as customer, " +
          "  cr.check_md5, " +
          "  cr.mode " +
          "from tb_crawl_request cr, tb_customer c " +
          "where " +
          "  cr.status in ('CRS001', 'CRS002', 'CRS005') " + // 대기, 진행, 에러
          // "  cr.status in ('CRS006') " + // CRS006 >> test auto일 경우 바꿔줘야함.
          "  and cr.customer_seq=c.seq";

        if (mode === "auto") {
          query +=
            " and cr.mode = 'auto' and cr.schedules like '%" + hours + "%'";
        } else if (mode === "instagram") {
          query +=
            " and cr.mode = 'instagram' and cr.schedules like '%" +
            hours +
            "%'";
        } else if (mode === "cafe") {
          query +=
            " and cr.mode = 'cafe' and cr.schedules like '%" + hours + "%'";
        } else if (mode === "portal") {
          query +=
            " and cr.mode = 'portal' and cr.schedules like '%" + hours + "%'";
        } else if (mode === "community") {
          query +=
            " and cr.mode = 'community' and cr.schedules like '%" +
            hours +
            "%'";
        } else if (mode === "retroactive") {
          query +=
            " and cr.mode = 'retroactive' and cr.schedules like '%" +
            hours +
            "%'";
        } else if (mode === "clien") {
          query +=
            " and cr.mode = 'clien' and cr.schedules like '%" + hours + "%'";
        }
        if (s_year !== null && s_year !== undefined) {
          query +=
            " and (cr.year_schedules like '%" +
            s_year +
            "%' or cr.year_schedules = '*')";
        }

        if (s_month !== null && s_month !== undefined) {
          query +=
            " and (cr.month_schedules like '%" +
            s_month +
            "%' or cr.month_schedules = '*')";
        }

        if (s_day !== null && s_day !== undefined) {
          query +=
            " and (cr.day_schedules like '%" +
            s_day +
            "%' or cr.day_schedules = '*')";
        }

        if (crawlReq.seq !== undefined && crawlReq.seq !== 0) {
          query += " and cr.seq=?";
          params.push(crawlReq.seq);
        }

        conn.query(query, params, function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null, camelize(results));
          }
        }); // query
      }
    }); // getConnection
  }; // selectCrawlRequestList

  var selectCrawlRequestList2 = function(
    crawlReq,
    s_year,
    s_month,
    s_day,
    hours,
    mode,
    callback
  ) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var params = [];
        var query =
          "select " +
          "  cr.seq, " +
          "  cr.channel_seq, " +
          "  cr.type_cd, " +
          "  cr.status, " +
          "  cr.period, " +
          "  cr.start_dt, " +
          "  cr.end_dt, " +
          "  cr.keyword, " +
          "  c.name as customer, " +
          "  cr.check_md5, " +
          "  cr.mode " +
          "from tb_crawl_request cr, tb_customer c " +
          "where " +
          "  cr.status in ('CRS001', 'CRS002', 'CRS005') " + // 대기, 진행, 에러
          // "  cr.status in ('CRS006') " + // CRS006 >> test auto일 경우 바꿔줘야함.
          "  and cr.customer_seq=c.seq";

        if (mode === "auto") {
          query +=
            " and cr.mode = 'auto' and cr.schedules like '%" + hours + "%'";
        } else if (mode === "instagram") {
          query +=
            " and cr.mode = 'instagram' and cr.schedules like '%" +
            hours +
            "%'";
        } else if (mode === "cafe") {
          query +=
            " and cr.mode = 'cafe' and cr.schedules like '%" + hours + "%'";
        } else if (mode === "portal") {
          query +=
            " and cr.mode = 'portal' and cr.schedules like '%" + hours + "%'";
        } else if (mode === "community") {
          query +=
            " and cr.mode = 'community' and cr.schedules like '%" +
            hours +
            "%'";
        } else if (mode === "retroactive") {
          query +=
            " and cr.mode = 'retroactive' and cr.schedules like '%" +
            hours +
            "%'";
        } else if (mode === "clien") {
          query +=
            " and cr.mode = 'clien' and cr.schedules like '%" + hours + "%'";
        }
        if (s_year !== null && s_year !== undefined) {
          query +=
            " and (cr.year_schedules like '%" +
            s_year +
            "%' or cr.year_schedules = '*')";
        }

        if (s_month !== null && s_month !== undefined) {
          query +=
            " and (cr.month_schedules like '%" +
            s_month +
            "%' or cr.month_schedules = '*')";
        }

        if (s_day !== null && s_day !== undefined) {
          query +=
            " and (cr.day_schedules like '%" +
            s_day +
            "%' or cr.day_schedules = '*')";
        }

        if (crawlReq.seq !== undefined && crawlReq.seq !== 0) {
          query += " and cr.seq=?";
          params.push(crawlReq.seq);
        }
        conn.query(query, params, function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null, camelize(results));
          }
        }); // query
      }
    }); // getConnection
  }; // selectCrawlRequestList2

  // 수집 요청 조회
  var selectCrawlRequest = function(crawlReq, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "select channel_seq, type_cd, period, start_dt, end_dt, keyword from tb_crawl_request where seq=?";

        conn.query(query, [crawlReq.seq], function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null, camelize(results));
          }
        }); // query
      }
    }); // getConnection
  }; // selectCrawlRequest

  // 수집 요청 상태 업데이트
  var updateCrawlRequest = function(crawlReq, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "update tb_crawl_request set status=?, upd_dt=now() where seq=?";

        conn.query(query, [crawlReq.status, crawlReq.seq], function(
          err,
          results,
          fields
        ) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null);
          }
        }); // query
      }
    }); // getConnection
  }; // updateCrawlRequest

  return {
    selectCrawlRequestList: selectCrawlRequestList,
    selectCrawlRequestList2: selectCrawlRequestList2,
    selectCrawlRequest: selectCrawlRequest,
    updateCrawlRequest: updateCrawlRequest
  };
})();

if (exports) {
  module.exports = crawlRequestDao;
}
