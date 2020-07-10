var mysqlPool = require("../lib/mysqlPool");

var workerDao = (function() {
  /**
   * 클라이언트의 상태 업데이트
   * @param  {jsonObj}   worker   워커 객체
   * @param  {Function} callback 콜백 함수
   */
  var upsertWokerStatus = function(worker, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "insert into tb_worker_info (ip, host, status, error_msg) values (?, ?, ?, ?) " +
          "on duplicate key update host=?, status=?, error_msg=?, upd_dt=NOW()";

        conn.query(
          query,
          [
            worker.ip,
            worker.host,
            worker.status,
            worker.errorMsg,
            worker.host,
            worker.status,
            worker.errorMsg
          ],
          function(err, results, fields) {
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
  }; // upsertWokerStatus

  return {
    upsertWokerStatus: upsertWokerStatus
  };
})();

if (exports) {
  module.exports = workerDao;
}
