var mysqlPool = require('../lib/mysqlPool');
var camelize = require('camelize');

var userAgentDao = (function() {
	// userAgent 리스트 조회
	var selectUserAgentList = function(callback) {
		mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
				var query = "select seq, useragent from tb_crawl_useragent where use_yn='Y' order by seq desc";

				conn.query(query, function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null, camelize(results));
          }
        });
			}
		});
	};

	// userAgent 등록
	var insertUserAgent = function(userAgent, callback) {
		mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
				var query = "insert into tb_crawl_useragent (useragent) values (?)";

				conn.query(query, [userAgent.userAgent], function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null);
          }
        });
			}
		});
	};

	// userAgent 삭제
	var deleteUserAgent = function(userAgent, callback) {
		mysqlPool.getConnection(function(err, conn) {
			if (err) {
				callback(err);
			} else {
				var query = "delete from tb_crawl_useragent where seq=?";

				conn.query(query, [userAgent.seq], function(err, results, fields) {
					if (err) {
						callback(err);
					} else {
						conn.release();
						callback(null);
					}
				});
			}
		});
	};

	return {
		selectUserAgentList: selectUserAgentList,
		insertUserAgent: insertUserAgent,
		deleteUserAgent: deleteUserAgent
	};
})();

if (exports) {
    module.exports = userAgentDao;
}
