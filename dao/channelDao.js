var mysqlPool = require("../lib/mysqlPool");
var camelize = require("camelize");

var channelDao = (function() {
  /**
   * 수집처 리스트 조회
   * @param  {Function} callback 콜백 함수
   */
  var selectChannelList = function(callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query = "select name, url from tb_crawl_channel order by seq desc";

        conn.query(query, function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null, camelize(results));
          }
        }); // query
      }
    }); // getConnection
  }; // selectChannelList

  /**
   * 수집처 등록
   * @param  {jsonObj}   channelInfo 수집 채널 정보
   * @param  {Function} callback    콜백 함수
   */
  var insertChannelInfo = function(channelInfo, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query = "insert into tb_crawl_channel (name, url) values (?, ?)";

        conn.query(query, [channelInfo.name, channelInfo.url], function(
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
  }; // insertChannelInfo

  /**
   * 수집처 조회
   * @param  {jsonObj}   channelInfo 	수집 채널 정보
   * @param  {Function} callback    	콜백 함수
   */
  var selectChannelInfo = function(channelInfo, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "select cc.name, cc.url, cc.chrome_use_yn as chromeUseYn, cr.name as fileName, cr.path as filePath, cc.login_seq, (SELECT cl.id FROM tb_crawl_login cl WHERE cl.seq=cc.login_seq) AS id, (SELECT cl.password FROM tb_crawl_login cl WHERE cl.seq=cc.login_seq) AS password  from tb_crawl_channel cc, tb_crawl_rule cr where cc.seq=? and cc.rule_seq=cr.seq";

        conn.query(query, [channelInfo.seq], function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null, camelize(results));
          }
        }); // query
      }
    }); // getConnection
  }; // selectChannelInfo

  /**
   * 수집처 삭제
   * @param  {jsonObj}   channelInfo 수집 채널 정보
   * @param  {Function} callback    콜백 함수
   */
  var deleteChannelInfo = function(channelInfo, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query = "delete from tb_crawl_channel where seq=?";

        conn.query(query, [channelInfo.seq], function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null);
          }
        }); // query
      }
    }); // getConnection
  }; // deleteChannelInfo

  /**
   * 로그인 정보 조회
   * @param  {jsonObj}   channelInfo 채널 정보
   * @param  {Function} callback    콜백 함수
   */
  var selectChannelLoginInfo = function(channelInfo, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query = "select seq, id, password from tb_crawl_login where seq=?";

        conn.query(query, [channelInfo.loginSeq], function(
          err,
          results,
          fields
        ) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null, results);
          }
        }); // query
      }
    }); // getConnection
  }; // selectChannelLoginInfo

  /**
   * 채널 엔진 정보 가져오기
   * @param  {jsonObj}   channelInfo 수집 채널 정보
   * @param  {Function} callback    콜백 함수
   */
  var selectChannelEngineList = function(channelInfo, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "select type_cd, name as fileName, path as filePath from tb_crawl_channel_engine where channel_seq=?";

        conn.query(query, [channelInfo.seq], function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null, camelize(results));
          }
        }); // query
      }
    }); // getConnection
  }; // selectChannelEngineList

  /**
   * 채널 inject scritp 정보 가져오기
   * @param  {jsonObj}   channelInfo 수집 채널 정보
   * @param  {Function} callback    콜백 함수
   */
  var selectChannelInjectScriptList = function(channelInfo, callback) {
    mysqlPool.getConnection(function(err, conn) {
      if (err) {
        callback(err);
      } else {
        var query =
          "select ci.name as fileName, ci.path as filePath from tb_crawl_channel_inject cci, tb_crawl_inject ci where channel_seq=? and cci.injectscript_seq=ci.seq";

        conn.query(query, [channelInfo.seq], function(err, results, fields) {
          if (err) {
            callback(err);
          } else {
            conn.release();
            callback(null, camelize(results));
          }
        }); // query
      }
    }); // getConnection
  }; // selectChannelInjectScriptList

  return {
    selectChannelList: selectChannelList,
    insertChannelInfo: insertChannelInfo,
    selectChannelInfo: selectChannelInfo,
    deleteChannelInfo: deleteChannelInfo,
    selectChannelLoginInfo: selectChannelLoginInfo,
    selectChannelEngineList: selectChannelEngineList,
    selectChannelInjectScriptList: selectChannelInjectScriptList
  };
})();

if (exports) {
  module.exports = channelDao;
}
