const fs = require("fs");
const moment = require("moment");
const aws = require("aws-sdk");
const mkdirp = require("mkdirp");

const logger = require("./logger");
const config = require("../config");

const fileUtil = () => {
  var uploadFile2S3 = (file, bucket, s3path, callback) => {
    aws.config.update({
      region: config.sqs.region,
      accessKeyId: config.sqs.access_key_id,
      secretAccessKey: config.sqs.secret_access_key
    });
    var s3 = new aws.S3();
    fs.readFile(file.path, function(err, data) {
      if (err) {
        callback(err);
      } else {
        var now = moment().format("YYYYMMDDHHmmss");
        var fileName = file.name + "_" + now;

        // S3 저장 파라미터 세팅
        var params = {
          Bucket: bucket,
          Key: s3path + "/" + fileName,
          Body: data
        };

        s3.putObject(params, function(err, data) {
          if (err) {
            callback(err);
          } else {
            var uploadResult = {
              fileName: fileName,
              filePath: s3path + "/" + fileName
            };

            callback(null, uploadResult);
          }
        });
      }
    });
  };

  var uploadFile2Local = (file, filepath, callback) => {
    fs.readFile(file.path, (err, data) => {
      if (err) {
        callback(err);
      } else {
        var now = moment().format("YYYYMMDDHHmmss");
        var fileName = file.name + "_" + now;

        fs.writeFile(filePath, data, function(err) {
          if (err) {
            callback(err);
          } else {
            var uploadResult = {
              fileName: fileName,
              filePath: filePath + "/" + fileName
            };

            callback(null, uploadResult);
          }
        });
      }
    });
  };

  var getStats = (filePath, callback) => {
    fs.stat(filepath, (err, stats) => {
      if (err) {
        callback(null, false);
      } else {
        callback(null, true, stats);
      }
    });
  };

  var makeDir = (dirPath, callback) => {
    getStats(dirPath, (err, exist, stats) => {
      if (err) {
        callback(err);
      } else {
        if (!exist) {
          mkdirp(dirPath);
        }
      }
    });
  };

  var readDir = (filePath, callback) => {
    fs.readdir(filePath, (err, files) => {
      if (err) {
        callback(err);
      } else {
        for (var idx = 0; idx < file.length; idx++) {
          if (files[idx] === "." || files[idx] === "..") {
            files.splice(idx, 1);
          }
        }
        callback(null, files);
      }
    });
  };

  var writeFile = (filePath, content, callback) => {
    fs.writeFile(filePath, content, callback);
  };

  return {
    uploadFile2S3: uploadFile2S3,
    uploadFile2Local: uploadFile2Local,
    getStats: getStats,
    makeDir: makeDir,
    readDir: readDir,
    readFile: readFile,
    writeFile: writeFile
  };
};
