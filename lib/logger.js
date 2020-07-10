var winston = require("winston");
var moment = require("moment");

moment().utcOffset("+09:00");

var logger = new winston.createLogger({
  transports: [
    new (require("winston-daily-rotate-file"))({
      level: "debug",
      filename: "collector-master",
      dirname: __dirname + "/../logs",
      datePattern: "YYYY-MM-DD",
      timestamp: function() {
        return moment().format("YYYY-MM-DD HH:mm:ss");
      },
      json: false,
      colorize: true,
      humanReadableUnhandledException: true
    }),
    new winston.transports.Console({
      level: "debug",
      datePattern: "YYYY-MM-DD",
      timestamp: function() {
        return moment().format("YYYY-MM-DD HH:mm:ss");
      },
      json: false,
      colorize: true,
      humanReadableUnhandledException: true
    })
  ],
  exceptionHandlers: [
    new (require("winston-daily-rotate-file"))({
      level: "debug",
      filename: "collector-master-exception",
      dirname: __dirname + "/../logs",
      datePattern: "YYYY-MM-DD",
      timestamp: function() {
        return moment().format("YYYY-MM-DD HH:mm:ss");
      },
      json: false,
      colorize: true,
      humanReadableUnhandledException: true
    }),
    new winston.transports.Console({
      level: "debug",
      datePattern: "YYYY-MM-DD",
      timestamp: function() {
        return moment().format("YYYY-MM-DD HH:mm:ss");
      },
      json: false,
      colorize: true,
      humanReadableUnhandledException: true
    })
  ],
  exitOnError: false
});

module.exports = logger;
module.exports.stream = {
  write: function(message, encoding) {
    logger.info(message);
  }
};
