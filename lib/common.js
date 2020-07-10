const common = (function() {
  let timeout = (timeout, callback) => {
    if (process.platform === "win32") {
      setTimeout(function() {
        callback(null);
      }, timeout * 1000);
    } else {
      require("sleep").sleep(timeout);
      callback(null);
    }
  };
  return { timeout: timeout };
})();

if (exports) {
  module.exports = common;
}
