const needle = require("needle");

const config = require("../config");

const slack = (() => {
  const sendMessage = (message, callback) => {
    var webHookUri = config.slack.webHookUri;

    var options = {
      headers: { "Content-Type": "application/x-www-form-urlencoded" }
    };

    var payload = {
      attachments: [
        {
          fallback: message.title,
          color: message.color,
          fields: [
            {
              title: message.title,
              value: message.value,
              short: false
            }
          ]
        }
      ]
    };

    needle.post(
      webHookUri,
      `payload=${JSON.stringify(payload)}`,
      options,
      function(err, response) {
        if (err) {
          callback(err);
        } else {
          callback(null);
        }
      }
    );
  };

  return {
    sendMessage: sendMessage
  };
})();

if (exports) {
  module.exports = slack;
}
