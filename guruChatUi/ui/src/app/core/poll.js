;(function (angular) {
  "use strict";

  angular.module("poll", []).service("Poll", ["$q", "$http",

    function ($q, $http) {

      function poll (url, params, method, minWaitTime, validator) {

        var deferred = $q.defer();

        if (!url) {

          throw new Error("URL missing");
        }

        params   = params || {};
        method   = method || "get";

        var pollingStopped = false;
        var req, resolvedData, timer;

        function stopPolling () {

          if (pollingStopped) {

           return;
          }

          pollingStopped = true;

          window.clearTimeout(timer);

          resolvedData.onPollStop = true;

          deferred.resolve(resolvedData);
        }

        (function _poll() {

          var startTime = new Date();

          if (pollingStopped) {

            return;
          }

          req = $http[method](url, params).then(function (resp) {

			resolvedData = resp;

			if (pollingStopped) {

			  return;
			}

			if (validator && !validator(resp)) {

			  return stopPolling();
			}

			if (minWaitTime) {

			  var diffTime = (new Date()) - startTime;

			  if (diffTime < minWaitTime) {

			    timer = window.setTimeout(_poll, minWaitTime - diffTime);
			  } else {

			    _poll();
			  }
			} else {

			  _poll();
			}
          }, function () {

			if (pollingStopped) {

			  return;
			}

			resolvedData = {"error": 1, "msg": "An Error Occured during poll"};
			stopPolling();
          });
        })();

        return {"stopPolling": stopPolling,
                "deferred": deferred.promise};
      }

	  this.poll = poll;
    }
  ]);

}(window.angular));
