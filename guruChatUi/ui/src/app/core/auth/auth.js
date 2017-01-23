;(function (angular) {
  "use strict";

  var endPoint = "/api/auth";

  angular.module("auth", []).service("Auth", ["$q", "$http", "Session",

    function ($q, $http, Session) {

      var deferredStatus = null;

      this.login = function (credentials) {

        deferredStatus = null;

        return $http.post(endPoint + "/login/", credentials)
                    .then(function (resp) {

          resp = resp.data;

          Session.set(resp.result);
        });
      };

      this.isAuthorized = function () {

        var session = Session.get();

        return !!(session && session.userId);
      };

      this.logout = function () {

        return $http.get(endPoint + "/logout/").then(function () {

          Session.unset();
          deferredStatus = null;
        });
      };

      this.status = function () {

        if (deferredStatus) {

          return deferredStatus.promise;
        }

        deferredStatus = $q.defer();

        $http.get(endPoint + "/status/").then(function (resp) {

          resp = resp.data;

          if (resp.result && resp.result.user) {

            Session.set(resp.result.user);
          }

          deferredStatus.resolve(resp.result);
        });

        return deferredStatus.promise;
      };
  }]);

}(window.angular));
