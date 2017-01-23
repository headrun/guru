;(function (angular) {
  "use strict";

  var endPoint = "/api";

  angular.module("csrf", []).service("Csrf", ["$q", "$http",
    function ($q, $http) {

      this.get = function () {

        var deferred = $q.defer();

        $http.get(endPoint + "/csrf").then(function (resp) {

          deferred.resolve({});
        });

        return deferred.promise;
      }
    }
  ]);
}(window.angular));
