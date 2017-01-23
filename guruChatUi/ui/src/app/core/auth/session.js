;(function (angular) {
  "use strict";

  angular.module("auth").service("Session", function () {

    var that = this;

    function resetSession () {

      angular.extend(that, {

        "userId"  : null,
        "userName": null,
        "email"   : null
      });
    }

    resetSession();

    this.get = function () {

      return {

        "userId"  : this.userId,
        "userName": this.userName,
        "email"   : this.email
      };
    };

    this.set = function (data) {

      this.userId   = data.userId;
      this.userName = data.userName;
      this.email    = data.email;
    };

    this.unset = resetSession;
  });

}(window.angular));
