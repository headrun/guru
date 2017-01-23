;(function (angular, global) {
  "use strict";

  angular.module("chat")
         .directive("messages", function () {

           function controller (scope) {

             var unwatch = scope.$watchCollection("messages",
               function () {

                 return scope.onMessage && scope.onMessage();
               }
             );

             this.$onDestroy = function () {

               return unwatch();
             };

           }

           function link (scope, ele, attrs) {

             scope.onMessage = function () {

               window.setTimeout(function () {

                 ele[0].scrollTop = ele.children()[0].clientHeight;
               }, 500);
             };
           }

           return {

             "scope": {
               "messages": "<"
             },
             "restrict": "E",
             "templateUrl": global.templateDir + "/messages.html",
             "link": link,
             "controller": ["$scope", controller]
           };
         });

}(window.angular, window.GURU));
