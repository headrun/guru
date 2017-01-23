;(function (angular, global) {
  "use strict";

  angular.module("highcharts")
         .component("highcharts", {

           "templateUrl": global.templateDir + "/highcharts.html",
           "controller" : ["Highcharts", "$element", "$scope", "$q",

           function (Highcharts, $element, $scope, $q) {

             $scope.options = this.options;

             var highchartsEle, $ele = $element.children()[0];

             var unWatch;

             var rendered = $q.defer();

             this.onRender = function () {
            
               rendered.resolve();
             };

             this.$onInit = function () {

               unWatch = $scope.$watch("options",
               function(newVal) {

                 if (highchartsEle) {

                   highchartsEle.destroy();
                 }

                 rendered.promise.then(function () {

                   highchartsEle = Highcharts.render($ele,
                                                     JSON.parse($scope.options));
                 });
               });
             };

             this.$onDestroy = function () {

               rendered.reject();
               return unWatch && unWatch();
             }
           }],
           "bindings": {

             "options": "@"
           }
         })
}(window.angular, window.GURU));
