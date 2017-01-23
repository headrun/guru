;(function (angular) {
  "use strict";

  var highchartOptions = {

    "chart":{
      "alignTicks":false,
      "backgroundColor":null,
      "borderColor":"#2A2A2A",
      "marginTop":20,
      "plotBackgroundColor":null,
      "plotBorderWidth":null,
      "plotShadow":false,
      "spacingLeft":5,
      "spacingRight":20,
      "spacingTop":20,
      "style":{
        "cursor":"auto"
      },
    },
    "colors": [
      "#5290e9", //blue
      "#71b37c", //green
      "#ec932f", //Orange
      "#e14d57", //red
      "#965994", //violet
      "#9d7952", //brown
      "#cc527b",
      "#33867f",
      "#ada434"
    ],
    "credits":{
      "enabled":false
    },
    "plotOptions":{
      "area":{
        "stacking":null,
        "shadow":false
      },
      "areaspline":{
        "shadow":false,
        "stacking":null
      },
      "column":{
        "borderWidth":0,
        "groupPadding":0.07,
        "pointPadding":0.05,
        "shadow":false,
        "stacking":null
      },
      "pie": {
        "dataLabels": {

            "color": "#707070",
            "format": '{y}',
            "distance": 8
        },
        "showInLegend": true
      },
      "line":{
        "shadow":false,
        "stacking":null
      },
      "series":{
        "animation":true,
        "events":{

        },
        "fillOpacity":0.2,
        "pointStart":0
      },
      "spline":{
        "shadow":false,
        "stacking":null
      },
      "bubble": {
        "cursor": "pointer",
        "marker": {
          "states": {
            "select": {
              "fillColor": "#e14d57",
              "lineColor": "#9d1b23"
            }
          }
        }
      }
    },
    "legend": {

      "itemStyle": {"color": "#707070"}
    },
    "xAxis": {

      "type": "category"
    },
    "yAxis": {

      "allowDecimals": false,
      "gridLineColor": "transparent"
    },
    "title":{
      "text":null
    },
    "tooltip":{
      "borderRadius":4,
      "borderWidth":2,
      "style":{
        "fontFamily":"Arial,  Helvetica, sans-serif",
        "fontSize":"13px",
        "cursor":"default"
      },
      "shared": true
    }
  };

  var Hc = window.Highcharts;

  angular.module("highcharts", [])
         .service("Highcharts", function () {

           var that = this;

           this.options = highchartOptions;

           this.render = function (element, options) {

             options = options || {};

             options = angular.extend({}, this.options, options);

             options.chart.renderTo = element;

             return new Hc.Chart(options);
           };
         });

}(window.angular));
