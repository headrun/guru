var system = require('system');
var args = system.args;
var page = require('webpage').create();
page.viewportSize = { width:896, height:200};
//page.clipRect = { top: 0, left: 0, width: 1024, height: 768 };
page.zoomFactor = 0.7;
page.open(args[1], function() {

 window.setTimeout(function () {
    page.render(args[2]);
    phantom.exit();
    }, 1000);
    });
