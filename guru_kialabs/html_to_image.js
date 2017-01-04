
var system = require('system');
var args = system.args;
if (args.length < 3)
    {  
        console.log('accepts two arguments: html_in image_out');
        phantom.exit(); 
    }

input = args[1];
output = args[2];

var page = require('webpage').create();
page.open(input, function(status) {
    if (status !== 'success') {
        console.log('Unable to load the address!');
        phantom.exit();
         } 
    else {
        window.setTimeout(function () {
            page.render(output);
            console.log(input + '-->' + output);
            phantom.exit();
            }, 1000); // Change timeout as required to allow sufficient time 
        }
});
/*
var page = require('webpage').create();
page.open('rendered.html', function() {
          page.render('image.png');
            phantom.exit();
            });
*/



