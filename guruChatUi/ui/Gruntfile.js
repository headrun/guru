"use strict";

const fs     = require("fs"),
      uglify = require("uglify-js"),
      grunt  = require("grunt"),
      _      = require("underscore"),
      htmlMinify = require('html-minifier').minify,
      jsdom = require("jsdom").jsdom,
      serializeDocument = require("jsdom").serializeDocument,
      babel = require("babel-core"),
      babelOpts = { "presets": ["es2015"],
                    "compact": false};


var pkg = JSON.parse(fs.readFileSync("package.json")),
    isProd = pkg.environment === "production";

var uglifyJSOps = {

  "banner": "/*! <%= pkg.name %> - v<%= pkg.version %> - " +
            "<%= grunt.template.today(\"yyyy-mm-dd\") %> */"
};

if (isProd) {

  _.extend(uglifyJSOps, {

    "stripBanners": true,
    "separator": ";",
    "process": function (src) {

      return uglify.minify(src, {

        "fromString": true,
        "compress": {

          "dead_code"    : true,
          "drop_console" : true,
          "drop_debugger": true,
          "unused"       : true,
          "join_vars"    : true
        },
        "mangle": {

          "toplevel": true
        }
      }).code;
    }
  });
} else {

  _.extend(uglifyJSOps, {

    "separator": "\n/*===================================*/\n",
    "process": function (src, filePath) {

      return "/* FILE: " + filePath + " */\n\n" + src;
    }
  });
}

function getLessCssOpts () {

  return {

    "plugins": [

      new (require('less-plugin-autoprefix'))({}),
      new (require('less-plugin-clean-css'))({})
    ],
    "compress": true
  };
}

function getHtmlOptions (staticUrl, templateParams, doNotParse) {

  var version = pkg.version;

  templateParams = templateParams || {};

  return {

    "process": function (template) {

      template = _.template(template)(_.extend({}, templateParams, {

        "title": null,
        "static": function (url, noVersioning) {

          return staticUrl + "/" + url +
                 (!noVersioning ? "?v=" + version : "");
        }
      }));

      if (!doNotParse) {

	/* parse script tags with type template and put actual
	 * content in 'em */
	var doc = jsdom(template, {}),
	    window = doc.defaultView,
	    document = window.document,
	    scripts = document.querySelectorAll('[type="template"]');

	_.each(scripts, function (el) {

	  var src = el.src;

	  src = fs.readFileSync(src);

	  el.innerHTML = src;
	  el.removeAttribute("src");
	});

        template = serializeDocument(doc);
      }

      return htmlMinify(template,
                        {
                          "processScripts": ["template"],
                          "removeComments": true,
                          "collapseWhitespace": true,
                          "sortAttributes": true,
                          "sortClassName": true
                        });
    }
  };
}

var config = {

  "pkg"        : pkg,
  "dist"       : "dist",
  "src"        : "src",
  "libsPath"   : "<%= src %>/libs",
  "vendorPath" : "<%= src %>/vendor",
  "appPath": "<%= src %>/app",
  "concat": {

    "appJs": {

      "options": uglifyJSOps,
      "src": ["<%= libsPath %>/underscore/underscore-min.js",
              "<%= libsPath %>/sweetalert/dist/sweetalert.min.js",
              "<%= libsPath %>/highcharts/highcharts.js",
              "<%= libsPath %>/angular/angular.min.js",
              "<%= libsPath %>/angular-ui-router/release/angular-ui-router.min.js",
              "<%= libsPath %>/angular-bind-html-compile/angular-bind-html-compile.min.js",
              "<%= appPath %>/core/auth/auth.js",
              "<%= appPath %>/core/auth/session.js",
              "<%= appPath %>/core/auth/authEvents.js",
              "<%= appPath %>/core/csrf/csrf.js",
              "<%= appPath %>/login/login.module.js",
              "<%= appPath %>/login/login.component.js",
              "<%= appPath %>/logout/logout.module.js",
              "<%= appPath %>/logout/logout.component.js",
              "<%= appPath %>/header/header.module.js",
              "<%= appPath %>/header/header.component.js",
              "<%= appPath %>/highcharts/highcharts.module.js",
              "<%= appPath %>/highcharts/highcharts.component.js",
              "<%= appPath %>/chat/chat.module.js",
              "<%= appPath %>/chat/messages.directive.js",
              "<%= appPath %>/chat/chat.component.js",
              "<%= appPath %>/dashboard/dashboard.module.js",
              "<%= appPath %>/dashboard/dashboard.component.js",
              "<%= appPath %>/app.js",
              "<%= appPath %>/app.config.js"],
      "dest": "<%= dist %>/js/app.js"
    },
    "appHtml": {

      "options": getHtmlOptions("",
                                {templateDir: "/views"}),
      "src": ["<%= src %>/index.tpl"],
      "dest": "<%= dist %>/index.html"
    },
    "appHtmlTmpl": {

      "options": getHtmlOptions("", null, true),
      "files": [{
        "expand": true,
        "flatten": true,
        "src"   : "<%= src %>/app/**/*.html",
        "dest"  : "<%= dist %>/views/"
      }]
    }
  },
  "less": {

    "app": {

      "options": getLessCssOpts(),
      "src": ["<%= src %>/less/app.less"],
      "dest": "<%= dist %>/css/app.css"
    }
  },
  "copy": {

    "assets": {

      "files": [
        {
          expand: true,
          cwd: "<%= src %>/",
          src: ["js/**"],
          dest: "<%= dist %>/"
        },
        {
          expand: true,
          cwd: "<%= src %>/",
          src: ["fonts/**"],
          dest: "<%= dist %>/"
        },
        {
          expand: true,
          cwd: "<%= src %>/",
          src: ["css/**"],
          dest: "<%= dist %>/"
        },
        {
          expand: true,
          cwd: "<%= src %>/",
          src: ["libs/**", "vendor/**", "images/**", "videos/**"],
          dest: "<%= dist %>/"
        },
        {
          expand: true,
          cwd: "<%= appPath %>/",
          src: ["fonts/**"],
          dest: "<%= dist %>/app/"
        }
      ]
    },
  }
};

grunt.initConfig(config);

grunt.loadNpmTasks("grunt-contrib-concat");
grunt.loadNpmTasks("grunt-contrib-less");
grunt.loadNpmTasks("grunt-contrib-copy");

grunt.registerTask("app", ["concat:appHtml", "concat:appHtmlTmpl", "less:app", "concat:appJs"]);
grunt.registerTask("default", ["app"]);
grunt.registerTask("init", ["copy:assets", "default"]);
