<!DOCTYPE html>
<html lang="en">

  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">

    <title><% if (title){ %><%= title %><% } else { %>Guru<% } %></title>

    <!-- Vendor styles -->
    <link rel="stylesheet" href="<%= static("libs/font-awesome/css/font-awesome.min.css", true) %>" />
    <link rel="stylesheet" href="<%= static("libs/material-design-iconic-font/dist/css/material-design-iconic-font.min.css", true) %>" />
    <link rel="stylesheet" href="<%= static("libs/sweetalert/dist/sweetalert.css", true) %>" />
    <link rel="stylesheet" href="<%= static("libs/bootstrap/dist/css/bootstrap.min.css", true) %>">

    <!-- application css -->
    <link rel="stylesheet" href="<%= static("css/app.css") %>"/>
  </head>

  <body>
    <div ng-app="bootstrap" class="app">
      <div ui-view autoscroll="false" class="content"></div>

      <!-- Loading overlay, which block screen unless everything is rendered -->
      <div ng-if="loading" class="loading-overlay">
        <div><span>Loading...</span></div>
      </div>
    </div>

    <script>;(function () { window.GURU = {"templateDir": "<%= templateDir %>"}}());</script>
    <script src="<%= static("js/app.js") %>"></script>
  </body>
</html>
