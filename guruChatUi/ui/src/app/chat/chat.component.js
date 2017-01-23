;(function (angular, global) {
  "use strict";

  var alert = window.swal,// sweetalert.js
          _ = window._; // underscore.js

  angular.module("chat")
         .component("chat", {
           "templateUrl": global.templateDir + "/chat.html",
           "controller" : ["$scope", "$http", "$sce",
           function ($scope, $http, $sce) {

             var that = this;

             this.messages = [];

             this.awaitingResponse = false;

             var prevChatText = this.chatText = "";

             function handleChatError () {

               alert("Oops!",
                     "Something went wrong!, Please try again",
                     "error");

               that.messages.splice(that.messages.length - 1, 1);

               that.chatText = prevChatText;
             }

             function updateMessages (messages) {

               _.each(messages, function (message) {
               
                 if (message.type === "html") {

                   message.data = $sce.trustAsHtml(message.data);
                 }

                 that.messages.push(message);
               });
             }

             function submit () {

               if (that.chatText.length === 0) {

                 return;
               }

               var userMessage = {
                 "data"   : that.chatText,
                 "type"   : "text"
               };

               updateMessages([userMessage]);

               $http.post("/api/messages/", {"input": that.chatText})
                    .then(function (resp) {

                      resp = resp.data;

                      if (resp.error) {

                        return handleChatError();
                      }

                      userMessage.isNew = false;

                      var messages = resp.result.messages;

                      updateMessages(messages);
                    }, handleChatError).then(function () {

                      that.awaitingResponse = true;
                    });
             }

             this.onKeydown = function (event) {

               if (event.which === 13 || event.keycode === 13) {

                 event.preventDefault();

                 submit();
                 prevChatText = this.chatText;
                 this.chatText = "";
               }
             };

             this.onSend = submit;

             function handleMessagesError () {
             
               alert("Oops!", "Something went wrong", "error");
             }

             $http.get("/api/messages/").then(function (resp) {
             
               resp = resp.data;

               if (resp.error) {
               
                 return handleMessagesError();
               }

               updateMessages(resp.result.messages);

               if (that.messages.length === 0) {

                 updateMessages([
                 
                   {"data" : "Hi!",
                    "type" : "text",
                    "dummy": true},

                   {"data" : "How may i help you today?",
                    "type" : "text",
                    "dummy": true}
                 ]);
               }
             }, handleMessagesError).then(function () {
             
               that.hideLoading();
             });
           }],
           "bindings": {

             "showLoading": "&",
             "hideLoading": "&"
           }
         });

}(window.angular, window.GURU));
