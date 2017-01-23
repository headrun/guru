;(function (angular) {
  "use strict";

  // If User is not logged in, he will be redirected to LOGIN_STATE
  var LOGIN_STATE = "login",

      // Once the user is logged in using the form he will be redirected
      // to LOGIN_REDIRECT_STATE
      LOGIN_REDIRECT_STATE = "dashboard";

  angular.module("bootstrap")
         .config(["$locationProvider", "$stateProvider",
                  "$httpProvider", "$urlRouterProvider",

           function ($lp, $sp, $hp, $urp) {

             $lp.hashPrefix('!');

             /* ALL YOUR CONFIG GOES HERE */

             $sp.state("login", {

               "url"     : "/login",
               "template": "<login></login>",

               /*
                ensure csrf if you are using direct form POST sumbmissions,
                instead of ajax calls, else DO NOT put this
               */
               "ensureCSRF": true
             }).state("dashboard", {

               "url"     : "/",
               "template": "<dashboard></dashboard>",

               /*
                * If authRequired is true,
                * login check will be done before loadin the page
                * if not authenticated, he will be redirected to LOGIN_PAGE
                *
                * ensureCSRF is not needed when authRequired is given
               */
               "authRequired": true
             });

             $urp.otherwise("/");

             $hp.defaults.xsrfCookieName = "csrftoken";
             $hp.defaults.xsrfHeaderName = "X-CSRFToken";
           }
         ]).run(["$rootScope", "$state",
                 "Auth", "AUTH_EVENTS", "Csrf",

          function ($rootScope, $state, Auth, AUTH_EVENTS, Csrf) {

            // DO NOT EDIT THIS FUNCTION

            var skipAsync = false;

            $rootScope.$on("$stateChangeStart", function (event, next, params) {

              if (skipAsync) {

                skipAsync = false;
                return;
              }

              if (next.authRequired || next.ensureCSRF) {

                event.preventDefault();

                ;(function (thisNext, params) {

                  function continueToNext () {

                    skipAsync = true;
                    $state.go(thisNext.name, params);
                  }

                  if (next.authRequired) {

                    Auth.status().then(function (resp) {

                      if (thisNext.name !== next.name) {

                        return;
                      }

                      if (!resp.user) {

                        $rootScope.$broadcast(AUTH_EVENTS.unAuthorized);
                        return;
                      }

                      continueToNext();

                    });
                  } else {

                    Csrf.get().then(continueToNext);
                  }
                }(next, params));
              }
            });

            $rootScope.$on(AUTH_EVENTS.loginSuccess, function () {

              $state.go(LOGIN_REDIRECT_STATE, {"location": "replace"});
            });

            function goToLogin () {

              $state.go(LOGIN_STATE, {"location": "replace"});
            }

            $rootScope.$on(AUTH_EVENTS.unAuthorized, goToLogin);
            $rootScope.$on(AUTH_EVENTS.logoutSuccess, goToLogin);
          }
         ]);
}(window.angular));
