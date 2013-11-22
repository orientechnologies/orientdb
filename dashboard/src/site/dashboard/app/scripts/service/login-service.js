/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 26/08/13
 * Time: 13.55
 * To change this template use File | Settings | File Templates.
 */


var spinner = angular.module('login.services', []);

spinner.factory('Login', function (Monitor,$rootScope, $location,$http) {

    var login = {
        logged: false,
        username: "",
        login: function (username, password) {
            var self = this;
            Monitor.connect(username, password, function (data) {
                self.logged = true;
                self.username = username;
                $rootScope.loggedIn = true;
                $location.path("/dashboard");
            }, function (data) {

            });
        },
        isLogged: function(){
        	return this.logged;
        },
        current: function () {
            var data = Monitor.get();


            var self = this;
            data.$then(function(){
            	self.logged = true;
                self.username = data.currentUser;
            });
        },
        logout: function () {
            var self = this;
            Monitor.disconnect(function (data) {
                self.logged = false;
                self.username = "";
                $rootScope.loggedIn = false;
                $http.defaults.headers.common['Authorization'] = null;
                $location.path("/login");
            }, function (data) {

            });
        }

    };
    return login;
});