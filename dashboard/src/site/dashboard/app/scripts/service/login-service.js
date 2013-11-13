/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 26/08/13
 * Time: 13.55
 * To change this template use File | Settings | File Templates.
 */


var spinner = angular.module('login.services', []);

spinner.factory('Login', function (Monitor, $location) {

    var login = {
        logged: true,
        username: "",
        login: function (username, password) {
            var self = this;
            Monitor.connect(username, password, function (data) {
                self.logged = true;
                self.username = username;
                $location.path("/dashboard");
            }, function (data) {

            });
        },
        current: function () {
            var data = Monitor.get();


            var self = this;
            data.$then(function(){
                self.username = data.currentUser;
            });
        },
        logout: function () {
            var self = this;
            Monitor.disconnect(function (data) {
                self.logged = false;
                self.username = "";
                $location.path("/login");
            }, function (data) {

            });
        }

    };
    return login;
});