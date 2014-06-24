/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 26/08/13
 * Time: 13.55
 * To change this template use File | Settings | File Templates.
 */


var spinner = angular.module('login.services', []);

spinner.factory('Login', function (Monitor, $rootScope, $location, $http, storage) {

    var login = {
        logged: false,
        usr: "",
        login: function (username, password, ok, err) {
            var self = this;
            Monitor.connect(username, password, function (data) {
                self.logged = true;
                self.usr = username;
                $rootScope.loggedIn = true;
                storage.set('login', true);
                storage.set('username', username);
                ok(data);
                $location.path("/dashboard");
            }, function (data) {
                err(data);
            });
        },
        isLogged: function () {
            return this.logged;
        },
        current: function () {
            var data = Monitor.get();


            var self = this;
            data.$then(function () {
                self.logged = true;
                self.username = data.currentUser;
            });
        },
        username: function () {
            if (this.usr == "") {
                this.usr = storage.get("username");
            }
            return this.usr;
        },
        logout: function () {
            var self = this;
            Monitor.disconnect(function (data) {
                self.logged = false;
                self.username = "";
                $rootScope.loggedIn = false;
                storage.set('login', false);
                storage.clearAll();
                delete $http.defaults.headers.common['Authorization'];
                $location.path("/login");
            }, function (data) {

            });
        }

    };
    return login;
});