var login = angular.module('login.controller', ['database.services']);
login.controller("LoginController", ['$scope', '$routeParams', '$location', '$modal', '$q', 'Database', 'DatabaseApi', 'Notification', function ($scope, $routeParams, $location, $modal, $q, Database, DatabaseApi, Notification) {

    $scope.server = "http://localhost:2480"

    DatabaseApi.listDatabases(function (data) {
        $scope.databases = data.databases;
        if ($scope.databases.length > 0) {
            $scope.database = $scope.databases[0];
        }
    });

    $scope.connect = function () {
        $scope.$broadcast("autofill:update");
        Database.connect($scope.database, $scope.username, $scope.password, function () {
            $location.path("/database/" + $scope.database + "/browse");
        }, function () {
            var noti = "Invalid username or password";
            Notification.push({content: noti});
        });
    }
    $scope.createNew = function () {
        modalScope = $scope.$new(true);
        modalScope.name = null;
        modalScope.creating = false;
        modalScope.stype = "plocal";
        modalScope.type = "graph";
        modalScope.username = "root";
        modalScope.types = ['document', 'graph']
        modalScope.stypes = ['local', 'plocal', 'memory']
        var modalPromise = $modal({template: 'views/database/newDatabase.html', scope: modalScope, show: false});
        modalScope.createNew = function () {
            modalScope.creating = true;
            DatabaseApi.createDatabase(modalPromise.$scope.name, modalPromise.$scope.type, modalPromise.$scope.stype, modalPromise.$scope.username, modalPromise.$scope.password, function (data) {
                $scope.databases.push(modalPromise.$scope.name);
                modalScope.creating = false;
                modalPromise.hide();
                var noti = "Database " + modalPromise.$scope.name + " created.";
                Notification.push({content: noti});
            }, function (data) {
                modalScope.creating = false;
                modalScope.error = data;
            });
        }

        modalPromise.$promise.then(modalPromise.show);
    }
    $scope.deleteDb = function () {
        modalScope = $scope.$new(true);
        modalScope.name = $scope.database;
        var modalPromise = $modal({template: 'views/database/deleteDatabase.html', scope: modalScope, show: false});
        modalScope.delete = function () {
            modalScope.creating = true;
            DatabaseApi.deleteDatabase(modalPromise.$scope.name, modalPromise.$scope.username, modalPromise.$scope.password).then(function (data) {
                var noti = "Database " + modalPromise.$scope.name + " dropped.";
                var idx = $scope.databases.indexOf(modalPromise.$scope.name);
                $scope.databases.splice(idx, 1);
                if ($scope.databases.length > 0) {
                    $scope.database = $scope.databases[0];
                }
                Notification.push({content: noti});
                modalPromise.hide();
            }, function (data) {
                modalScope.creating = false;
                modalScope.error = data;
            })
        }

        modalPromise.$promise.then(modalPromise.show);
    }
}]);
