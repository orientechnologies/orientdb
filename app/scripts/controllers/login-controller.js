var login = angular.module('login.controller', ['database.services']);
login.controller("LoginController", ['$scope', '$routeParams', '$location', '$modal', '$q', 'Database', 'DatabaseApi', 'Notification', '$rootScope', function ($scope, $routeParams, $location, $modal, $q, Database, DatabaseApi, Notification, $rootScope) {

    $scope.server = "http://localhost:2480"

    if (Database.isConnected()) {
        //$location.path("/database/" + Database.getName() + "/browse");
    }
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
    $rootScope.$on("db:created", function (event, db) {
        $scope.databases.push(db);
    })
    $scope.createNew = function () {

        $modal({template: 'views/database/newDatabase.html', show: true});
    }
}]);

login.controller("NewDatabaseController", function ($scope, DatabaseApi, $rootScope) {


    $scope.stype = "plocal";
    $scope.type = "graph";
    $scope.username = "root";
    $scope.types = ['document', 'graph']
    $scope.stypes = ['local', 'plocal', 'memory']
    $scope.createNew = function () {
        DatabaseApi.createDatabase($scope.name, $scope.type, $scope.stype, $scope.username, $scope.password, function (data) {
            $rootScope.$emit("db:created", $scope.name);
            $scope.$hide();
        });
    }
});