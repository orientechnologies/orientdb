var schemaModule = angular.module('users.controller', ['database.services']);
schemaModule.controller("SecurityController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'FunctionApi', 'DocumentApi', '$modal', '$q', '$route', function ($scope, $routeParams, $location, Database, CommandApi, FunctionApi, DocumentApi, $modal, $q, $route) {

    $scope.db = $routeParams.database;
    $scope.active = $routeParams.tab || "users";
    $scope.tabs = ['users', 'roles'];
    $scope.tabsI18n = new Array;
    $scope.tabsI18n['users'] = 'Users';
    $scope.tabsI18n['roles'] = 'Roles';

    $scope.getTemplate = function (tab) {
//        console.log(tab);
        return 'views/database/security/' + tab + '.html';
    }
}]);
schemaModule.controller("UsersController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route) {

    $scope.database = Database;
    $scope.usersResult = new Array;

    var selectAllUsers = 'select * from oUser fetchPlan *:1 order by name  ';

    $scope.getListUsers = function () {
        $scope.functions = new Array;
        CommandApi.queryText({database: $routeParams.database, language: 'sql', verbose: false, text: selectAllUsers, limit: $scope.limit, shallow: true}, function (data) {
            if (data.result) {
                for (i in data.result) {
                    $scope.usersResult.push(data.result[i]);
                }
            }
        });
    }


    $scope.getListUsers();

}]);

schemaModule.controller("RolesController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'CommandApi', 'Database', function ($scope, $routeParams, $location, DatabaseApi, CommandApi, Database) {

    var selectAllUsers = 'select * from oRole fetchPlan *:1 order by name  ';
    $scope.usersResult = new Array;
    $scope.selectedRole = null;
    $scope.roleMode = [ 'DENY_ALL_BUT', 'ALLOW_ALL_BUT'];

    $scope.getListUsers = function () {
        $scope.functions = new Array;
        CommandApi.queryText({database: $routeParams.database, language: 'sql', verbose: false, text: selectAllUsers, limit: $scope.limit, shallow: true}, function (data) {
            if (data.result) {
                for (i in data.result) {
                    $scope.usersResult.push(data.result[i]);
                }
            }
        });
    }

    $scope.getListUsers();
    $scope.selectRole = function (selectedRole) {

        $scope.selectedRole = selectedRole;
        $scope.rules = Object.keys(selectedRole['rules']).sort();

    }

    $scope.calcolaBitmask = function (item) {

        var DecToBin = '';
        var Num1 = item

        Num1 = item % 2;

        while (item != 0) {

            DecToBin = Num1.toString().concat(DecToBin);
            item = Math.floor(item / 2);
            Num1 = item % 2;
        }
        var synch = 4 - DecToBin.length;
        var i = 0;
        for (i = 0; i < synch; i++) {
            DecToBin = '0'.concat(DecToBin);
        }
        var matrix = new Array;
        for (z in DecToBin) {
            if (z != 'contains')
                matrix.push(DecToBin[z] == '1')
        }
        return matrix;

    }
}]);