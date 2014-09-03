var schemaModule = angular.module('users.controller', ['database.services']);
schemaModule.controller("SecurityController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'FunctionApi', 'DocumentApi', '$modal', '$q', '$route', function ($scope, $routeParams, $location, Database, CommandApi, FunctionApi, DocumentApi, $modal, $q, $route) {

    $scope.db = $routeParams.database;
    $scope.active = $routeParams.tab || "users";
    $scope.tabs = ['users', 'roles'];
    $scope.tabsI18n = new Array;
    $scope.tabsI18n['users'] = 'Users';
    $scope.tabsI18n['roles'] = 'Roles';

    $scope.getTemplate = function (tab) {
        return 'views/database/security/' + tab + '.html';
    }
}]);
schemaModule.controller("UsersController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', '$filter', 'ngTableParams', 'DocumentApi', '$modal', '$q', 'Notification', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route, $filter, ngTableParams, DocumentApi, $modal, $q, Notification) {

    $scope.database = Database;
    $scope.usersResult = new Array;

    var selectAllUsers = 'select * from oUser fetchPlan *:1 order by name  ';
    var selectAllRoles = 'select * from oRole fetchPlan *:1 order by name  ';

    $scope.getListUsers = function () {
        $scope.functions = new Array;
        CommandApi.queryText({database: $routeParams.database, language: 'sql', verbose: false, text: selectAllUsers, limit: $scope.limit, shallow: false}, function (data) {
            $scope.usersResult = data.result;

            $scope.tableParams = new ngTableParams({
                page: 1,            // show first page
                count: 10          // count per page

            }, {
                total: $scope.usersResult.length, // length of data
                getData: function ($defer, params) {
                    var orderedData = params.sorting() ?
                        $filter('orderBy')($scope.usersResult, params.orderBy()) :
                        $scope.usersResult;
                    $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
                }
            });
        });
    }
    $scope.getListRoles = function () {
        $scope.functions = new Array;
        CommandApi.queryText({database: $routeParams.database, language: 'sql', verbose: false, text: selectAllRoles, limit: $scope.limit, shallow: false}, function (data) {
            $scope.roles = data.result;
            $scope.dataRoles = [];
            $scope.roles.forEach(function (e, idx, arr) {
                $scope.dataRoles.push({ id: idx, text: e});
            })
            $scope.select2Options = {
                'multiple': true,
                'tags': $scope.roles,
                formatResult: function (item) {
                    if (item.name) {
                        return item.name
                    } else {
                        return item.text.name;
                    }
                },
                formatSelection: function (item) {
                    if (item.name) {
                        return item.name
                    } else {
                        return item.text.name;
                    }
                }
            };
        });
    }

    $scope.addUser = function () {
        var modalScope = $scope.$new(true);
        modalScope.user = DocumentApi.createNewDoc("OUser");
        modalScope.select2Options = $scope.select2Options;
        var modalPromise = $modal({template: 'views/database/users/newUser.html', scope: modalScope, show: false});
        modalScope.save = function () {
            if (modalPromise.$scope.roles) {
                modalPromise.$scope.user.roles = [];
                modalPromise.$scope.roles.forEach(function (e) {
                    modalPromise.$scope.user.roles.push(e.text["@rid"])
                });
            }
            DocumentApi.createDocument($scope.database.getName(), modalPromise.$scope.user["@rid"], modalPromise.$scope.user).then(function (data) {
                $scope.usersResult.push(data);
                $scope.tableParams.reload();
                Notification.push({content: 'User ' + data.name + ' has been created.'});
            }, function error(err) {
                Notification.push({content: err, error: true});
            })
        }
        modalPromise.$promise.then(modalPromise.show);
    }
    $scope.changeRoles = function (result) {

    }
    $scope.edit = function (user) {
        var modalScope = $scope.$new(true);
        modalScope.user = user;
        modalScope.select2Options = $scope.select2Options;
        var modalPromise = $modal({template: 'views/database/users/newUser.html', scope: modalScope, show: false});
        modalScope.save = function () {
            if (modalPromise.$scope.roles) {
                modalPromise.$scope.user.roles = [];
                modalPromise.$scope.roles.forEach(function (e) {
                    modalPromise.$scope.user.roles.push(e.text["@rid"])
                });
            }
            DocumentApi.updateDocument($scope.database.getName(), modalPromise.$scope.user["@rid"], modalPromise.$scope.user).then(function () {

            }, function error(err) {
                Notification.push({content: err, error: true});
            })
        }
        modalPromise.$promise.then(modalPromise.show);
    }
    $scope.delete = function (user) {
        Utilities.confirm($scope, $modal, $q, {
            title: 'Warning!',
            body: 'You are deleting user ' + user.name + '. Are you sure?',
            success: function () {
                DocumentApi.deleteDocument($scope.database.getName(), user['@rid'], function (data) {
                    Notification.push({content: 'User ' + user.name + ' has been deleted.'});
                    var idx = $scope.usersResult.indexOf(user);
                    if (idx > -1) {
                        $scope.usersResult.splice(idx, 1);
                        $scope.tableParams.reload();
                    }
                });
            }
        });
    }
    $scope.getListRoles();
    $scope.getListUsers();

}]);

schemaModule.controller("RolesController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'CommandApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DatabaseApi, CommandApi, Database, Notification) {

    var selectAllUsers = 'select * from oRole fetchPlan *:1 order by name  ';
    $scope.usersResult = new Array;
    $scope.selectedRole = null;
    $scope.roleMode = [ 'DENY_ALL_BUT', 'ALLOW_ALL_BUT'];

    $scope.getListUsers = function () {
        $scope.functions = new Array;
        CommandApi.queryText({database: $routeParams.database, language: 'sql', verbose: false, text: selectAllUsers, limit: $scope.limit, shallow: false}, function (data) {
            if (data.result) {
                $scope.usersResult = data.result;
                $scope.selectRole(data.result[0])
            }
        });
    }

    $scope.getListUsers();
    $scope.selectRole = function (selectedRole) {
        $scope.selectedRole = selectedRole;
        $scope.rules = Object.keys(selectedRole['rules']).sort();
    }

    $scope.changeRules = function (resource, role, idx, old) {


        var params = { ops: old ? "GRANT" : "REVOKE", permission: "update", resource: resource, role: role}
        var sql = "{{ops}} {{permission}} ON {{resource}} TO {{role}}"
        var query = S(sql).template(params).s
        CommandApi.queryText({database: $routeParams.database, language: 'sql', verbose: false, text: query, limit: $scope.limit, shallow: false}, function (data) {


            switch (params.ops) {
                case "GRANT":
                    Notification.push({content: S("Permission of '{{permission}}' granted on resource '{{resource}}' to '{{role}}'").template(params).s});
                    break;
                case "REVOKE":
                    break;
            }
        });
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