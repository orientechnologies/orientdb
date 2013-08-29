var dbModule = angular.module('database.controller', ['database.services']);
dbModule.controller("BrowseController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'Spinner', function ($scope, $routeParams, $location, Database, CommandApi, Spinner) {

    $scope.database = Database;
    $scope.limit = 20;
    $scope.queries = new Array;
    $scope.editorOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: false,
        theme: 'ambiance',
        mode: 'text/x-sql',
        extraKeys: {
            "Ctrl-Enter": function (instance) {
                $scope.query()
            }
        }
    };
    $scope.query = function () {
        Spinner.loading = true;
        CommandApi.queryText({database: $routeParams.database, language: 'sql', text: $scope.queryText, limit: $scope.limit}, function (data) {
            if (data.result) {
                $scope.headers = Database.getPropertyTableFromResults(data.result);
                        $scope.results = data.result;
            }
            if ($scope.queries.indexOf($scope.queryText) == -1)
                $scope.queries.push($scope.queryText);
            Spinner.loading = false;
        });
    }
    $scope.openRecord = function (doc) {
        $location.path("/database/" + $scope.database.getName() + "/browse/edit/" + doc["@rid"].replace('#', ''));
    }

    if ($routeParams.query) {
        $scope.queryText = $routeParams.query;
        $scope.query();
    }
}]);

