var dbModule = angular.module('database.controller', ['database.services']);
dbModule.controller("BrowseController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'Spinner', function ($scope, $routeParams, $location, Database, CommandApi, Spinner) {

    $scope.database = Database;
    $scope.limit = 20;
    $scope.queries = new Array;
    $scope.language = 'sql';
    $scope.countPage = 10;
    $scope.countPageOptions = [10, 20, 50, 100];
    $scope.table = true;
    $scope.editorOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: false,
        theme: 'ambiance',
        mode: 'text/x-sql',
        metadata: Database,
        extraKeys: {
            "Ctrl-Enter": function (instance) {
                $scope.$apply(function () {
                    $scope.query();
                });

            },
            "Ctrl-Space": "autocomplete"

        }
    };
    $scope.viewerOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: true,
        mode: 'javascript'

    };


    $scope.query = function () {
        Spinner.loading = true;
        $scope.queryText = $scope.queryText.trim();
        if ($scope.queryText.startsWith('g.')) {
            $scope.language = 'gremlin';
        }
        if ($scope.queryText.startsWith('#')) {
            $location.path('/database/' + $routeParams.database + '/browse/edit/' + $scope.queryText.replace('#', ''));
        }

        CommandApi.queryText({database: $routeParams.database, language: $scope.language, text: $scope.queryText, limit: $scope.limit}, function (data) {
            if (data.result) {
                $scope.headers = Database.getPropertyTableFromResults(data.result);
                if ($scope.headers.length == 00) {
                    $scope.alerts = new Array;
                    $scope.alerts.push({content: "No records found."});
                }
                $scope.rawData = JSON.stringify(data);
                $scope.resultTotal = data.result;
                $scope.results = data.result.slice(0, $scope.countPage);
                $scope.currentPage = 1;

                $scope.numberOfPage = new Array(Math.ceil(data.result.length / $scope.countPage));

            }
            if ($scope.queries.indexOf($scope.queryText) == -1)
                $scope.queries.push($scope.queryText);
            Spinner.loading = false;
        }, function (data) {
            Spinner.loading = false;
        });
    }
    $scope.switchPage = function (index) {
        if (index != $scope.currentPage) {
            $scope.currentPage = index;
            $scope.results = $scope.resultTotal.slice(
                (index - 1) * $scope.countPage,
                index * $scope.countPage
            );
        }
    }
    $scope.previous = function () {
        if ($scope.currentPage > 1) {
            $scope.switchPage($scope.currentPage - 1);
        }
    }
    $scope.next = function () {

        if ($scope.currentPage < $scope.numberOfPage.length) {
            $scope.switchPage($scope.currentPage + 1);
        }
    }
    $scope.$watch("countPage", function (data) {
        if ($scope.resultTotal) {
            $scope.results = $scope.resultTotal.slice(0, $scope.countPage);
            $scope.currentPage = 1;
            $scope.numberOfPage = new Array(Math.ceil($scope.resultTotal.length / $scope.countPage));
        }
    });
    $scope.openRecord = function (doc) {
        $location.path("/database/" + $scope.database.getName() + "/browse/edit/" + doc["@rid"].replace('#', ''));
    }

    if ($routeParams.query) {
        $scope.queryText = $routeParams.query;
        $scope.query();
    }
}]);

