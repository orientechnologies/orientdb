var dbModule = angular.module('database.controller', ['database.services']);
dbModule.controller("BrowseController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'localStorageService', 'Spinner', function ($scope, $routeParams, $location, Database, CommandApi, localStorageService, Spinner) {

    $scope.database = Database;
    $scope.limit = 20;

    Database.setWiki("https://github.com/orientechnologies/orientdb-studio/wiki/Query");
    if (localStorageService.get("Queries") == null) {
        localStorageService.add("Queries", new Array);
    }
    $scope.queries = localStorageService.get("Queries");
    $scope.language = 'sql';
    $scope.countPage = 10;
    $scope.countPageOptions = [10, 20, 50, 100, 500, 1000, 2000, 5000];
    $scope.table = true;
    $scope.contentType = ['JSON', 'CSV'];
    $scope.selectedContentType = $scope.contentType[0];
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
        Spinner.start();

        $scope.queryText = $scope.queryText.trim();
        $scope.queryText = $scope.queryText.replace(/\n/g, " ");
        if ($scope.queryText.startsWith('g.')) {
            $scope.language = 'gremlin';
        }
        if ($scope.queryText.startsWith('#')) {
            $location.path('/database/' + $routeParams.database + '/browse/edit/' + $scope.queryText.replace('#', ''));
        }

        var conttype;
        if ($scope.selectedContentType == 'CSV')
            conttype = 'text/csv';
        CommandApi.queryText({database: $routeParams.database, contentType: conttype, language: $scope.language, text: $scope.queryText, limit: $scope.limit}, function (data) {

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
                if ($scope.queries.indexOf($scope.queryText) != -1) {
                    var idx = $scope.queries.indexOf($scope.queryText);
                    $scope.queries.splice(idx, 1);

                }
                $scope.queries.unshift($scope.queryText);
                localStorageService.add("Queries", $scope.queries);
                Spinner.stopSpinner();
            }
        }, function (data) {
            Spinner.stopSpinner();
            $scope.headers = undefined;
            $scope.resultTotal = undefined;
            $scope.results = undefined;
        });

    }

    $scope.clear = function () {
        $scope.queries = new Array;
        localStorageService.add("Queries", $scope.queries);
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

