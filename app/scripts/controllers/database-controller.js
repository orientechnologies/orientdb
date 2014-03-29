var dbModule = angular.module('database.controller', ['database.services']);
dbModule.controller("BrowseController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'localStorageService', 'Spinner', '$modal', '$q', '$window', function ($scope, $routeParams, $location, Database, CommandApi, localStorageService, Spinner, $modal, $q, $window) {

    $scope.database = Database;
    $scope.limit = 20;

    $scope.countPage = 5;
    $scope.keepLimit = localStorageService.get("keepLimit");
    if (!$scope.keepLimit) {
        $scope.keepLimit = 10;
        localStorageService.add("keepLimit", $scope.keepLimit);
    }
    $scope.countPageOptions = [5, 10, 20, 50, 100, 500, 1000, 2000, 5000];
    var dbTime = localStorageService.get("Timeline");
    if (!dbTime) {
        dbTime = new Object;
        localStorageService.add("Timeline", dbTime);
    }

    $scope.timeline = dbTime[Database.getName()];
    if (!$scope.timeline) {
        $scope.timeline = new Array;
        var localTime = localStorageService.get("Timeline");
        localTime[Database.getName()] = $scope.timeline;
        localStorageService.add("Timeline", localTime);


    }
    Database.setWiki("https://github.com/orientechnologies/orientdb-studio/wiki/Query");

    $scope.language = 'sql';

    $scope.table = true;
    $scope.contentType = ['JSON', 'CSV'];
    $scope.shallow = true;
    $scope.selectedContentType = $scope.contentType[0];
    $scope.editorOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: false,
//        theme: 'ambiance',
        mode: 'text/x-sql',
        metadata: Database,
        extraKeys: {
            "Ctrl-Enter": function (instance) {
                $scope.$apply(function () {
                    if ($scope.queryText)
                        $scope.query();
                });

            },
            "Ctrl-Space": "autocomplete"

        },
        onLoad: function (_cm) {
            $scope.cm = _cm;
            if ($routeParams.query) {
                $scope.queryText = $routeParams.query;
                $scope.cm.setValue($scope.queryText);

                $scope.query();
            }
            $scope.cm.on("change", function () { /* script */
                var wrap = $scope.cm.getWrapperElement();
                var approp = $scope.cm.getScrollInfo().height > 300 ? "300px" : "auto";
                if (wrap.style.height != approp) {
                    wrap.style.height = approp;
                    $scope.cm.refresh();
                }
            });

        }
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

        CommandApi.queryText({database: $routeParams.database, contentType: conttype, language: $scope.language, text: $scope.queryText, limit: $scope.limit, shallow: $scope.shallow, verbose: false}, function (data) {

            if (data.result) {

                var item = new Object;
                item.query = $scope.queryText;
                item.language = $scope.language;
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
                item.headers = $scope.headers;
                item.rawData = $scope.rawData;
                item.resultTotal = $scope.resultTotal;
                item.results = $scope.results;
                item.currentPage = $scope.currentPage;
                item.numberOfPage = $scope.numberOfPage;
                item.countPage = 5;
                item.countPageOptions = [5, 10, 20, 50, 100, 500, 1000, 2000, 5000];
                item.notification = data.notification;
                $scope.timeline.unshift(item);
                if ($scope.timeline.length > $scope.keepLimit) {
                    $scope.timeline.splice($scope.timeline.length - 1, 1);
                }
                var dbTime = localStorageService.get("Timeline");
                dbTime[Database.getName()] = $scope.timeline;
                localStorageService.add("Timeline", dbTime);

                Spinner.stopSpinner();
            }
            Spinner.stopSpinner();
        }, function (data) {
            Spinner.stopSpinner();
            $scope.headers = undefined;
            $scope.resultTotal = undefined;
            $scope.results = undefined;
        });

    }

    $scope.clear = function () {

        Utilities.confirm($scope, $modal, $q, {
            title: 'Warning!',
            body: 'You are clearing history. Are you sure?',
            success: function () {
                $scope.timeline = new Array;
                var dbTime = localStorageService.get("Timeline");
                dbTime[Database.getName()] = $scope.timeline;
                localStorageService.add("Timeline", dbTime);
            }
        });

    }

    $scope.removeItem = function (item) {
        var idx = $scope.timeline.indexOf(item);
        $scope.timeline.splice(idx, 1);
        localStorageService.add("Timeline", $scope.timeline);
    }

}]);
dbModule.controller("QueryController", ['$scope', '$routeParams', '$filter', '$location', 'Database', 'CommandApi', 'localStorageService', 'Spinner', 'ngTableParams', 'scroller', '$ojson', function ($scope, $routeParams, $filter, $location, Database, CommandApi, localStorageService, Spinner, ngTableParams, scroller, $ojson) {


    var data = $scope.item.resultTotal;

    $scope.viewerOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: true,
        mode: 'javascript',
        onLoad: function (_cm) {
            $scope.vcm = _cm;
            $scope.vcm.setValue($ojson.format($scope.item.rawData));
        }

    };
    $scope.tableParams = new ngTableParams({
        page: 1,            // show first page
        count: 10          // count per page

    }, {
        total: data.length, // length of data
        getData: function ($defer, params) {
            // use build-in angular filter
            console.log($scope);
            var orderedData = params.sorting() ?
                $filter('orderBy')(data, params.orderBy()) :
                data;
            $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
        }
    });

    $scope.switchPage = function (index) {
        if (index != $scope.item.currentPage) {
            $scope.item.currentPage = index;
            $scope.item.results = $scope.item.resultTotal.slice(
                (index - 1) * $scope.item.countPage,
                index * $scope.item.countPage
            );
        }
    }
    $scope.previous = function () {
        if ($scope.item.currentPage > 1) {
            $scope.switchPage($scope.item.currentPage - 1);
        }
    }
    $scope.next = function () {

        if ($scope.item.currentPage < $scope.item.numberOfPage.length) {
            $scope.switchPage($scope.item.currentPage + 1);
        }
    }
    $scope.$watch("item.countPage", function (data) {
        if ($scope.item.resultTotal) {
            $scope.item.results = $scope.item.resultTotal.slice(0, $scope.item.countPage);
            $scope.item.currentPage = 1;
            $scope.item.numberOfPage = new Array(Math.ceil($scope.item.resultTotal.length / $scope.item.countPage));
        }
    });
    $scope.openRecord = function (doc) {
        $location.path("/database/" + $scope.database.getName() + "/browse/edit/" + doc["@rid"].replace('#', ''));
    }
    $scope.changeQuery = function () {
        $scope.queryText = $scope.item.query;
        scroller.scrollTo(0, 0, 2000);
        $scope.cm.focus();

        $scope.cm.setValue($scope.queryText);
        $scope.cm.setCursor($scope.cm.lineCount());

    }

}]);
dbModule.controller("QueryConfigController", ['$scope', '$routeParams', 'localStorageService', function ($scope, $routeParams, localStorageService) {


    $scope.$watch("limit", function (data) {
        $scope.$parent.limit = data;
    });
    $scope.$watch("selectedContentType", function (data) {
        $scope.$parent.selectedContentType = data;
    });
    $scope.$watch("shallow", function (data) {
        $scope.$parent.shallow = data;
    });
    $scope.$watch("keepLimit", function (data) {
        $scope.$parent.keepLimit = data;
        localStorageService.add("keepLimit", data);
    });
}]);



