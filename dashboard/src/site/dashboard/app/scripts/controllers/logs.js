var dbModule = angular.module('workbench-logs.controller', ['workbench-logs.services']);
dbModule.controller("LogsController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor) {
    $scope.countPage = 1000;
    $scope.countPageOptions = [100, 500, 1000];
    $scope.types = ['CONFIG', 'FINE', 'FINER', 'FINEST', 'INFO', 'SEVE', 'WARN'];
    $scope.files = ['ALL_FILES', 'LAST'];
    $scope.selectedType = undefined;
    $scope.selectedFile = undefined;
    $scope.server = undefined;
    Monitor.getServer($routeParams.server, function (data) {
        $scope.server = data;
        CommandLogApi.getListFiles({server: $scope.server.name }, function (data) {
            if (data) {
                for (entry in data['files']) {
                    $scope.files.push(data['files'][entry]['name']);
                }
            }
        });
    });


    $scope.results = undefined;
    $scope.selectedSearch = '';
    $scope.getListFiles = function () {
        CommandLogApi.getListFiles({server: $scope.server }, function (data) {

            if (data) {
                for (entry in data['files']) {
                    $scope.files.push(data['files'][entry]['name']);
                }
            }
        });
    }
    $scope.$watch("countPage", function (data) {
        if ($scope.resultTotal) {
            $scope.results = $scope.resultTotal.slice(0, $scope.countPage);
            $scope.currentPage = 1;
            $scope.numberOfPage = new Array(Math.ceil($scope.resultTotal.length / $scope.countPage));
        }
    });
    $scope.clear = function () {
        $scope.queries = new Array;
    }
    $scope.switchPage = function (index) {
        if (index != $scope.currentPage) {
            $scope.currentPage = index;
            $scope.results = $scope.resultTotal.logs.slice(
                (index - 1) * $scope.countPage,
                index * $scope.countPage
            );
        }
    }
    $scope.getListFiles();
    $scope.checkDateFrom = function () {
        if ($scope.selectedDateFrom == undefined || $scope.selectedDateFrom == '') {
            return true;
        }
        return false
    }
    $scope.checkHourFrom = function () {
        if ($scope.selectedHourFrom == undefined || $scope.selectedHourFrom == '') {
            return true;
        }
        return false
    }
    $scope.checkFile = function () {
        if ($scope.selectedFile == 'LAST') {
            return true;
        }
        return false;
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
    $scope.search = function () {

        var typeofS = undefined;
        var filess = undefined;
        if ($scope.selectedFile == undefined || $scope.selectedFile == '') {
            return;
        }
        if ($scope.selectedFile == 'ALL_FILES') {
            typeofS = 'search';
        }
        else if ($scope.selectedFile == 'LAST') {
            typeofS = 'tail';
        }
        else {
            typeofS = 'file';
            filess = $scope.selectedFile
        }
        CommandLogApi.getLastLogs({server: $scope.server.name, file: filess, typeofSearch: typeofS, searchvalue: $scope.selectedSearch, logtype: $scope.selectedType, dateFrom: $scope.selectedDateFrom, hourFrom: $scope.selectedHourFrom, dateTo: $scope.selectedDateTo, hourTo: $scope.selectedHourTo}, function (data) {
            if (data) {
                $scope.resultTotal = data;
                $scope.results = data.logs.slice(0, $scope.countPage);
                $scope.currentPage = 1;
                $scope.numberOfPage = new Array(Math.ceil(data.logs.length / $scope.countPage));
            }
        });
    }
    $scope.selectType = function (selectedType) {
        $scope.selectedType = selectedType;
    }
    $scope.clearType = function () {
        $scope.selectedType = undefined;
    }
    $scope.clearSearch = function () {
        $scope.selectedSearch = undefined;
    }

}]);

dbModule.controller("LogsJavaController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor) {

    var sql = "select * from Log";

    $scope.level = undefined;
    $scope.description = undefined;
    $scope.date = undefined;
    $scope.levels = ['1', '2', '3', '4', '5', '6', '7'];
    $scope.selectedDateFrom = undefined;
    $scope.selectedHourFrom = undefined;
    $scope.selectedDateTo = undefined;
    $scope.selectedHourTo = undefined;
    $scope.countPage = 1000;
    $scope.countPageOptions = [100, 500, 1000];

    $scope.metadata = CommandLogApi.refreshMetadata('monitor', function (data) {
        $scope.eventsWhen = listClassesForSuperclass('EventWhen');
        $scope.eventsWhen = listClassesForSuperclass('EventWhat');
    });

    $scope.getJavaLogs = function () {
        CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sql }, function (data) {
            if (data) {
                $scope.headers = CommandLogApi.getPropertyTableFromResults(data.result);
                $scope.resultTotal = data;
                $scope.results = data.result.slice(0, $scope.countPage);
                $scope.currentPage = 1;
                $scope.numberOfPage = new Array(Math.ceil(data.result.length / $scope.countPage));
            }
        });
    }
    $scope.getJavaLogs();
    $scope.parseTime = function (parsehour) {

        if (parsehour != undefined && parsehour != null && parsehour != '') {
            var hour = parsehour.split(" ");
            var hh = hour[0].split(":")[0]
            var mm = hour[0].split(":")[1];
            if (hour[1].contains('PM')) {
                hh = parseInt(hh) + 12;
            }
            return ' ' + hh + ':' + mm + ':00'
        }
        return '';
    }
    $scope.search = function () {
        var day = moment($scope.selectedDateFrom);

        var hourFrom = moment(new Date());

        var first = true
        var sql = "select * from Log ";

        if ($scope.level != undefined && $scope.level != null) {
            var sqlapp = "where level = " + $scope.level;

            sql = sql.concat(sqlapp);
            first = false;
        }
        if ($scope.description != undefined && $scope.description != null && $scope.description != '') {
            if (!first) {
                var sqlapp = " and  description like " + "'%" + $scope.description + "%' ";
                sql = sql.concat(sqlapp);
            }
            else {
                first = false;
                var sqlapp = " WHERE description like " + "'%" + $scope.description + "%' ";
                sql = sql.concat(sqlapp);
            }
        }
        if ($scope.selectedDateFrom != undefined && $scope.selectedDateFrom != null && $scope.selectedDateFrom != '') {

            var hour = $scope.parseTime($scope.selectedHourFrom);

            var day = moment($scope.selectedDateFrom);
            var formatted = day.format("YYYY-MM-DD");
            if (!first) {
                var sqlapp = " and  date >= " + "'" + formatted + '' + hour + "'";
                sql = sql.concat(sqlapp);
            }
            else {
                first = false;
                var sqlapp = " WHERE date >= " + "'" + formatted + '' + hour + "'";
                sql = sql.concat(sqlapp);
            }
        }
        if ($scope.selectedDateTo != undefined && $scope.selectedDateTo != null && $scope.selectedDateTo != '') {
            var hour = $scope.parseTime($scope.selectedHourTo);
            var day = moment($scope.selectedDateTo);
            var formatted = day.format("YYYY-MM-DD");
            if (!first) {
                var sqlapp = " and  date <= " + "'" + formatted + '' + hour + "'";
                sql = sql.concat(sqlapp);
            }
            else {
                first = false;
                var sqlapp = " WHERE date <= " + "'" + formatted + '' + hour + "'";
                sql = sql.concat(sqlapp);
            }
        }
        CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sql }, function (data) {
            if (data) {
                $scope.headers = CommandLogApi.getPropertyTableFromResults(data.result);
                $scope.resultTotal = data;
                $scope.results = data.result.slice(0, $scope.countPage);
                $scope.currentPage = 1;
                $scope.numberOfPage = new Array(Math.ceil(data.result.length / $scope.countPage));
            }
        });
    }
    $scope.checkDateFrom = function () {
        if ($scope.selectedDateFrom == undefined || $scope.selectedDateFrom == '') {
            return true;
        }
        return false
    }
    $scope.checkDateTo = function () {
        if ($scope.selectedDateTo == undefined || $scope.selectedDateTo == '') {
            return true;
        }
        return false
    }
}]);