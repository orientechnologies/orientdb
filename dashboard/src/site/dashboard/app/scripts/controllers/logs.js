//function LogsController($scope, $http, $location, $routeParams, Report, CommandLogApi) {
var dbModule = angular.module('workbench-logs.controller', ['workbench-logs.services']);
dbModule.controller("LogsController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi','Monitor', function ($scope, $http, $location, $routeParams, CommandLogApi,Monitor) {

    $scope.types = ['CONFIG', 'FINE', 'FINER', 'FINEST', 'INFO', 'SEVE', 'WARN'];
    $scope.files = ['ALL_FILES', 'LAST'];
    $scope.selectedType = undefined;
    $scope.selectedFile = undefined;
    Monitor.getServer($routeParams.server,function(data){
       $scope.server = data;
    });
//    $scope.selectedDateFrom = undefined;
//    $scope.selectedDateTo = undefined;
    $scope.results = undefined;
//    $scope.selectedHourFrom = undefined;
//    $scope.selectedHourTo = undefined;
    $scope.selectedSearch = '';

    $scope.getListFiles = function () {
        CommandLogApi.getListFiles(function (data) {

            if (data) {
                for (entry in data['files']) {
                    $scope.files.push(data['files'][entry]['name']);
                }
            }

        });
    }

    $scope.getListFiles();

    $scope.checkDateFrom = function () {
        if ($scope.selectedDateFrom == undefined || $scope.selectedDateFrom == '') {
//            $scope.selectedDateTo = undefined;
            return true;

        }
        return false
    }
    $scope.checkHourFrom = function () {
        if ($scope.selectedHourFrom == undefined || $scope.selectedHourFrom == '') {
//            $scope.selectedHourTo = undefined;
            return true;

        }
        return false
    }
    $scope.checkFile = function () {
        if ($scope.selectedFile == 'LAST') {
//            $scope.selectedDateFrom = undefined;
//            $scope.selectedDateTo = undefined;
//            $scope.selectedHourFrom = undefined;
//            $scope.selectedHourTo = undefined;
            return true;
        }
        return false;
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
        CommandLogApi.getLastLogs({server : $scope.server.name,file: filess, typeofSearch: typeofS, searchvalue: $scope.selectedSearch, logtype: $scope.selectedType, dateFrom: $scope.selectedDateFrom, hourFrom: $scope.selectedHourFrom, dateTo: $scope.selectedDateTo, hourTo: $scope.selectedHourTo}, function (data) {
            if (data) {
                $scope.results = data;
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