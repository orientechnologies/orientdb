var dbModule = angular.module('messages.controller', []);
dbModule.controller("MessagesController", ['$scope', '$http', '$route', '$location', '$routeParams', 'CommandLogApi', 'Monitor', 'MetricConfig', '$modal', '$q', function ($scope, $http, $route, $location, $routeParams, CommandLogApi, Monitor, MetricConfig, $modal, $q) {
    $scope.countPage = 5;
    $scope.countPageOptions = [5, 10, 20];
    $scope.unread = 'unread';
    var sql = "select from Message order by date";
    var sqlCount = "select count(*) from Message where read = false ";
    var sqlCountAll = "select count(*) from Message ";

    $scope.refreshCount = function () {
        CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sqlCount, shallow: 'shallow' }, function (data) {
                $scope.countUnread = data.result[0]['count'];
            }
        );
        CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sqlCountAll, shallow: 'shallow' }, function (data) {
                $scope.countAll = data.result[0]['count'];
            }
        );
    }
    $scope.refresh = function () {
        CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sql, shallow: 'shallow' }, function (data) {
                $scope.msgsTotal = data.result;
                $scope.msgs = data.result.slice(0, $scope.countPage);
                $scope.currentPage = 1;
                $scope.numberOfPage = new Array(Math.ceil(data.result.length / $scope.countPage));
            }
        );

    }
    $scope.refresh();
    $scope.refreshCount();
    $scope.selectMsg = function (msg) {
        $scope.selectedName = msg;
    }

    $scope.save = function () {

        var resultsApp = JSON.parse(JSON.stringify($scope.msgsTotal));
        resultsApp.forEach(function (elem, idx, array) {
            MetricConfig.saveConfig(elem, function (data) {
                    var index = array.indexOf(elem);
                    array.splice(index, 1);
                    if (array.length == 0) {
                        $scope.testMsg = "Messages updated successfully.";
                        $scope.testMsgClass = 'alert alert-setting';
                        $scope.refresh();
                        $scope.refreshCount();
                    }
                },
                function (error) {
                    $scope.testMsg = error;
                    $scope.testMsgClass = 'alert alert-error alert-setting';
                });
        });
    }
    $scope.checkAll = function (bool) {
        for (var entry in $scope.msgsTotal) {

            if (bool != $scope.msgsTotal[entry]['read'] && $scope.msgsTotal[entry]['read']) {
                $scope.countUnread++;
            }
            else if (bool != $scope.msgsTotal[entry]['read'] && !$scope.msgsTotal[entry]['read']) {
                $scope.countUnread--;
            }
            $scope.msgsTotal[entry]['read'] = bool;
        }
        $scope.save();
//        $scope.refreshCount();
    }
    $scope.switchPage = function (index) {
        if (index != $scope.currentPage) {
            $scope.currentPage = index;
            $scope.msgs = $scope.msgsTotal.slice(
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
    $scope.openMessageText = function (msg) {


        $scope.messageText = msg;


        if (!msg['read']) {
            $scope.countUnread--;
        }
        msg['read'] = true;
        MetricConfig.saveConfig(msg, function (data) {

                for (var entry in $scope.msgs) {

                    if ($scope.msgs[entry]['@rid'] == msg['@rid']) {

                        $scope.msgs[entry] = data;
                    }
                }


                for (var entry in $scope.msgsTotal) {
                    if ($scope.msgsTotal[entry]['@rid'] == msg['@rid']) {

                        $scope.msgsTotal[entry] = data;
                    }
                }
            },
            function (error) {
                $scope.testMsg = error;
                $scope.testMsgClass = 'alert alert-error alert-setting';
            });

        $scope.selectedName = msg;
    }
    $scope.deleteAll = function () {
        Utilities.confirm($scope, $modal, $q, {

            title: 'Warning!',
            body: 'Delete all messages' + '. Are you sure?',
            success: function () {
                for (var entry in $scope.msgsTotal) {

                    var sql = 'DELETE FROM Message WHERE @rid = ' + $scope.msgsTotal[entry]['@rid'];

                    CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
                    });

                }
                $scope.messageText = null;
                $scope.refresh();
                $scope.refreshCount();
            }

        });
    }
    $scope.deleteMsg = function (msg) {
        var index = $scope.msgs.indexOf(msg);
        $scope.msgs.splice(index, 1);
        index = $scope.msgsTotal.indexOf(msg);
        $scope.msgsTotal.splice(index, 1);
        MetricConfig.deleteConfig(msg, function (data) {

            $scope.testMsg = "Message deleted";
            $scope.testMsgClass = 'alert alert-setting';
            $scope.refresh();
            $scope.refreshCount();

        });
    }
}]);