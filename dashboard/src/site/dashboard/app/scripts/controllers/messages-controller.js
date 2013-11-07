var dbModule = angular.module('messages.controller', []);
dbModule.controller("MessagesController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor) {


    var sql = "select from Message order by date";
    var sqlCount = "select count(*) from Message where read = false ";
    CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sql, shallow: 'shallow' }, function (data) {
            $scope.messages = data.result;
        }
    );
    CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sqlCount, shallow: 'shallow' }, function (data) {
            console.log(data)
            $scope.countUnread= data.result[0]['count'];
        }
    );


}]);