/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 02/09/13
 * Time: 18.41
 * To change this template use File | Settings | File Templates.
 */

var ctrl = angular.module('server.controller', []);
ctrl.controller("ServerController", ['$scope', '$routeParams', 'ServerApi', 'Database','ngTableParams', function ($scope, $routeParams, ServerApi, Database,ngTableParams) {

  $scope.active = $routeParams.tab || "conn";
  $scope.database = Database;
  $scope.tabs = ["conn", "config", "storage"];
  $scope.version = Database.getVersion();
  Database.setWiki("Server-Management.html")
  ServerApi.getServerInfo(function (data) {
    $scope.connections = data.connections;
    $scope.properties = data.properties;
    $scope.storages = data.storages;

    $scope.tableParams = new ngTableParams({
      page: 1,            // show first page
      count: 10          // count per page

    }, {
      total: $scope.connections.length, // length of data
      getData: function ($defer, params) {
//            use build-in angular filter
        var emtpy = !params.orderBy() || params.orderBy().length == 0;
        var orderedData = (params.sorting() && !emtpy) ?
          $filter('orderBy')($scope.connections, params.orderBy()) :
          $scope.connections;
        $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
      }
    });
  });


  $scope.getTemplate = function (tab) {
    return 'views/server/' + tab + '.html';
  }

  $scope.killConnection = function (n) {
    ServerApi.killConnection(n.connectionId, function () {
      var index = $scope.connections.indexOf(n);
      $scope.connections.splice(index, 1);
    });
  }
  $scope.interruptConnection = function (n) {
    ServerApi.interruptConnection(n.connectionId, function () {
      var index = $scope.connections.indexOf(n);
      $scope.connections.splice(index, 1);
    });
  }
}]);


ctrl.controller("ServerStatusController", ['$scope', '$rootScope', function ($scope, $rootScope) {

  $scope.isDown = false;
  $scope.serverclass = 'hide';
  $rootScope.$on('server:down', function () {
    $scope.isDown = true;
  })
  $rootScope.$on('server:up', function () {
    $scope.isDown = false;
  })
}]);
