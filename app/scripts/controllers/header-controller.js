angular.module('header.controller', ['database.services']).controller("HeaderController", ['$scope', '$rootScope', '$routeParams', '$http', '$location', '$modal', '$q', 'Database', 'Aside', 'Profiler', function ($scope, $rootScope, $routeParams, $http, $location, $modal, $q, Database, Aside, Profiler) {
  $scope.database = Database;
  $scope.selectedMenu = null;
  $scope.menus = [];


  $scope.$watch(Database.getWiki, function (data) {
    if (data != null) {

      $scope.urlWiki = data;
    }

  });
  $scope.toggleAside = function () {
    Aside.toggle();
  }
  $scope.$watch(Database.getName, function (data) {


    $scope.links = {
      ee: "http://www.orientechnologies.com/orientdb-enterprise"
    }
    if (data != null) {

      $scope.menus = [
        {name: "browse", link: '#/database/' + data + '/browse', icon: "fa fa-eye", show: true},
        {name: "schema", link: '#/database/' + data + '/schema', icon: "fa fa-tasks", show: true},
        {name: "security", link: '#/database/' + data + '/security', icon: 'fa fa-user', show: true},
        {name: "graph", link: '#/database/' + data + '/graph', icon: 'fa fa-circle-o', show: true},
        {name: "functions", link: '#/database/' + data + '/functions', icon: 'fa fa-code', show: true},
        {name: "DB", link: '#/database/' + data + '/db', icon: 'fa fa-database', show: true},
        {
          name: "profiler",
          link: '#/database/' + data + '/profiler',
          icon: 'fa fa-rocket',
          ee: true,
          show: true,
          help: 'agent.notFound'
        },
        {
          name: "auditing",
          link: '#/database/' + data + '/auditing',
          icon: 'fa fa-headphones',
          ee: true,
          show: true,
          help: 'agent.notFound'
        },

      ];

      $scope.setSelected();

      Profiler.profilerData(data).then(function (data) {

      }).catch(function (error) {
        if (error.status == 405) {
          $scope.menus.forEach(function (e) {
            if (e.ee) {
              e.show = false;
            }
          })
        }
      });

    }
  });

  $scope.setSelected = function () {


    $scope.menus.forEach(function (element, index, array) {
      var find = $location.path().indexOf("/" + element.name.toLowerCase());

      if (find != -1) {
        $scope.selectedMenu = element;
        if (!$scope.$$phase && !$scope.$root.$$phase) {
          $scope.$apply();
        }
        return;
      }

    });
  }
  $scope.getClass = function (menu) {
    return menu == $scope.selectedMenu ? 'active' : '';
  }
  $rootScope.$on('$routeChangeSuccess', function (scope, next, current) {
    $scope.setSelected();
  });
  $scope.refreshMetadata = function () {
    Database.refreshMetadata($routeParams.database, function () {

    });
  };
  $scope.showAbout = function () {

    var modalScope = $scope.$new(true);
    modalScope.oVersion = Database.getMetadata()["server"].version;
    modalScope.version = STUDIO_VERSION;
    var modalPromise = $modal({template: 'views/server/about.html', show: false, scope: modalScope});
    modalPromise.$promise.then(modalPromise.show);

  }
  $scope.manageServer = function () {
    $location.path("/server");
  }
  $rootScope.$on('request:logout', function () {
    $scope.logout()
  })
  $scope.logout = function () {
    Database.disconnect(function () {
      $scope.menus = [];
      $location.path("/");
    });
  }

}
])
;
