import  '../views/database/context/bookmarksAside.html';

import AsideServices from '../services/aside-services';

let aside = angular.module('aside.controller', [AsideServices]);
aside.controller("AsideController", ['$scope', '$routeParams', '$http', '$location', 'Aside', function ($scope, $routeParams, $http, $location, Aside) {
  $scope.model = Aside.params;
  $scope.close = function () {
    Aside.hide();
  }
}]);
aside.controller("AsideManagerController", ['$scope', '$routeParams', '$http', '$location', 'Aside', '$rootScope', function ($scope, $routeParams, $http, $location, Aside, $rootScope) {


  $rootScope.$on('aside:open', function () {


    $scope.sticky = Aside.params.sticky;
    if (Aside.isAbsolute() == false) {

      if (!Aside.isSmall()) {
        $scope.asideClass = "col-md-3";
        $scope.containerClass = "col-md-9";
      } else {
        $scope.asideClass = "col-md-1";
        $scope.containerClass = "col-md-11";
      }
    }
  })

  $rootScope.$on('servermgmt:open', function () {
    $scope.containerClass = "app-view";
  })
  $rootScope.$on('servermgmt:close', function () {
    $scope.containerClass = "";
  })
  $rootScope.$on('aside:close', function () {
    $scope.asideClass = "";
    $scope.containerClass = "";
  })
}]);

export default  aside.name;
