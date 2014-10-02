var aside = angular.module('aside.controller', ['aside.services']);
aside.controller("AsideController", ['$scope', '$routeParams', '$http', '$location', 'Aside', function ($scope, $routeParams, $http, $location, Aside) {
    $scope.model = Aside.params;
    $scope.close = function () {
        Aside.hide();
    }
}]);
aside.controller("AsideManagerController", ['$scope', '$routeParams', '$http', '$location', 'Aside', '$rootScope', function ($scope, $routeParams, $http, $location, Aside, $rootScope) {


    $rootScope.$on('aside:open', function () {
        if (Aside.isAbsolute() == false) {
            $scope.asideClass = "col-md-3";
            $scope.containerClass = "col-md-9";
        }
    })
    $rootScope.$on('aside:close', function () {
        $scope.asideClass = "";
        $scope.containerClass = "";
    })
}]);