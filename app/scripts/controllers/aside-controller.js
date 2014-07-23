angular.module('aside.controller', ['aside.services']).controller("AsideController", ['$scope', '$routeParams', '$http', '$location', 'Aside', function ($scope, $routeParams, $http, $location, Aside) {
    $scope.model = Aside.params;




    $scope.close = function () {
        Aside.hide();
    }
}]);