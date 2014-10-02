angular.module('notification.controller', ['notification.services']).controller("NotificationController", ['$scope', 'Notification', '$rootScope', function ($scope, Notification, $rootScope) {
    $scope.alerts = Notification.notifications;
    $scope.errors = Notification.errors;
    $scope.warnings = Notification.warnings;

    $scope.clear = function () {
        Notification.clear();
    }
    $scope.onAlertHover = function () {
        $rootScope.$broadcast("alert:hover");
    }
    $scope.onAlertOut = function () {
        $rootScope.$broadcast("alert:out");
    }
}]);