angular.module('header.controller', ['database.services']).controller("HeaderController", ['$scope', '$routeParams', '$http', '$location', '$modal', '$q', 'Database', function ($scope, $routeParams, $http, $location, $modal, $q, Database) {
    $scope.database = Database;
    $scope.selectedMenu = null;
    $scope.menus = [];
//    $scope.urlWiki = Database.getWiki();

    $scope.$watch(Database.getWiki, function (data) {
        if (data != null) {
            $scope.urlWiki = data;
        }

    });
    $scope.$watch(Database.getName, function (data) {

        if (data != null) {
            $scope.setSelected();
            $scope.menus = [
                { name: "browse", link: '#/database/' + data + '/browse', icon: "icon-eye-open", wiki: "https://github.com/orientechnologies/orientdb-studio/wiki/Query"},
                { name: "schema", link: '#/database/' + data + '/schema', icon: "icon-tasks", wiki: "https://github.com/orientechnologies/orientdb-studio/wiki/Schema"},
                { name: "security", link: '#/database/' + data + '/users', icon: 'icon-user', wiki: ""},
                { name: "functions", link: '#/database/' + data + '/functions', icon: 'icon-signal', wiki: "https://github.com/orientechnologies/orientdb-studio/wiki/Functions"},
                { name: "DB", link: '#/database/' + data + '/db', icon: 'icon-book'}

            ];
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
    $scope.$on('$routeChangeSuccess', function (scope, next, current) {
        //$scope.refreshMetadata();
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
        var modalPromise = $modal({template: 'views/server/about.html', persist: true, show: false, backdrop: 'static', scope: modalScope});
        $q.when(modalPromise).then(function (modalEl) {
            modalEl.modal('show');
        });
    }
    $scope.logout = function () {
        Database.disconnect(function () {
            $scope.menus = [];
            $location.path("/");
        });
    }

}]);