var dbModule = angular.module('workbench-events.controller', ['workbench-logs.services']);

dbModule.controller("EventsController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', '$modal', '$q', 'MetricConfig', '$route', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor, $modal, $q, MetricConfig, $route) {

    var sql = "select * from Event";

    $scope.countPage = 1000;
    $scope.countPageOptions = [100, 500, 1000];
    $scope.selectedWhen = new Array;
    $scope.selectedWhat = new Array;

    //query degli event when


    $scope.refresh = function () {
        $scope.metadata = CommandLogApi.refreshMetadata('monitor', function (data) {
            $scope.eventsWhen = CommandLogApi.listClassesForSuperclass('EventWhen');
            $scope.eventsWhat = CommandLogApi.listClassesForSuperclass('EventWhat');
            $scope.selectedEventWhen = undefined;
            $scope.selectedEventWhat = undefined;
        });
    }

    $scope.refresh();

    $scope.prova = function (lll) {
        console.log(lll)
    }

    $scope.getEvents = function () {
        CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sql, shallow: 'shallow' }, function (data) {
            if (data) {
                $scope.headers = CommandLogApi.getPropertyTableFromResults(data.result);
                $scope.resultTotal = data;
                $scope.results = data.result.slice(0, $scope.countPage);
                $scope.currentPage = 1;
                $scope.numberOfPage = new Array(Math.ceil(data.result.length / $scope.countPage));
            }
        });
    }
    $scope.getEvents();

    $scope.onWhenChange = function (event, eventWhen) {
        modalScope = $scope.$new(true);

        modalScope.eventParent = event;
        if (event['when'] == undefined || event['when']['@class'] != $scope.selectedWhen[event.name] && $scope.selectedWhen[event.name] != undefined) {
            event['when'] = {};
            event['when']['@class'] = $scope.selectedWhen[event.name];
            event['when']['@type'] = 'd';
        }
        else {
            event['when']['@class'] = eventWhen['@class'];
        }
        modalScope.eventWhen = event['when'];
        modalScope.parentScope = $scope;
        var modalPromise = $modal({template: 'views/eventWhen/' + event['when']['@class'].toLowerCase() + '.html', scope: modalScope});
        $q.when(modalPromise).then(function (modalEl) {
            modalEl.modal('show');
        });
    }
    $scope.onWhatChange = function (event, eventWhat) {
        modalScope = $scope.$new(true);

        modalScope.eventParent = event;
        console.log(event['when']['@class'])
        if (event['what'] == undefined || (event['what']['@class'] != $scope.selectedWhat[event.name] && $scope.selectedWhat[event.name] != undefined)) {
            event['what'] = {};
            event['what']['@class'] = $scope.selectedWhat[event.name];
            event['what']['@type'] = 'd';
        }
        else {
            event['what']['@class'] = eventWhat['@class'];
        }
        modalScope.eventWhat = event['what'];
        modalScope.parentScope = $scope;
        var modalPromise = $modal({template: 'views/eventWhat/' + event['what']['@class'].toLowerCase() + '.html', scope: modalScope});
        $q.when(modalPromise).then(function (modalEl) {
            modalEl.modal('show');
        });
    }
    $scope.deleteEvent = function (event) {
        Utilities.confirm($scope, $modal, $q, {

            title: 'Warning!',
            body: 'You are dropping class ' + event['name'] + '. Are you sure?',
            success: function () {
                console.log(event['name']);
                var sql = 'DELETE FROM Event WHERE name = ' + "'" + event['name'] + "'";

                CommandLogApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
                    var index = $scope.results.indexOf(event);
                    $scope.results.splice(index, 1);
                    $scope.results.splice();
                });

            }

        });

    }
    $scope.refreshPage = function () {
        console.log('refresh');
        $scope.refresh();
        $route.reload();
    }
    $scope.saveEvent = function (result) {
        MetricConfig.saveConfig(result, function (data) {
            Utilities.message($scope, $modal, $q, {
                title: 'Message',
                body: data,
                success: function(){
                    $scope.refreshPage();
                }

            });


        }, function (error) {
            Utilities.message($scope, $modal, $q, {
                title: 'Error',
                body: error

            });
        })
    };

    $scope.newEvent = function () {
        var object = {"name": "", '@rid': "#-1:-1", "@class": "Event"};
        $scope.results.push(object);

    }
}
])
;

dbModule.controller("LogWhenController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', '$modal', '$q', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor, $modal, $q) {

    $scope.properties = CommandLogApi.listPropertiesForClass($scope.eventWhen['@class']);
}]);

dbModule.controller("MetricsWhenController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', '$modal', '$q', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor, $modal, $q) {

    $scope.properties = CommandLogApi.listPropertiesForClass($scope.eventWhen['@class']);
}]);

dbModule.controller("SchedulerWhenController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', '$modal', '$q', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor, $modal, $q) {

    $scope.properties = CommandLogApi.listPropertiesForClass($scope.eventWhen['@class']);
}]);
dbModule.controller("HttpWhatController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', '$modal', '$q', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor, $modal, $q) {
    $scope.properties = CommandLogApi.listPropertiesForClass($scope.eventWhat['@class']);
}]);


dbModule.controller("MailWhatController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', '$modal', '$q', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor, $modal, $q) {

    $scope.properties = CommandLogApi.listPropertiesForClass($scope.eventWhat['@class']);
}]);

dbModule.controller("FunctionWhatController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Monitor', '$modal', '$q', function ($scope, $http, $location, $routeParams, CommandLogApi, Monitor, $modal, $q) {

    $scope.properties = CommandLogApi.listPropertiesForClass($scope.eventWhat['@class']);
}]);
