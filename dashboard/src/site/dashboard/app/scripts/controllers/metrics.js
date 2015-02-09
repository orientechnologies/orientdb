'use strict';

var app = angular.module('MonitorApp');
app.controller('SingleMetricController', function ($scope, $location, $routeParams, $timeout, Monitor, Metric, $modal, $q) {


        $scope.loading = false;
        $scope.pollTime = 10000;
        $scope.render = "bar";
        $scope.compress = "1"
        $scope.range = { start: moment().subtract('days', 6), end: moment()};
        $scope.popover = { compress: $scope.compress, pollTime: $scope.pollTime, range: $scope.range, render: $scope.render};


        $scope['fs'] = $scope.metricScope['fullScreen'];
        $scope.metricScope.$watch($scope.metric, function (data) {
            $scope.config = data;
            if (data && $scope.range)
                $scope.refreshData(data, $scope.range.start.format("YYYY-MM-DD HH:mm:ss"), $scope.range.end.format("YYYY-MM-DD HH:mm:ss"));

        });
        var real;
        $scope.startRealtime = function () {
            if ($scope.realtime) {
                real = $timeout(function () {
                    $scope.refreshRealtime($scope.config);
                    $scope.startRealtime();
                }, $scope.pollTime);
            }
        }
        $scope.stopRealtime = function () {
            $timeout.cancel(real);
        }
        $scope.$watch('realtime', function (data) {
            if (data != undefined) {
                if (data) {
                    $scope.metricsData = null;
                    $scope.refreshRealtime($scope.config)
                    $scope.startRealtime();
                    $scope.render = "area";
                } else {
                    $scope.metricsData = null;
                    $scope.stopRealtime();
                    $scope.metricsData = null;
                    if ($scope.range)
                        $scope.refreshData($scope.config, $scope.range.start.format("YYYY-MM-DD HH:mm:ss"), $scope.range.end.format("YYYY-MM-DD HH:mm:ss"));
                }
            }
        });
        $scope.$on('$routeChangeStart', function (scope, next, current) {
            $scope.stopRealtime();
        });
        $scope.fullScreen = function () {
            var modalScope = $scope.$new(true);
            modalScope.metricScope = modalScope;
            modalScope.metricScope.selectedConfig = $scope.config;
            modalScope.metricScope.fullScreen = true;
            var modalPromise = $modal({template: 'views/modal/metric.html', scope: modalScope, modalClass: 'viewChart'});
            $q.when(modalPromise).then(function (modalEl) {
                modalEl.modal('show');
            });
        }
        $scope.applyChanges = function () {
            if ($scope.popover.compress) {
                $scope.compress = $scope.popover.compress;
            }
            if ($scope.popover.range) {
                $scope.range = $scope.popover.range;
            }
            if ($scope.popover.pollTime) {
                $scope.pollTime = $scope.popover.pollTime;
            }
            if ($scope.popover.render) {
                $scope.render = $scope.popover.render;
            }
            if ($scope.popover.realtime != undefined) {
                $scope.realtime = $scope.popover.realtime;
            } else {
                $scope.refreshData($scope.config, $scope.range.start.format("YYYY-MM-DD HH:mm:ss"), $scope.range.end.format("YYYY-MM-DD HH:mm:ss"));
            }


        }
        $scope.refreshRealtime = function (metrics) {

            var names = "";
            metrics.config.forEach(function (elem, idx) {
                if (idx == 0) {
                    names = elem.name;
                } else {
                    names = names + "," + elem.name;
                }

            });
            var databases = undefined;
            if (metrics.databases) {
                databases = metrics.databases;
            }
            if (!metrics.server.name) {
                Monitor.getServer(metrics.server, function (data) {
                    if (databases) {
                        var params = {  server: data.name, databases: databases, type: 'realtime', kind: 'chrono', names: names };
                    } else {
                        var params = {  server: data.name, type: 'realtime', kind: 'chrono', names: names };
                    }
                    Metric.get(params, function (data) {
                        $scope.renderRealTimeData(data, metrics);
                    });
                });
            } else {
                if (databases) {

                    var params = {  server: metrics.server.name, databases: databases, type: 'realtime', kind: 'chrono', names: names };
                } else {

                    var params = {  server: metrics.server.name, type: 'realtime', kind: 'chrono', names: names };
                }
                var params = {  server: metrics.server.name, type: 'realtime', kind: 'chrono', names: names };
                Metric.get(params, function (data) {
                    $scope.renderRealTimeData(data, metrics);
                });
            }
        }
        var getFirstKey = function (data) {
            for (var elem in data)
                return elem;
        };
        $scope.renderRealTimeData = function (data, metrics) {
            var tmpArr = new Array;
            var names = new Array;
            var configs = new Array;
            if (!$scope.metricsData)$scope.metricsData = new Array;

            metrics.config.forEach(function (elem, idx, array) {
                names.push(elem.name);
                configs[elem.name] = elem.field;
            });
            data.result.forEach(function (elem, idx, array) {
                var date = new Date();
                Object.keys(elem).forEach(function (elemKey) {
                    if (!tmpArr[elemKey]) {
                        tmpArr[elemKey] = new Array;
                    }
                    var el = undefined;
                    if (configs[elemKey]) {
                        el = elem[elemKey][configs[elemKey]];
                    } else if (elem['class'] == 'Information') {
                        el = elem.value
                    } else {
                        el = elem.entries;
                    }
                    tmpArr[elemKey][moment(date).format("YYYY-MM-DD HH:mm:ss")] = el;
                });
            });
            var clone = new Array;
            Object.keys($scope.metricsData).forEach(function (k) {
                if (!clone[k]) {
                    clone[k] = new Array;
                }
                var len = Object.keys($scope.metricsData[k]).length;
                Object.keys($scope.metricsData[k]).forEach(function (sK, idx) {

                    if (len < 20)
                        clone[k][sK] = $scope.metricsData[k][sK];
                });
            });
            Object.keys(tmpArr).forEach(function (k) {
                if (!clone[k]) {
                    clone[k] = new Array;
                }
                Object.keys(tmpArr[k]).forEach(function (sK) {
                    if (!clone[k][sK]) {
                        clone[k][sK] = new Array;
                    }
                    var lastDiff = tmpArr[k][sK];
                    var cfg = configs[k];

                    if ($scope.lastArr) {
                        lastDiff = $scope.lastArr[k][getFirstKey($scope.lastArr[k])];
                    }
                    if (cfg != 'entries' && cfg != 'total') {
                        lastDiff = 0;
                    }
                    clone[k][sK] = tmpArr[k][sK] - lastDiff;
                });

            });
            $scope.metricsData = clone;

            $scope.lastArr = tmpArr;
        }

        $scope.refreshData = function (metrics, dataFrom, dataTo) {


            $scope.loading = true;
            var names = new Array;
            var configs = new Array;

            if (metrics.config) {
                metrics.config.forEach(function (elem, idx, array) {
                    names.push(elem.name);
                    configs[elem.name] = elem.field;
                });

                var calculateArray = function (data) {
                    var tmpArr = new Array;

                    data.result.forEach(function (elem, idx, array) {
                        if (!tmpArr[elem.name]) {
                            tmpArr[elem.name] = new Array;
                        }
                        var el = undefined;

                        if (configs[elem.name]) {
                            el = elem[configs[elem.name]];
                        } else if (elem['class'] == 'Information') {
                            el = elem.value
                        } else {
                            el = elem.entries;
                        }
                        tmpArr[elem.name][elem.dateTo] = el;
                    });
                    $scope.metricsData = tmpArr

                    $scope.loading = false;
                }
                var databases = undefined;
                if (metrics.databases) {
                    databases = metrics.databases;

                }
                var compress = $scope.compress || 'none';

                if (metrics.server) {
                    if (!metrics.server.name) {
                        Monitor.getServer(metrics.server, function (data) {
                            if (databases) {
                                var params = {  server: data.name, databases: databases, type: 'snapshot', kind: 'chrono', names: names, limit: '100', compress: compress, from: dataFrom, to: dataTo };
                            } else {
                                var params = {  server: data.name, type: 'snapshot', kind: 'chrono', names: names, limit: '100', compress: compress, from: dataFrom, to: dataTo };
                            }

                            Metric.get(params, function (data) {
                                calculateArray(data);
                            });
                        });
                    } else {
                        if (databases) {
                            var params = {  server: metrics.server.name, databases: databases, type: 'snapshot', kind: 'chrono', names: names, compress: compress, from: dataFrom, to: dataTo };
                        } else {
                            var params = {  server: metrics.server.name, type: 'snapshot', kind: 'chrono', names: names, compress: compress, from: dataFrom, to: dataTo };
                        }

                        Metric.get(params, function (data) {
                            calculateArray(data);
                        });
                    }
                }
            }
        }
    }
)
;

app.controller('MetricsMonitorController', function ($scope, $location, $routeParams, $odialog, Monitor, Metric, Server, MetricConfig, Settings) {

    $scope.rid = $routeParams.server;
    $scope.names = new Array;
    $scope.render = 'bar';
    $scope.fields = ['value', 'entries', 'min', 'max', 'average', 'total'];
    $scope.mType = {};
    $scope.inDashboard = {};
    Monitor.getServers(function (data) {
        $scope.servers = data.result;
    });


    Metric.getMetricTypes({}, function (data) {
        $scope.metrics = data.result;
        if ($scope.metrics.length > 0) {
            $scope.metric = $scope.metrics[0].name;

        }
        $scope.metrics.forEach(function (elem) {
            $scope.mType[elem.name] = elem.type;
        });
    });
    $scope.refreshMetricConfig = function () {
        MetricConfig.getAll(function (data) {
            $scope.savedMetrics = data.result;
            if ($scope.savedMetrics.length > 0) {
                $scope.selectedConfig = $scope.savedMetrics[0];
                $scope.currentIdx = 0;
            } else {
                $scope.selectedConfig = MetricConfig.create();
            }
        });
    }


    $scope.newMetricConfig = function () {
        $scope.selectedConfig = MetricConfig.create();
    }
    $scope.saveMetricConfig = function () {

        var rid = $scope.selectedConfig["@rid"]
        MetricConfig.saveConfig($scope.selectedConfig, function (data) {

            $scope.testMsg = 'Metrics configuration saved.';
            $scope.testMsgClass = 'alert alert-setting'

            if (rid == "#-1:-1") {

                var msg = 'Do you want to add the new chart ' + data.name + ' to the Dashboard?';
                $odialog.confirm({title: 'Info', body: msg, success: function () {
                    $scope.addToDashboard(data);
                }, cancel: function () {
                    $scope.refreshMetricConfig();
                }});

            } else {
                $scope.refreshMetricConfig();
            }

        });
    }

    $scope.selectConfig = function (config, idx) {
        $scope.selectedConfig = config;
        $scope.currentIdx = idx;
    }
    $scope.addToDashboard = function (config) {

        if (!$scope.config['metrics']) {
            $scope.config['metrics'] = new Array;
        }

        $scope.inDashboard[config["@rid"]] = true;
        $scope.config['metrics'].push(config);
        $scope.saveDashboardConfig();

    }
    $scope.removeFromDashboard = function (config) {

        var index = -1;

        $scope.config['metrics'].forEach(function (element, idx, arr) {

            if (config["@rid"] == element["@rid"]) {
                index = idx;
                delete $scope.inDashboard[element["@rid"]];
            }
        });

        if (index != -1) {
            $scope.config['metrics'].splice(index, 1);
            $scope.saveDashboardConfig(true);
        }
    }
    $scope.deleteConfig = function (config) {
        var msg = 'You are removing chart ' + config.name + '. Are you sure?';
        $odialog.confirm({title: 'Warning', body: msg, success: function () {
            MetricConfig.deleteConfig(config, function (data) {
                $scope.refreshMetricConfig();
            });
        }});

    }
    $scope.addConfig = function () {
        if (!$scope.selectedConfig['config']) {
            $scope.selectedConfig['config'] = new Array;
        }
        $scope.selectedConfig['config'].push({});
    }

    $scope.removeMetric = function (met) {
        var idx = $scope.selectedConfig['config'].indexOf(met);
        $scope.selectedConfig['config'].splice(idx, 1);
    }
    $scope.refreshMetricConfig();


    $scope.saveDashboardConfig = function (remove) {
        Settings.put($scope.config, function (data) {
            if (remove) {
                $scope.testMsg = 'Chart removed from dashboard.';
            } else {
                $scope.testMsg = 'Chart added to dashboard.';
            }


            $scope.testMsgClass = 'alert alert-setting'
            $scope.refreshMetricConfig();
            $scope.init();

        }, function (error) {
            $scope.testMsg = error;
            $scope.testMsgClass = 'alert alert-error alert-setting'
        });
    }
    $scope.init = function () {
        Settings.get(function (data) {
            if (data.result.length == 0) {
                $scope.config = Settings.new();
            } else {
                $scope.config = data.result[0];

                if ($scope.config['metrics']) {
                    !$scope.config['metrics'].forEach(function (elem) {
                        $scope.inDashboard[elem["@rid"]] = true;
                    });

                }
            }
        });
    }
    $scope.init();
});

app.controller('ConfigChartController', function ($scope, $location, $routeParams) {


});
