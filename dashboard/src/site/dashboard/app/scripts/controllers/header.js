'use strict';

angular.module('MonitorApp')
    .controller('HeaderController', function ($scope, Monitor) {


        $scope.login = function () {
            Monitor.connect($scope.username, $scope.password, function (data) {
                     console.log(data);
            }, function (data) {

            });
        }
    });
angular.module('MonitorApp')
    .controller('MessageController', function ($scope, $timeout, $location, Message, MetricConfig) {


        $.extend($.gritter.options, {
            position: 'bottom-right', // defaults to 'top-right' but can be 'bottom-left', 'bottom-right', 'top-left', 'top-right' (added in 1.7.1)
        });
        (function tick() {
            $scope.data = Message.getUnread(function (data) {
                if (data.result.length > 0) {
                    data.result.forEach(function (elem) {
                        $.gritter.add({
                            // (string | mandatory) the heading of the notification
                            title: 'New Message (<a href="#/dashboard/messages">Inbox</a>)',
                            class_name: 'onotification',
                            // (string | mandatory) the text inside the notification
                            text: elem.message

                        });
                        elem.status = 'notified';
                        MetricConfig.saveConfig(elem, function (data) {

                        });

                    })

                }
            });
            $timeout(tick, 60000);
        })();

    });