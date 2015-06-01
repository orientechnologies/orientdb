'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
    .controller('MainCtrl', function ($scope, Organization, User) {


        $scope.issue = {}
        User.whoami().then(function (data) {
            $scope.member = data;
            $scope.issue.assignee = data;
            if (User.isMember(ORGANIZATION)) {
                $scope.query = 'is:open ' + 'assignee:' + $scope.member.name + " sort:priority-desc";
            } else if (User.isClient(ORGANIZATION)) {
                var client = User.getClient(ORGANIZATION);
                $scope.query = 'is:open client:\"' + client.name + "\" sort:priority-desc";
            } else {
                $scope.query = 'is:open ';
            }
            Organization.all("members").getList().then(function (data) {
                $scope.assignees = data.plain();
            })
            Organization.all("milestones").all('current').getList().then(function (data) {
                $scope.milestones = data.plain().map(function (m) {
                    m.current = true;
                    return m;
                });
            })
            Organization.all('issues').customGET("", {q: $scope.query, page: $scope.page}).then(function (data) {
                $scope.issues = data.content;
            });
            function loadBoard() {

                if ($scope.isMember) {
                    var assignee = $scope.issue.assignee ? $scope.issue.assignee.name : "";
                    var assigneeFilter = assignee == "" ? "" : 'assignee:' + assignee;
                    var milestone = $scope.issue.milestone ? "milestone:\"" + $scope.issue.milestone.title + "\"" : "milestone:_current";
                    $scope.queryBacklog = 'is:open ' + assigneeFilter + " !label:\"in progress\" " + milestone + " sort:dueTime-asc sort:priority-desc";
                    Organization.all('board').all("issues").customGET("", {
                        q: $scope.queryBacklog,
                        page: $scope.page
                    }).then(function (data) {
                        $scope.backlogs = data.content;
                    });
                    milestone = $scope.issue.milestone ? "milestone:\"" + $scope.issue.milestone.title + "\"" : "";
                    $scope.queryProgress = 'is:open ' + assigneeFilter + " label:\"in progress\" " + milestone + " sort:dueTime-asc sort:priority-desc ";
                    Organization.all('board').all("issues").customGET("", {
                        q: $scope.queryProgress,
                        page: $scope.page
                    }).then(function (data) {
                        $scope.inProgress = data.content;
                    });

                    $scope.queryZombies = 'is:open no:assignee sort:createdAt-desc'
                    Organization.all('board').all("issues").customGET("", {
                        q: $scope.queryZombies,
                        page: $scope.page,
                        per_page: 6
                    }).then(function (data) {
                        $scope.zombies = data.content;
                    });
                }
                if ($scope.isMember || $scope.isSupport) {

                    $scope.queryClient = 'is:open has:client sort:dueTime-asc sort:priority-desc '
                    Organization.all('board').all("issues").customGET("", {
                        q: $scope.queryClient,
                        page: $scope.page,
                        per_page: 6
                    }).then(function (data) {
                        $scope.clientIssues = data.content;
                    });
                }
            }

            $scope.isMember = User.isMember(ORGANIZATION);
            $scope.isSupport = User.isSupport(ORGANIZATION);


            loadBoard();


            $scope.$on("assignee:changed", function (e, assignee) {

                $scope.issue.assignee = assignee;
                loadBoard();

            });
            $scope.$on("milestone:changed", function (e, m) {
                $scope.issue.milestone = m;
                loadBoard();
            });
        });


    });
