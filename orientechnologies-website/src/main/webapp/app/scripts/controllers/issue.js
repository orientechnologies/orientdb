'use strict';
angular.module('webappApp')
  .controller('IssueCtrl', function ($scope, Organization) {


    $scope.issues = Organization.all('issues').getList().$object;

  });

angular.module('webappApp')
  .controller('IssueNewCtrl', function ($scope, Organization) {


  });
angular.module('webappApp')
  .controller('IssueEditCtrl', function ($scope, $routeParams, Repo) {

    var id = $routeParams.id.split("@");

    var repo = id[0];
    var number = id[1];

    $scope.issue = Repo.one(repo).all("issues").one(number).get().$object;


    $scope.comments = Repo.one(repo).all("issues").one(number).all("comments").getList().$object;

  });

