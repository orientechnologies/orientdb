'use strict';


angular.module('webappApp').factory("Repo", function (Restangular) {
  return Restangular.service('repos').one(ORGANIZATION);
});



