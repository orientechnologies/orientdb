'use strict';


angular.module('webappApp').factory("Organization", function (Restangular) {
  return Restangular.service('orgs').one(ORGANIZATION);
});
