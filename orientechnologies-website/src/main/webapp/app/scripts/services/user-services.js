'use strict';


angular.module('webappApp').factory("User", function (Restangular, $q) {

  var userService = Restangular.all('user');

  return {

    current: {},

    isClient: function (org) {

      var found = false;
      if (this.current.clientsOf) {

        this.current.clientsOf.forEach(function (o) {
          if (org == o.name) {
            found = true;
          }
        });
      }
      return found;
    },
    getClient: function (org) {

      if (this.current.clients) {
        return this.current.clients[0];
      }
      return null;
    },
    isMember: function (repo) {
      return this.current.repositories.length > 0;
    },
    whoami: function () {
      var deferred = $q.defer();
      var self = this;
      if (!self.current.name) {
        userService.customGET().then(function (data) {
          self.current = data;
          deferred.resolve(data);
        });
      } else {
        deferred.resolve(self.current);
      }
      return deferred.promise;
    }
  }
});

angular.module('webappApp').service("AccessToken", function ($localStorage) {

  return {
    get: function () {
      return $localStorage.token;
    },
    set: function (token) {
      $localStorage.token = token;
    },
    delete: function () {
      delete  $localStorage.token;
    }
  }
});
