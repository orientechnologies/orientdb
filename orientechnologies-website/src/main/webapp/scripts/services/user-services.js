'use strict';


angular.module('webappApp').factory("User", function (Restangular, $q) {

  var userService = Restangular.all('user');
  var allUserService = Restangular.all('users');
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
      var found = false
      this.current.repositories.forEach(function (e) {
        if (e.organization.name == repo) {
          found = true;
        }
      })
      return found;
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
    },
    save: function (user) {
      var deferred = $q.defer();
      var self = this;
      allUserService.one(user.name).patch(user).then(function (data) {
        self.current = data;
        deferred.resolve(data);
      });
      return deferred.promise;
    },
    environments: function () {
      var deferred = $q.defer();
      allUserService.one(this.current.name).all('environments').getList().then(function (data) {
        deferred.resolve(data);
      })
      return deferred.promise;
    },
    addEnvironment: function (env) {
      var deferred = $q.defer();
      allUserService.one(this.current.name).all('environments').post(env).then(function (data) {
        deferred.resolve(data);
      })
      return deferred.promise;
    },
    deleteEnvironment: function (env) {
      var deferred = $q.defer();
      allUserService.one(this.current.name).all('environments').one(env.eid.toString()).remove().then(function (data) {
        deferred.resolve(data);
      })
      return deferred.promise;
    },
    changeEnvironment: function (env) {
      var deferred = $q.defer();
      allUserService.one(this.current.name).all('environments').one(env.eid.toString()).patch(env).then(function (data) {
        deferred.resolve(data);
      })
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
