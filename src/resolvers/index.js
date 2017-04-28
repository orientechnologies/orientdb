let DatabaseResolve = {
  current: function (Database, $q, $route) {
    var deferred = $q.defer();
    if (!Database.getMetadata()) {
      Database.refreshMetadata($route.current.params.database, function () {
        deferred.resolve();
      });
    } else {
      deferred.resolve();
    }
    return deferred.promise;
  },
  delay: function ($q, $timeout) {
    var delay = $q.defer();
    $timeout(delay.resolve, 0);
    return delay.promise;
  }
}
let InstantDatabaseResolve = {
  current: function (Database, $q, $route) {
    var deferred = $q.defer();
    Database.refreshMetadata($route.current.params.database, function () {
      deferred.resolve();
    })
    return deferred.promise;
  },
  delay: function ($q, $timeout) {
    var delay = $q.defer();
    $timeout(delay.resolve, 0);
    return delay.promise;
  }
}


let AgentResolve = {
  current: function (AgentService, $q, ServerApi, $location) {
    var deferred = $q.defer();

    ServerApi.getServerInfo().then((data) => {
      AgentService.isActive().then(function () {
        deferred.resolve();
      })
    }).catch((err) => {
      if (err.status === 401) {
        $location.path("/");
      }
    });

    return deferred.promise;
  },
  delay: function ($q, $timeout) {
    var delay = $q.defer();
    $timeout(delay.resolve, 0);
    return delay.promise;
  }
}

export {DatabaseResolve, InstantDatabaseResolve, AgentResolve};
