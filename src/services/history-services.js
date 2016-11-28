let History = angular.module('history.services', []);
History.factory('History', function (localStorageService, Database) {

  var queries = {

    push: function (q) {

      var history = localStorageService.get("QueryHistory");

      if (!history) {
        history = {}
      }

      var db = Database.getName();
      var user = Database.currentUser();

      if (!history[db]) {
        history[db] = {};
      }

      if (!history[db][user]) {
        history[db][user] = [];
      }

      var idx = history[db][user].indexOf(q)

      if (idx != -1) {
        history[db][user].splice(idx, 1)
      }
      history[db][user].unshift(q);
      localStorageService.add("QueryHistory", history);
      return history[db][user];
    },
    histories: function () {
      var history = localStorageService.get("QueryHistory");
      var db = Database.getName();
      var user = Database.currentUser();

      if (!history || !history[db] || !history[db][user]) {
        return [];
      }

      return history[db][user];
    }

  }


  return queries;
});

export default History.name;
