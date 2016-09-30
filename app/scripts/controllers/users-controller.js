var schemaModule = angular.module('users.controller', ['database.services']);
schemaModule.controller("SecurityController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'FunctionApi', 'DocumentApi', '$modal', '$q', '$route', function ($scope, $routeParams, $location, Database, CommandApi, FunctionApi, DocumentApi, $modal, $q, $route) {

  $scope.database = Database;
  $scope.db = $routeParams.database;
  $scope.active = $routeParams.tab || "users";
  $scope.tabs = ['users', 'roles'];
  $scope.tabsI18n = new Array;
  $scope.tabsI18n['users'] = 'Users';
  $scope.tabsI18n['roles'] = 'Roles';

  Database.setWiki("Security.html");
  $scope.getTemplate = function (tab) {
    return 'views/database/security/' + tab + '.html';
  }
}]);
schemaModule.controller("UsersController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', '$filter', 'ngTableParams', 'DocumentApi', '$modal', '$q', 'Notification', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route, $filter, ngTableParams, DocumentApi, $modal, $q, Notification) {

  $scope.database = Database;
  $scope.usersResult = new Array;


  $scope.links = {
    users: Database.getOWikiFor("Security.html#users")
  }


  $scope.strictSql = Database.isStrictSql();


  if ($scope.strictSql) {
    var selectAllUsers = 'select * from oUser order by name fetchPlan *:1';
    var selectAllRoles = 'select * from oRole order by name fetchPlan *:1';
  } else {
    var selectAllUsers = 'select * from oUser fetchPlan *:1 order by name ';
    var selectAllRoles = 'select * from oRole fetchPlan *:1 order by name ';
  }

  $scope.getListUsers = function () {
    $scope.functions = new Array;
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      verbose: false,
      text: selectAllUsers,
      limit: $scope.limit,
      shallow: false
    }, function (data) {
      $scope.usersResult = data.result;


      $scope.tableParams = new ngTableParams({
        page: 1,            // show first page
        count: 10          // count per page

      }, {
        total: $scope.usersResult.length, // length of data
        getData: function ($defer, params) {
          var orderedData = params.sorting() ?
            $filter('orderBy')($scope.usersResult, params.orderBy()) :
            $scope.usersResult;
          $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
        }
      });
    });
  }
  $scope.loadRoles = function ($query) {
    return $scope.getListRoles();
  }
  $scope.getListRoles = function () {
    var deferred = $q.defer();
    $scope.functions = new Array;
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      verbose: false,
      text: selectAllRoles,
      limit: $scope.limit,
      shallow: false
    }, function (data) {
      $scope.roles = data.result;
      deferred.resolve($scope.roles);
      $scope.dataRoles = [];
      $scope.roles.forEach(function (e, idx, arr) {
        $scope.dataRoles.push({id: idx, text: e});
      })
    });
    return deferred.promise;
  }

  $scope.addUser = function () {
    var modalScope = $scope.$new(true);
    modalScope.user = DocumentApi.createNewDoc("OUser");
    modalScope.user.roles = [];
    modalScope.select2Options = $scope.select2Options;
    modalScope.loadRoles = $scope.loadRoles;
    modalScope.title = "New User"
    modalScope.saveButton = "Add User"
    var modalPromise = $modal({template: 'views/database/users/newUser.html', scope: modalScope, show: false});
    modalScope.save = function () {
      if (modalPromise.$scope.roles) {
        modalPromise.$scope.user.roles = [];
        modalPromise.$scope.roles.forEach(function (e) {
          modalPromise.$scope.user.roles.push(e.text["@rid"])
        });
      }
      DocumentApi.createDocument($scope.database.getName(), modalPromise.$scope.user["@rid"], modalPromise.$scope.user).then(function (data) {
        data.roles = modalPromise.$scope.user.roles;
        $scope.usersResult.push(data);
        $scope.tableParams.reload();
        Notification.push({content: 'User ' + data.name + ' has been created.'});
      }, function error(err) {
        Notification.push({content: err, error: true});
      })
    }
    modalPromise.$promise.then(modalPromise.show);
  }
  $scope.tagAdded = function (tag, user) {

    var query = 'update {{rid}} add roles = {{role}}';
    query = S(query).template({rid: user['@rid'], role: tag['@rid']}).s;

    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      verbose: false,
      text: query,
      limit: $scope.limit,
      shallow: false
    }, function (data) {
      Notification.push({
        content: S("Role '{{role}}' added to user  '{{user}}'").template({
          role: tag.name,
          user: user.name
        }).s
      });
    });
  }
  $scope.tagRemoved = function (tag, user) {

    var query = 'update {{rid}} remove roles = {{role}}';
    query = S(query).template({rid: user['@rid'], role: tag['@rid']}).s;

    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      verbose: false,
      text: query,
      limit: $scope.limit,
      shallow: false
    }, function (data) {

      Notification.push({
        content: S("Role '{{role}}' removed from user  '{{user}}'").template({
          role: tag.name,
          user: user.name
        }).s
      });

    });
  }
  $scope.edit = function (user) {
    var modalScope = $scope.$new(true);
    modalScope.user = user;
    modalScope.loadRoles = $scope.loadRoles;
    modalScope.title = "Edit User"
    modalScope.saveButton = "Save User"
    var modalPromise = $modal({template: 'views/database/users/newUser.html', scope: modalScope, show: false});
    modalScope.save = function () {
      if (modalPromise.$scope.roles) {
        modalPromise.$scope.user.roles = [];
        modalPromise.$scope.roles.forEach(function (e) {
          modalPromise.$scope.user.roles.push(e.text["@rid"])
        });
      }
      DocumentApi.updateDocument($scope.database.getName(), modalPromise.$scope.user["@rid"], modalPromise.$scope.user).then(function (data) {

        var idx = $scope.usersResult.indexOf(modalPromise.$scope.user);
        $scope.usersResult.splice(idx, 1);
        data.roles = modalPromise.$scope.user.roles;
        $scope.usersResult.push(data);
        $scope.tableParams.reload();
        Notification.push({content: 'User ' + data.name + ' has been updated.'});

      }, function error(err) {
        Notification.push({content: err, error: true});
      })
    }
    modalPromise.$promise.then(modalPromise.show);
  }
  $scope.delete = function (user) {
    Utilities.confirm($scope, $modal, $q, {
      title: 'Warning!',
      body: 'You are deleting user ' + user.name + '. Are you sure?',
      success: function () {
        DocumentApi.deleteDocument($scope.database.getName(), user['@rid']).then(function (data) {
          Notification.push({content: 'User ' + user.name + ' has been deleted.'});
          var idx = $scope.usersResult.indexOf(user);
          if (idx > -1) {
            $scope.usersResult.splice(idx, 1);
            $scope.tableParams.reload();
          }
        }).catch(function () {

        });
      }
    });
  }

  $scope.getListUsers();

}]);

schemaModule.controller("RolesController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'CommandApi', 'Database', 'Notification', 'DocumentApi', '$modal', '$q', function ($scope, $routeParams, $location, DatabaseApi, CommandApi, Database, Notification, DocumentApi, $modal, $q) {

  $scope.database = Database;


  $scope.strictSql = Database.isStrictSql();


  if ($scope.strictSql) {
    var selectAllUsers = 'select * from oRole order by name fetchPlan *:1 ';
  } else {
    var selectAllUsers = 'select * from oRole fetchPlan *:1  order by name ';
  }


  $scope.links = {
    resources: Database.getOWikiFor("Security.html#resources"),
    roles: Database.getOWikiFor("Security.html#roles"),
    roles_error: Database.getOWikiFor("Security.html#roles_error")
  }
  $scope.usersResult = new Array;
  $scope.selectedRole = null;
  $scope.roleMode = ['DENY_ALL_BUT', 'ALLOW_ALL_BUT'];
  $scope.permissions = ["execute", "delete", "update", "read", "create"];
  $scope.getListUsers = function () {
    $scope.functions = new Array;
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      verbose: false,
      text: selectAllUsers,
      limit: $scope.limit,
      shallow: false
    }, function (data) {
      if (data.result) {
        $scope.usersResult = data.result;
        $scope.selectRole(data.result[0])
      }
    });
  }

  $scope.deleteRole = function (role) {

    Utilities.confirm($scope, $modal, $q, {
      title: 'Warning!',
      body: 'You are deleting role ' + role.name + '. Are you sure?',
      success: function () {
        DocumentApi.deleteDocument($routeParams.database, role["@rid"]).then(function () {
          var idx = $scope.usersResult.indexOf(role);
          $scope.usersResult.splice(idx, 1);
          if ($scope.selectedRole == role) {
            $scope.selectRole($scope.usersResult.result[0])
          }
          Notification.push({content: 'Role ' + role.name + ' has been deleted.'});
        }, function err() {

        })
      }
    });

  }
  $scope.getListUsers();
  $scope.selectRole = function (selectedRole) {
    $scope.selectedRole = selectedRole;
    if (!selectedRole['rules']) {
      selectedRole['rules'] = {};
    }

    if ((selectedRole['rules'] instanceof Array)) {
      $scope.disabledSecurity = true;
    } else {
      $scope.rules = Object.keys(selectedRole['rules']).sort();

    }
  }

  $scope.dropRule = function (rule) {
    delete $scope.selectedRole['rules'][rule];
  }
  $scope.changeMode = function (res) {
    var query = 'update {{rid}} set mode = {{mode}}';

    query = S(query).template({rid: res['@rid'], mode: res.mode}).s;

    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      verbose: false,
      text: query,
      limit: $scope.limit,
      shallow: false
    }, function (data) {

      Notification.push({
        content: S("Mode of '{{role}}' changed in '{{mode}}'").template({
          role: res.name,
          mode: $scope.roleMode[res.mode]
        }).s
      });

    });
  }
  $scope.addRule = function () {
    var modalScope = $scope.$new(true);
    var modalPromise = $modal({template: 'views/database/users/newRule.html', scope: modalScope, show: false});
    modalScope.save = function () {
      if (!$scope.selectedRole['rules'][modalPromise.$scope.name]) {


        var params = {
          resource: modalPromise.$scope.name,
          role: $scope.selectedRole.name
        }

        var sql = "REVOKE ALL ON {{resource}} FROM {{role}}"
        var query = S(sql).template(params).s
        CommandApi.queryText({
          database: $routeParams.database,
          language: 'sql',
          verbose: false,
          text: query,
          limit: $scope.limit,
          shallow: false
        }, function (data) {
          $scope.selectedRole['rules'][modalPromise.$scope.name] = 0;
          $scope.rules = Object.keys($scope.selectedRole['rules']).sort();
          Notification.push({content: S("Resource '{{resource}}' added correctly to '{{role}}'").template(params).s});
        })

      }
    }
    modalPromise.$promise.then(modalPromise.show);
  }
  $scope.addRole = function () {
    var modalScope = $scope.$new(true);
    modalScope.user = DocumentApi.createNewDoc("ORole");
    modalScope.roles = $scope.usersResult;
    modalScope.select2Options = $scope.select2Options;
    var modalPromise = $modal({template: 'views/database/users/newRole.html', scope: modalScope, show: false});
    modalScope.save = function () {

      DocumentApi.createDocument($scope.database.getName(), modalPromise.$scope.user["@rid"], modalPromise.$scope.user).then(function (data) {
        $scope.usersResult.push(data);
        Notification.push({content: 'Role ' + data.name + ' has been created.'});
      }, function error(err) {
        Notification.push({content: err, error: true});
      })
    }
    modalPromise.$promise.then(modalPromise.show);
  }
  $scope.changeRules = function (resource, role, idx, val, matrix) {


    matrix[idx] = !matrix[idx];

    var val = $scope.calculateValue(matrix);
    var params = {
      ops: matrix[idx] ? "GRANT" : "REVOKE",
      direction: matrix[idx] ? "TO" : "FROM",
      permission: $scope.permissions[idx],
      resource: resource,
      role: role
    }
    var sql = "{{ops}} {{permission}} ON {{resource}} {{direction}} {{role}}"
    var query = S(sql).template(params).s
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      verbose: false,
      text: query,
      limit: $scope.limit,
      shallow: false
    }, function (data) {

      $scope.selectedRole['rules'][resource] = val;
      switch (params.ops) {
        case "GRANT":
          Notification.push({content: S("Permission of '{{permission}}' granted on resource '{{resource}}' to '{{role}}'").template(params).s});
          break;
        case "REVOKE":
          Notification.push({content: S("Permission of '{{permission}}' revoked on resource '{{resource}}' from '{{role}}'").template(params).s});
          break;
      }
    });
  }
  $scope.calcolaBitmask = function (item) {


    var DecToBin = '';
    var Num1 = item % 2;

    while (item != 0) {
      DecToBin = Num1.toString().concat(DecToBin);
      item = Math.floor(item / 2);
      Num1 = item % 2;
    }
    var synch = 5 - DecToBin.length;
    for (var i = 0; i < synch; i++) {
      DecToBin = '0'.concat(DecToBin);
    }
    var matrix = new Array;
    for (z in DecToBin) {
      if (z != 'contains')
        matrix.push(DecToBin[z] == '1')
    }
    return matrix;

  }
  $scope.calculateValue = function (matrix) {
    var binToDec = ''
    matrix.forEach(function (e) {
      var bit = e ? "1" : "0";
      binToDec = binToDec.concat(bit);
    });
    return parseInt(binToDec, 2);
  }
}]);
