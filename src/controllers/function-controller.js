import '../views/database/functions.html';
import '../views/database/function/functionAside.html';

import Utilities from '../util/library';
import angular from 'angular';

let functionModule = angular.module('function.controller', ['database.services']);
functionModule.controller("FunctionController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'FunctionApi', 'DocumentApi', '$modal', '$q', '$route', 'Spinner', 'Notification', 'Aside', '$rootScope', 'DatabaseApi', function ($scope, $routeParams, $location, Database, CommandApi, FunctionApi, DocumentApi, $modal, $q, $route, Spinner, Notification, Aside, $rootScope, DatabaseApi) {

  $scope.database = Database;
  $scope.listClass = 'fa-mail-reply';
  $scope.logLevel = ""
  $scope.isCode = true;
  $scope.isDirty = false;
  $scope.listClasses = $scope.database.listClasses();
  $scope.editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    readOnly: false,
    mode: 'javascript',
    extraKeys: {
      "Ctrl-Enter": function (instance) {
        $scope.executeFunction();

      }
    },
    onLoad: function (_cm) {
      $scope.vcm = _cm;
      //$scope.createNewFunction();
    }
  };

  $scope.viewerOptions = {
    lineWrapping: true,
    lineNumbers: true,
    readOnly: true,
    mode: 'javascript',
    onLoad: function (_cm) {
      $scope.vcm = _cm;
    }

  };
  Database.setWiki("Functions.html");
  $scope.functions = new Array;

  $scope.consoleValue = '';                           //code of the function
  $scope.nameFunction = '';                           //name of the function
  $scope.selectedLanguage = '';                       //language of the function
  $scope.languages = ['SQL', 'Javascript'];
  $scope.functionToExecute = undefined;

  DatabaseApi.getAvailableLanguages($routeParams.database).then(function (data) {
    $scope.languages = data.languages;
  })
  $scope.resultExecute = undefined;
  $scope.limit = -1;
  $scope.parametersToExecute = new Array;

  $scope.isNewFunction = false;

  var sqlText = 'select * from oFunction order by name';


  $scope.getListFunction = function () {
    $scope.functions = new Array;
    $scope.functionsrid = new Array;
    var deferred = $q.defer();
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      verbose: false,
      text: sqlText,
      limit: $scope.limit,
      shallow: false
    }, function (data) {
      if (data.result) {
        for (let i in data.result) {
          $scope.functions.push(data.result[i]);
          $scope.functionsrid.push(data.result[i]['name'])
        }


        if ($scope.functions.length > 0) {
          if ($scope.functionToExecute == undefined) {

            $scope.functionToExecute = $scope.functions[0];
          }
          var index = $scope.functionsrid.indexOf($scope.functionToExecute['name']);
          if (index != -1)
            $scope.showInConsole($scope.functions[index]);
        } else {
          $scope.createNewFunction();
        }
        deferred.resolve();
      }

    });
    return deferred.promise;

  }
  $scope.clearConsole = function () {
    $scope.functionToExecute['code'] = '';
  }
  $scope.getListFunction().then(function () {
    Aside.show({
      scope: $scope,
      title: "Functions",
      template: 'views/database/function/functionAside.html',
      show: true,
      absolute: false
    });
  });

  $scope.removeParam = function (index) {

    if ($scope.functionToExecute != undefined) {
      var numPar = parseInt($scope.functionToExecute['parameters']);
      var result = numPar - 1;
      $scope.functionToExecute['parameters'].splice(index, 1);
      $scope.parametersToExecute.splice(index, 1);
    }
    return result;
  }
  $scope.showAllFunctions = function () {

    Aside.toggle();

  }
  $rootScope.$on("aside:close", function () {
    $scope.listClass = 'fa-mail-forward';
  })
  $rootScope.$on("aside:open", function () {
    $scope.listClass = 'fa-mail-reply';
  })

  $scope.canSave = function (form) {

    return !$scope.isDirty || form.$invalid;
  }
  $scope.$watch("functionToExecute.code", function (newVal, oldVal) {
    if (oldVal && oldVal != newVal) {
      $scope.isDirty = true;
    }
  })
  $scope.$watch("functionToExecute.name", function (newVal, oldVal) {
    if (oldVal && oldVal != newVal) {
      $scope.isDirty = true;
    }
  })
  $scope.$watchCollection("functionToExecute.parameters", function (newVal, oldVal) {
    if (oldVal && oldVal != newVal) {
      $scope.isDirty = true;
    }
  })
  $scope.copyFunction = function () {
    if ($scope.functionToExecute != undefined) {

      var newFunc = JSON.parse(JSON.stringify($scope.functionToExecute));
      newFunc['name'] = $scope.functionToExecute['name'] + "_clone";
      newFunc['code'] = newFunc['code'] + ' '
      newFunc['$$hashKey'] = '';

      $scope.functions.push(newFunc);
      $scope.showInConsole(newFunc);
      $scope.isNewFunction = true;
    }
  }
  $scope.addParam = function () {


    if ($scope.functionToExecute['parameters'] == undefined) {
      $scope.functionToExecute['parameters'] = new Array;
    }
    $scope.functionToExecute['parameters'].push('');
    $scope.parametersToExecute.push('');
  }
  $scope.executeFunction = function () {
    $scope.resultExecute = '';

    if ($scope.functionToExecute != undefined) {
      var encodedParams = new Array();
      $scope.parametersToExecute.forEach(function (param) {
        encodedParams.push(encodeURIComponent(param));
      });
      var buildedParams = encodedParams.join('/');
      Spinner.start();
      FunctionApi.executeFunction({
        database: $routeParams.database,
        functionName: $scope.nameFunction,
        parameters: buildedParams,
        limit: $scope.limit
      }, function (data) {
        if (data.result) {
          $scope.resultExecute = JSON.stringify(data.result);
          Spinner.stopSpinner();

        }
        $scope.logLevel = "function-success-log";
        Spinner.stopSpinner();
      }, function (error) {
        if (typeof error == 'string') {
          $scope.resultExecute = JSON.stringify({error: error});
        } else {
          if (error)
            $scope.resultExecute = JSON.stringify(error);
        }
        $scope.logLevel = "function-error-log";
        Spinner.stopSpinner();
      });
    }
  }
  $scope.refreshPage = function () {

    $route.reload();
  }

  $scope.calculateNumParameters = function () {
    if ($scope.functionToExecute != undefined) {
      var numPar = parseInt($scope.functionToExecute['parameters']);
      var i = 0;
      var result = new Array;
      for (i = 0; i < numPar; i++) {

        result.push(numPar[i]);
      }
    }
    return result;
  }

  //when click on a function in list of functions

  $scope.showInConsoleAfterSave = function (selectedFunction) {
    $scope.consoleValue = selectedFunction['code'];
    $scope.nameFunction = selectedFunction['name'];
    $scope.selectedLanguage = selectedFunction['language'] ? selectedFunction['language'].toLowerCase() : selectedFunction['language'];
    $scope.functionToExecute = selectedFunction;

  }

  $scope.selectFunction = function (selected) {
    $scope.resultExecute = '';
    $scope.logLevel = '';
    $scope.showInConsole(selected);
  }
  $scope.showInConsole = function (selectedFunction) {

    $scope.showInConsoleAfterSave(selectedFunction);
    var size = 0;
    if ($scope.functionToExecute['parameters']) {
      size = $scope.functionToExecute['parameters'].length;
    }
    $scope.parametersToExecute = new Array(size);


    $scope.isNewFunction = false;
  }

  $scope.modifiedLanguage = function (lang) {
    $scope.functionToExecute['language'] = lang;
  }
  $scope.createNewFunction = function () {

    var newDoc = DocumentApi.createNewDoc('ofunction');
    $scope.showInConsole(newDoc);
    $scope.isNewFunction = true;

  }
  $scope.saveFunction = function () {
    $scope.resultExecute = '';
    $scope.logLevel = '';
    if ($scope.functionToExecute['language'] != undefined && $scope.functionToExecute['name'] != undefined && $scope.functionToExecute['name'] != '') {
      if ($scope.isNewFunction == true) {

//                if ($scope.functionToExecute['language'] == 'javascript') {
//                    $scope.functionToExecute['code'] = $scope.functionToExecute['code'].replace(/\//g, "\\/");
//                }
        DocumentApi.createDocument($scope.database.getName(), $scope.functionToExecute['@rid'], $scope.functionToExecute).then(function (data) {
          $scope.getListFunction();
          $scope.isNewFunction = false;
          var message = 'Function {{name}} saved successfully.';
          Notification.push({content: S(message).template({name: $scope.functionToExecute['name']}).s, autoHide: true});
        });

      }
      else {
//                if ($scope.functionToExecute['language'] == 'javascript') {
//                    $scope.functionToExecute['code'] = $scope.functionToExecute['code'].replace(/\//g, "\\/");
//                }
        DocumentApi.updateDocument($scope.database.getName(), $scope.functionToExecute['@rid'], $scope.functionToExecute).then(function (data) {
          var oldParams = JSON.stringify($scope.parametersToExecute);
          $scope.getListFunction().then(function () {
            oldParams = JSON.parse(oldParams);
            $scope.parametersToExecute = oldParams;
          });
          var message = 'Function {{name}} saved successfully.';
          Notification.push({content: S(message).template({name: $scope.functionToExecute['name']}).s, autoHide: true});
        });
      }
    }
    else {
      Utilities.confirm($scope, $modal, $q, {
        title: 'Warning!',
        body: 'Name and Language can not be empty',
        success: function () {

        }

      });
    }

  }

  $scope.deleteFunction = function () {

    var recordID = $scope.functionToExecute['@rid'];
    var clazz = $scope.functionToExecute['@class'];

    Utilities.confirm($scope, $modal, $q, {
      title: 'Warning!',
      body: 'You are removing ' + $scope.functionToExecute['name'] + '. Are you sure?',
      success: function () {
        DocumentApi.deleteDocument($scope.database.getName(), recordID, function (data) {

          $scope.getListFunction();
        });
      }

    });

  }
}]);

export default functionModule.name;

