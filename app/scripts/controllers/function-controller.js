var schemaModule = angular.module('function.controller', ['database.services']);
schemaModule.controller("FunctionController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'FunctionApi', 'DocumentApi', '$dialog', '$modal', '$q', function ($scope, $routeParams, $location, Database, CommandApi, FunctionApi, DocumentApi, $dialog, $modal, $q) {

    $scope.database = Database;
    $scope.listClasses = $scope.database.listClasses();
    $scope.functions = new Array;

    $scope.consoleValue = '';                           //code of the function
    $scope.nameFunction = '';                           //name of the function
    $scope.selectedLanguage = '';                       //language of the function
    $scope.languages = ['SQL', 'Javascript'];
    $scope.functionToExecute = undefined;

    $scope.resultExecute = undefined;

    $scope.parametersToExecute = new Array;
    $scope.parametersToExecute1 = {0: '', 1: ''};

    $scope.isNewFunction = false;


    var sqlText = 'select * from oFunction';


    $scope.getListFunction = function () {
        $scope.functions = new Array;
        CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sqlText, limit: $scope.limit, shallow: true}, function (data) {
            if (data.result) {
                for (i in data.result) {
                    $scope.functions.push(data.result[i]);
                }
            }
        });

    }
    $scope.clearConsole = function () {
        $scope.functionToExecute['code'] = '';
    }
    $scope.getListFunction();
    $scope.editorOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: false,
        mode: 'text/x-sql',
        extraKeys: {
            "Ctrl-Enter": function (instance) {
                $scope.executeFunction();

            }
        }
    };

    $scope.removeParam = function (index) {
        console.log('aaa')
        if ($scope.functionToExecute != undefined) {
            var numPar = parseInt($scope.functionToExecute['parameters']);

            var result = numPar - 1

            $scope.functionToExecute['parameters'].splice(index, 1);

        }
        return result;
    }
    $scope.addParam = function () {
        if ($scope.functionToExecute['parameters'] == undefined) {
            $scope.functionToExecute['parameters'] = new Array;
        }
        $scope.functionToExecute['parameters'].push('');
    }
    $scope.executeFunction = function () {


        if ($scope.functionToExecute != undefined) {
//            console.log($scope.parametersToExecute);
            var functionNamee = $scope.nameFunction;
            var buildedParams = '';
            for (i in $scope.parametersToExecute) {
                if (i == $scope.parametersToExecute.length - 1)
                    buildedParams = buildedParams.concat($scope.parametersToExecute[i])
                else
                    buildedParams = buildedParams.concat($scope.parametersToExecute[i] + '/');
            }
            console.log(buildedParams);
//
//
// console.log($scope.parametersToExecute);

            FunctionApi.executeFunction({database: $routeParams.database, functionName: $scope.nameFunction, parameters: buildedParams, limit: $scope.limit}, function (data) {
                if (data.result) {
//                    console.log(data.result);
                    $scope.resultExecute = JSON.stringify(data.result);
                }
            });
        }
    }
    $scope.calculateNumParameters = function () {
        if ($scope.functionToExecute != undefined) {
            console.log($scope.functionToExecute['parameters']);
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
    $scope.showInConsole = function (selectedFunction) {
        $scope.consoleValue = selectedFunction['code'];
        $scope.nameFunction = selectedFunction['name'];
        $scope.selectedLanguage = selectedFunction['language'];
        $scope.functionToExecute = selectedFunction;
        $scope.inParams = $scope.functionToExecute['parameters'];
        $scope.parametersToExecute = new Array;

        $scope.$watch('inParams.length', function (data) {
            $scope.parametersToExecute = new Array(data);
        });
        console.log(selectedFunction['idempotent']);


        $scope.isNewFunction = false;
    }

    $scope.modificataLang = function (lang) {
        console.log(lang);
        $scope.functionToExecute['language'] = lang;
    }
    $scope.createNewFunction = function () {


        var newDoc = DocumentApi.createNewDoc('ofunction');
        $scope.showInConsole(newDoc);
        console.log(newDoc);
        $scope.isNewFunction = true;

    }
    $scope.saveFunction = function () {


        if ($scope.isNewFunction == true) {
            DocumentApi.createDocument($scope.database.getName(), $scope.functionToExecute['@rid'], $scope.functionToExecute, function (data) {

                }
            );
        }
        else {
            DocumentApi.updateDocument($scope.database.getName(), $scope.functionToExecute['@rid'], $scope.functionToExecute, function (data) {

            });
        }
        $scope.getListFunction();
    }

    $scope.deleteFunction = function () {

        var recordID = $scope.functionToExecute['@rid'];
        var clazz = $scope.functionToExecute['@class'];

        Utilities.confirm($scope, $dialog, {
            title: 'Warning!',
            body: 'You are removing ' + $scope.functionToExecute['name'] + '. Are you sure?',
            success: function () {
                DocumentApi.deleteDocument($scope.database.getName(), recordID, function (data) {

                    $scope.getListFunction();
                });
            }
//        $scope.getListFunction();

        });

    }
}]);

