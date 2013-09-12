var schemaModule = angular.module('function.controller', ['database.services']);
schemaModule.controller("FunctionController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'FunctionApi', 'DocumentApi', '$modal', '$q', '$route', function ($scope, $routeParams, $location, Database, CommandApi, FunctionApi, DocumentApi, $modal, $q, $route) {

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

    var sqlText = 'select * from oFunction order by name';


    $scope.getListFunction = function () {
        $scope.functions = new Array;
        CommandApi.queryText({database: $routeParams.database, language: 'sql', verbose: false, text: sqlText, limit: $scope.limit, shallow: true}, function (data) {
            if (data.result) {
                for (i in data.result) {
                    $scope.functions.push(data.result[i]);
                }
                if ($scope.functions.length > 0)
                    $scope.showInConsole($scope.functions[0]);
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

        $scope.inParams = $scope.functionToExecute['parameters'];
        $scope.$watch('inParams.length', function (data) {
            console.log(data);
            if (data) {
                $scope.parametersToExecute = new Array(data);
            }
            else {

                $scope.parametersToExecute = null;
            }
        });

        $scope.functionToExecute['parameters'].push('');
    }
    $scope.
        executeFunction = function () {


        if ($scope.functionToExecute != undefined) {
            var functionNamee = $scope.nameFunction;
            var buildedParams = '';
            for (i in $scope.parametersToExecute) {
                buildedParams = buildedParams.concat($scope.parametersToExecute[i] + '/');
            }
            console.log(buildedParams);

            FunctionApi.executeFunction({database: $routeParams.database, functionName: $scope.nameFunction, parameters: buildedParams, limit: $scope.limit}, function (data) {
                if (data.result) {
                    $scope.resultExecute = JSON.stringify(data.result);
                }
            });
        }
    }
    $scope.refreshPage = function () {

        $route.reload();
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
            console.log(data);
            if (data) {
                $scope.parametersToExecute = new Array(data);
            }
            else {

                $scope.parametersToExecute = null;
            }
        });


        $scope.isNewFunction = false;
    }

    $scope.modificataLang = function (lang) {
        console.log(lang);
        $scope.functionToExecute['language'] = lang;
    }
    $scope.createNewFunction = function () {


        var newDoc = DocumentApi.createNewDoc('ofunction');
        $scope.showInConsole(newDoc);
//        console.log(newDoc);
        $scope.isNewFunction = true;

    }
    $scope.saveFunction = function () {

        if ($scope.functionToExecute['language'] != undefined) {

            if ($scope.isNewFunction == true) {
                DocumentApi.createDocument($scope.database.getName(), $scope.functionToExecute['@rid'], $scope.functionToExecute, function (data) {
                        $scope.getListFunction();
                        $scope.refreshPage();

                    }
                );
            }
            else {
                DocumentApi.updateDocument($scope.database.getName(), $scope.functionToExecute['@rid'], $scope.functionToExecute, function (data) {
                    $scope.getListFunction();
                    $scope.refreshPage();
                });
            }
        }
        else {
            Utilities.confirm($scope, $modal, $q, {
                title: 'Warning!',
                body: 'Language can not be empty',
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

