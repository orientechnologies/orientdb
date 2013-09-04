var schemaModule = angular.module('function.controller', ['database.services']);
schemaModule.controller("FunctionController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'FunctionApi', '$dialog', '$modal', '$q', function ($scope, $routeParams, $location, Database, CommandApi, FunctionApi, $dialog, $modal, $q) {

    $scope.database = Database;
    $scope.listClasses = $scope.database.listClasses();
    $scope.functions = new Array;

    $scope.languages = ['SQL', 'JavaScript'];
    var sqlText = 'select * from oFunction';

    CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sqlText, limit: $scope.limit}, function (data) {

        if (data.result) {
            for (i in data.result) {
                $scope.functions.push(data.result[i]['name']);
            }
        }
    });
    FunctionApi.executeFunction({database: $routeParams.database, functionName: 'prova', text: sqlText, limit: $scope.limit}, function (data) {

        if (data.result) {
            console.log(data.result);
        }
    });

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

    $scope.executeFunction = function () {
        console.log('aaaa');
    }

}
])
;
