/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 04/09/13
 * Time: 17.13
 * To change this template use File | Settings | File Templates.
 */

var configModule = angular.module('configuration.controller', []);
configModule.controller("ConfigurationController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'Database', function ($scope, $routeParams, $location, DatabaseApi, Database) {

    $scope.active = $routeParams.tab || "structure";
    $scope.db = $routeParams.database;
    $scope.tabs = ['structure', 'configuration', 'import-export', 'uml'];

    $scope.tabsI18n = new Array;

    if ($scope.active == "allocation") {
        Database.setWiki("https://github.com/orientechnologies/orientdb-studio/wiki/Defragmentation");
    }
    else {
        Database.setWiki("https://github.com/orientechnologies/orientdb-studio/wiki/Uml");
    }
    $scope.tabsI18n['structure'] = 'Structure';
//    $scope.tabsI18n['allocation'] = 'Defragmentation';
    $scope.tabsI18n['configuration'] = 'Configuration';
    $scope.tabsI18n['import-export'] = 'Export';
    $scope.tabsI18n['uml'] = 'UML Class Diagram';


    $scope.getTemplate = function (tab) {
        return 'views/database/config/' + tab + '.html';
    }
    $scope.exportDatabase = function () {
        DatabaseApi.exportDatabase($scope.db);
    }

    $scope.handleFile = function (files) {

        var reader = new FileReader();
        reader.onload = function (event) {
            object = {};
            object.filename = files[0].name;
            object.data = event.target.result;
            var blobInput = [event.target.result];
            var blob = new Blob(blobInput);
            DatabaseApi.importDatabase($scope.db, blob, files[0]);
        }
        reader.readAsDataURL(files[0]);
    }


}]);

configModule.controller("UMLController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'Database', function ($scope, $routeParams, $location, DatabaseApi, Database) {


    $scope.umlTypes = ['svg', 'png'];
    $scope.umlType = '.svg';
    $scope.classLimit = 20;
    $scope.displayConnected = true;
    $scope.displayAttribute = true;

    $scope.$watch("umlType", function (data) {
        $scope.uml = $scope.displayUML();
    });
    $scope.$watch("classLimit", function (data) {
        $scope.uml = $scope.displayUML();
    });
    $scope.$watch("displayConnected", function (data) {
        $scope.uml = $scope.displayUML();
    });
    $scope.$watch("displayAttribute", function (data) {
        $scope.uml = $scope.displayUML();
    });

    $scope.displayUML = function () {
        var umlDisplayAttributes = $scope.displayAttribute;
        var umlDisplayOnlyConnected = $scope.displayConnected;
        var umlLimitClasses = $scope.classLimit;
        var umlDisplayFormat = $scope.umlType;

        var umlURL = "";
        var databaseInfo = Database.getMetadata();
        var classIdx = 0;
        for (cls in databaseInfo['classes']) {
            var clazz = databaseInfo['classes'][cls];

            if (umlDisplayOnlyConnected) {
                if (clazz['properties'] == null || clazz['properties'].length == 0)
                // SKIPT IT
                    continue;

                var linked = false;
                for (p in clazz['properties']) {
                    if (clazz['properties'][p].type.indexOf('LINK') == 0 ||
                        clazz['properties'][p].type.indexOf('EMBEDDED') == 0) {
                        linked = true;
                        break;
                    }
                }
                if (!linked)
                // SKIPT IT
                    continue;
            }

            if (classIdx++ > umlLimitClasses)
                break;

            var clsName = clazz.name;

            if (umlURL.length > 0)
                umlURL += ", ";

            umlURL += "[" + clsName;
            var links = "";

            var propIdx = 0;
            for (p in clazz['properties']) {
                if (clazz['properties'][p].type.indexOf('LINK') == 0) {
                    links += ", [" + clsName + "]-" + clazz['properties'][p].name;

                    if (clazz['properties'][p].type == 'LINK')
                        links += "1>[" + clazz['properties'][p].linkedClass;
                    else
                        links += "*>[" + clazz['properties'][p].linkedClass;
                    links += "]";

                } else if (clazz['properties'][p].type.indexOf('EMBEDDED') == 0) {
                    links += ", [" + clsName + "]++-" + clazz['properties'][p].name;

                    if (clazz['properties'][p].type == 'EMBEDDED')
                        links += "1>[" + clazz['properties'][p].linkedClass;
                    else
                        links += "*>[" + clazz['properties'][p].linkedClass;
                    links += "]";

                } else if (umlDisplayAttributes) {
                    if (propIdx++ == 0)
                        umlURL += "|";
                    else
                        umlURL += ";";
                    umlURL += "+" + clazz['properties'][p].name;
                }
            }

            umlURL += "]";

            if (clazz.superClass != "") {
                links += ", [" + clazz.superClass + "]^-[" + clsName + "]";
            }

            umlURL += links;
        }

        umlURL += umlDisplayFormat;

        return  "http://yuml.me/diagram/scruffy/class/" + umlURL;

    }
    $scope.refreshUML = function () {
        $scope.uml = $scope.displayUML();
    }

}]);

configModule.controller("StructureController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'Database', function ($scope, $routeParams, $location, DatabaseApi, Database) {

    $scope.clusters = Database.getMetadata()['clusters'];
    $scope.dataSegments = Database.getMetadata()['dataSegments'];
    $scope.txSegments = Database.getMetadata()['txSegment'];


}]);
configModule.controller("DbConfigController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'Database', 'DatabaseAlterApi', 'Notification', function ($scope, $routeParams, $location, DatabaseApi, Database, DatabaseAlterApi, Notification) {


    $scope.values = Database.getMetadata()['config']['values'];

    $scope.canChange = ["clusterSelection", "minimumClusters"];
    $scope.changeTemplate = { clusterSelection: "views/database/config/clusterSelection.html"}
    $scope.dirty = [];
    $scope.clusterStrategies = ['round-robin', "default", "balanced"];

    $scope.isDisabledVal = function (val) {
        return $scope.canChange.indexOf(val.name) == -1
    }

    $scope.setDirty = function (val) {
        if ($scope.dirty.indexOf(val) == -1)
            $scope.dirty.push(val);
    }
    $scope.getRender = function (val) {
        var tpl = $scope.changeTemplate[val.name];
        return tpl ? tpl : "views/database/config/default.html";
    }
    $scope.save = function () {
        $scope.dirty.forEach(function (val) {
            DatabaseAlterApi.changeProperty(Database.getName(), val).then(function (data) {
                Notification.push({content: "Configuration Saved."});
            });
        });
    }

}]);
configModule.controller("AllocationController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'Database', function ($scope, $routeParams, $location, DatabaseApi, Database) {

    $scope.data = [
        {
            value: 30,
            color: "#F38630"
        },
        {
            value: 50,
            color: "#E0E4CC"
        },
        {
            value: 100,
            color: "#69D2E7"
        }
    ];
    $scope.db = $routeParams.database;
    DatabaseApi.getAllocation($scope.db, function (datas) {

        var size = datas.size;
        $scope.dataSize = datas.dataSize;
        $scope.dataSizePercent = datas.dataSizePercent;
        $scope.holesSize = datas.holesSize;
        $scope.holesSizePercent = datas.holesSizePercent;
        $scope.data.length = 0;
        var lastSize = 0;
        datas['segments'].forEach(function (val, idx, arr) {

            lastSize += val.size;

        });
        var percSum = 0;
        datas['segments'].forEach(function (val, idx, arr) {
            var color = val.type == 'd' ? '#F38630' : '#E0E4CC';
            var value = (val.size * 200) / lastSize;
            value = Math.round(value);
            value = value == 0 ? 1 : value;
            percSum += value;
            $scope.data.push({ value: value, color: color});

        });


    });


}]);