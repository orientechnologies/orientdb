/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 04/09/13
 * Time: 17.13
 * To change this template use File | Settings | File Templates.
 */

import '../views/database/configuration.html';
import  '../views/database/config/structure.html';
import  '../views/database/config/boolenaCustom.html';
import  '../views/database/config/clusterSelection.html';
import  '../views/database/config/conflictStrategy.html';
import  '../views/database/config/configuration.html';
import  '../views/database/config/import-export.html';
import  '../views/database/config/default.html';

let configModule = angular.module('configuration.controller', []);
configModule.controller("ConfigurationController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'Database', function ($scope, $routeParams, $location, DatabaseApi, Database) {

  $scope.database = Database;
  $scope.active = $routeParams.tab || "structure";
  $scope.db = $routeParams.database;
  $scope.tabs = ['structure', 'configuration', 'import-export'];

  $scope.tabsI18n = new Array;

  if ($scope.active == "structure") {
    Database.setWiki("Clusters.html");
  }

  else if ($scope.active == "configuration") {
    Database.setWiki("Configuration.html");
  } else if ($scope.active == "import-export") {
    Database.setWiki("Export-Import.html");
  }
  $scope.tabsI18n['structure'] = 'Structure';
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

    return "http://yuml.me/diagram/scruffy/class/" + umlURL;

  }
  $scope.refreshUML = function () {
    $scope.uml = $scope.displayUML();
  }

}]);

configModule.controller("StructureController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'Database', 'ClusterAlterApi', "Notification", function ($scope, $routeParams, $location, DatabaseApi, Database, ClusterAlterApi, Notification) {

  $scope.clusters = Database.getMetadata()['clusters'];
  $scope.conflictStrategies = ['version', 'content', 'automerge']
  $scope.dataSegments = Database.getMetadata()['dataSegments'];
  $scope.txSegments = Database.getMetadata()['txSegment'];

  $scope.links = {
    linkConflictStrategy: Database.getOWikiFor("SQL-Alter-Cluster.html")
  }
  $scope.version = Database.getVersion();

  $scope.changeStrategy = function (cluster) {

    ClusterAlterApi.changeProperty(Database.getName(), {
      cluster: cluster.name,
      name: "conflictStrategy",
      value: cluster.conflictStrategy
    }).then(function () {
      Notification.push({content: "Conflict strategy for cluster '" + cluster.name + "' changed in '" + cluster.conflictStrategy + "'."});
    });
  }
}]);
configModule.controller("DbConfigController", ['$scope', '$routeParams', '$location', 'DatabaseApi', 'Database', 'DatabaseAlterApi', 'Notification', '$q', function ($scope, $routeParams, $location, DatabaseApi, Database, DatabaseAlterApi, Notification, $q) {


  $scope.values = Database.getMetadata()['config']['values'];
  $scope.properties = Database.getMetadata()['config']['properties'];

  $scope.links = {
    useLightweightEdges: Database.getOWikiFor("Tutorial-Working-with-graphs.html#lightweight-edges"),
    clusterSelection: Database.getOWikiFor("SQL-Alter-Database.html"),
    minimumClusters: Database.getOWikiFor("SQL-Alter-Database.html"),
    conflictStrategy: Database.getOWikiFor("SQL-Alter-Database.html")
  }


  var found = false;
  var foundStrictSql = false;
  $scope.properties.forEach(function (val) {
    if (val.name == 'useLightweightEdges') {
      found = true;
    }
    if (val.name == 'strictSql') {
      foundStrictSql = true;
    }
  });
  if (!found) {
    $scope.properties.push({name: 'useLightweightEdges', value: 'false'});
  }
  if (!foundStrictSql) {
    $scope.properties.push({name: 'strictSql', value: 'false'});
  }


  $scope.canChange = ["clusterSelection", "minimumClusters", "localeCountry", "useLightweightEdges", "strictSql", "conflictStrategy"];
  $scope.changeTemplate = {
    clusterSelection: "views/database/config/clusterSelection.html",
    strictSql: "views/database/config/boolenaCustom.html",
    useLightweightEdges: "views/database/config/boolenaCustom.html",
    conflictStrategy: "views/database/config/conflictStrategy.html"
  }
  $scope.dirty = [];
  $scope.customDirty = [];
  $scope.clusterStrategies = ['round-robin', "default", "balanced", "local"];

  $scope.conflictStrategies = ['version', 'content', 'automerge']
  $scope.isDisabledVal = function (val) {
    return $scope.canChange.indexOf(val.name) == -1
  }

  $scope.setDirty = function (val) {
    if ($scope.dirty.indexOf(val) == -1)
      $scope.dirty.push(val);
  }
  $scope.setCustomDirty = function (val) {
    if ($scope.customDirty.indexOf(val) == -1)
      $scope.customDirty.push(val);
  }
  $scope.getRender = function (val) {
    var tpl = $scope.changeTemplate[val.name];
    return tpl ? tpl : "views/database/config/default.html";
  }
  $scope.getCustomRender = function (val) {
    var tpl = $scope.changeTemplate[val.name];
    return tpl ? tpl : "views/database/config/defaultCustom.html";
  }
  $scope.save = function () {
    var promises = []
    $scope.dirty.forEach(function (val) {

      var prop = angular.copy(val);
      if (prop.name === 'clusterSelection') {
        prop.value = '`' + prop.value + '`';
      }
      var p = DatabaseAlterApi.changeProperty(Database.getName(), prop);
      promises.push(p);
    });
    $scope.customDirty.forEach(function (val) {
      var p = DatabaseAlterApi.changeCustomProperty(Database.getName(), val);
      promises.push(p);
    });
    $q.all(promises).then(function () {
      Notification.push({content: "Configuration Saved."});
    }, function (err) {
      Notification.push({content: err, error: true});
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
      $scope.data.push({value: value, color: color});

    });


  });


}]);


export default configModule.name;
