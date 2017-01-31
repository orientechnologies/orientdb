import '../views/database/editclass.html';
import '../views/database/newProperty.html';
import '../views/database/newIndex.html';
import '../views/database/newClass.html';
import '../views/database/newVertex.html';
import '../views/database/newEdge.html';
import '../views/database/newGenericClass.html';
import '../views/database/index/indexMain.html';


import Utilities from '../util/library';
import angular from 'angular';

let schemaModule = angular.module('schema.controller', ['database.services']);
schemaModule.controller("SchemaController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'ClassAlterApi', '$modal', '$q', '$route', '$window', 'Spinner', 'Notification', '$popover', 'GraphConfig', 'DocumentApi', 'SchemaService', function ($scope, $routeParams, $location, Database, CommandApi, ClassAlterApi, $modal, $q, $route, $window, Spinner, Notification, $popover, GraphConfig, DocumentApi, SchemaService) {

  //for pagination
  $scope.countPage = 10;
  $scope.countPageOptions = [10, 20, 50, 100];
  $scope.currentPage = 1;
  $scope.links = {
    linkClasses: Database.getOWikiFor("Schema.html#class"),
    linkClusterSelection: Database.getOWikiFor("Cluster-Selection.html"),
    linkClusters: Database.getOWikiFor("Tutorial-Clusters.html"),
    linkInheritance: Database.getOWikiFor("Inheritance.html")
  }
  $scope.popover = {title: "Rename Class"};
  $scope.clusterStrategies = ['round-robin', "default", "balanced", ",local"];
  $scope.database = Database;
  $scope.database.refreshMetadata($routeParams.database);
  $scope.database = Database;
  $scope.listClassesTotal = $scope.database.listClasses();


  let isSystem = function (c) {
    return SchemaService.isSystemClass(c);
  }


  let gMap = {};
  $scope.systemClasses = $scope.listClassesTotal.filter((c) => {

    let system = isSystem(c.name);
    if (system) {
      gMap[c.name] = true;
    }
    return system;
  })
  $scope.vClasses = $scope.listClassesTotal.filter((c) => {
    let vertex = Database.isVertex(c.name);
    if (vertex) {
      gMap[c.name] = true;
    }
    return vertex;
  })
  $scope.eClasses = $scope.listClassesTotal.filter((c) => {
    let edge = Database.isEdge(c.name);
    if (edge) {
      gMap[c.name] = true;
    }
    return edge;
  })

  $scope.gClasses = $scope.listClassesTotal.filter((c) => {
    return !gMap[c.name];
  })


  $scope.numberOfPage = new Array(Math.ceil($scope.listClassesTotal.length / $scope.countPage));
  $scope.listClasses = $scope.listClassesTotal.slice(0, $scope.countPage);

  $scope.colors = d3.scale.category20();

  $scope.tab = 'user';

  GraphConfig.get().then(function (data) {
    $scope.config = data;
    if (!$scope.config) {
      $scope.config = DocumentApi.createNewDoc(GraphConfig.CLAZZ);
      $scope.config.config = {
        classes: {}
      }
    }
    if (!$scope.config.config) {
      $scope.config.config = {
        classes: {}
      }
    }
    $scope.listClassesTotal.forEach(function (c) {
      if (!$scope.config.config.classes[c.name]) {
        $scope.config.config.classes[c.name] = {}
        $scope.config.config.classes[c.name].fill = d3.rgb($scope.colors(c.name.toString(2))).toString();
        $scope.config.config.classes[c.name].stroke = d3.rgb($scope.colors(c.name.toString(2))).darker().toString();
      }
    })
  })
  $scope.headers = ['name', 'superClass', 'alias', 'abstract', 'clusters', 'defaultCluster', 'clusterSelection', 'records'];
  $scope.refreshPage = function () {
    $scope.database.refreshMetadata($routeParams.database);
    $route.reload();
  }
  $scope.setClass = function (clazz) {
    $scope.classClicked = clazz;
  }
  $scope.openClass = function (clazz) {
    $location.path("/database/" + $scope.database.getName() + "/schema/editclass/" + clazz.name);
  }
  $scope.refreshWindow = function () {
    $route.reload();
  }
  Database.setWiki("Schema.html");

  $scope.$watch("countPage", function (data) {
    if ($scope.listClassesTotal) {
      $scope.listClasses = $scope.listClassesTotal.slice(0, $scope.countPage);
      $scope.currentPage = 1;
      $scope.numberOfPage = new Array(Math.ceil($scope.listClassesTotal.length / $scope.countPage));
    }
  });
  $scope.saveColorConfig = function () {

    GraphConfig.set($scope.config).then(function () {

      var noti = "Colors Configuration saved correctly.";
      Notification.push({content: noti});
    })
  }
  $scope.canDrop = function (clazz) {
    return clazz != "V" && clazz != "E";
  }
  $scope.rename = function (cls, event) {


    //modal
    var modalScope = $scope.$new(true);
    modalScope.what = 'class';
    modalScope.tmpName = cls.name;
    var modalPromise = $modal({templateUrl: 'views/database/changeNameModal.html', scope: modalScope, show: false});

    modalScope.rename = function (name) {
      if (name != cls.name) {
        ClassAlterApi.changeProperty($routeParams.database, {
          clazz: cls.name,
          name: "name",
          value: name
        }).then(function (data) {
          var noti = S("The class {{name}} has been renamed to {{newName}}").template({
            name: cls.name,
            newName: name
          }).s;
          Notification.push({content: noti});
          cls.name = name;
        }, function err(data) {
          Notification.push({content: data, error: true});
        });
      }
    }
    modalPromise.$promise.then(modalPromise.show);

  }
  $scope.dropClass = function (nameClass) {

    Utilities.confirm($scope, $modal, $q, {

      title: 'Warning!',
      body: 'You are dropping class ' + nameClass['name'] + '. Are you sure?',
      success: function () {
        var sql = 'DROP CLASS `' + nameClass['name'] + "`";

        CommandApi.queryText({
          database: $routeParams.database,
          language: 'sql',
          text: sql,
          limit: $scope.limit,
          verbose: false
        }, function (data) {

          var elem = $scope.listClassesTotal.indexOf(nameClass);
          $scope.listClassesTotal.splice(elem, 1)
          $scope.listClassesTotal.splice();
          Notification.push({content: "Class '" + nameClass['name'] + "' dropped."});
        });
      }
    });

  }
  $scope.switchPage = function (index) {
    if (index != $scope.currentPage) {
      $scope.currentPage = index;
      $scope.listClasses = $scope.listClassesTotal.slice(
        (index - 1) * $scope.countPage,
        index * $scope.countPage
      );
    }
  }
  $scope.queryAll = function (className) {
    $location.path("/database/" + $scope.database.getName() + "/browse/select * from `" + className + "`");
  }
  $scope.createRecord = function (className) {
    $location.path("/database/" + $scope.database.getName() + "/browse/create/" + className);
  }
  $scope.allIndexes = function () {
    $location.path("/database/" + $scope.database.getName() + "/schema/indexes");
  }
  $scope.createNewClass = function () {
    let modalScope = $scope.$new(true);
    modalScope.db = $scope.database;

    modalScope.parentScope = $scope;
    var modalPromise = $modal({template: 'views/database/newClass.html', scope: modalScope, show: false});
    modalPromise.$promise.then(modalPromise.show);

  }
  $scope.createNewVertex = function () {
    let modalScope = $scope.$new(true);
    modalScope.db = $scope.database;

    modalScope.parentScope = $scope;
    var modalPromise = $modal({templateUrl: 'views/database/newVertex.html', scope: modalScope, show: false});
    modalPromise.$promise.then(modalPromise.show);

  }
  $scope.createNewEdge = function () {
    let modalScope = $scope.$new(true);
    modalScope.db = $scope.database;

    modalScope.parentScope = $scope;
    var modalPromise = $modal({template: 'views/database/newEdge.html', scope: modalScope, show: false});
    modalPromise.$promise.then(modalPromise.show);

  }
  $scope.createNewGeneric = function () {
    let modalScope = $scope.$new(true);
    modalScope.db = $scope.database;

    modalScope.parentScope = $scope;
    var modalPromise = $modal({template: 'views/database/newGenericClass.html', scope: modalScope, show: false});
    modalPromise.$promise.then(modalPromise.show);

  }
  $scope.rebuildAllIndexes = function () {
    var sql = 'REBUILD INDEX *';
    Spinner.start();
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: sql,
      limit: $scope.limit
    }, function (data) {
      Spinner.stopSpinner();
      Notification.push({content: "All Indexes rebuilded."})
    }, function (err) {
      Spinner.stopSpinner();
    });
  }
  $scope.setClusterStrategy = function (clazz) {
    ClassAlterApi.changeProperty($routeParams.database, {
      clazz: clazz.name,
      name: "clusterSelection",
      value: clazz.clusterSelection
    }).then(function (data) {
      var noti = S("Cluster selection strategy for the class {{name}} has been changed to {{clusterSelection}}").template(clazz).s;
      Notification.push({content: noti});
    }).catch(function (e) {
      Notification.push({content: e, error: true});
    });
  };
}
])
;
schemaModule.controller("ClassEditController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', '$window', 'DatabaseApi', 'Spinner', 'PropertyAlterApi', 'Notification', 'SchemaService', 'FormatErrorPipe', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route, $window, DatabaseApi, Spinner, PropertyAlterApi, Notification, SchemaService, FormatErrorPipe) {
  Database.setWiki("Class.html");
  var clazz = $routeParams.clazz;

  $scope.links = {
    properties: Database.getOWikiFor("Schema.html#property"),
    indexes: Database.getOWikiFor("Indexes.html"),
    type: Database.getOWikiFor("Indexes.html#index-types"),
    engine: Database.getOWikiFor("Indexes.html")
  }

  $scope.class2show = clazz;
  $scope.database = Database;
  $scope.database.refreshMetadata($routeParams.database);
  $scope.modificati = new Array;
  $scope.limit = 20;
  $scope.queries = new Array;
  $scope.classClickedHeaders = ['Name', 'Type', 'Linked_Type', 'Linked_Class', 'Mandatory', 'Read_Only', 'Not_Null', 'Min', 'Max', 'Collate', 'Actions'];
  $scope.property = null;
  $scope.property = Database.listPropertiesForClass(clazz);

  $scope.strict = Database.isStrictSql();

  $scope.clonedProperty = angular.copy($scope.property);
  $scope.propertyNames = new Array;

  for (let inn in $scope.property) {
    $scope.propertyNames.push($scope.property[inn]['name'])
  }
  $scope.createNewRecord = function (className) {
    $location.path("/database/" + $scope.database.getName() + "/browse/create/" + className);
  }
  $scope.queryAll = function (className) {
    className = $scope.strict ? `\`${className}\`` : className;
    $location.path("/database/" + $scope.database.getName() + "/browse/select * from " + className + "");
  }
  $scope.canDrop = function (clazz) {

    return clazz != "V" && clazz != "E";
  }
  $scope.dropClass = function (nameClass) {

    Utilities.confirm($scope, $modal, $q, {

      title: 'Warning!',
      body: 'You are dropping class ' + nameClass + '. Are you sure?',
      success: function () {
        SchemaService.dropClass($routeParams.database, nameClass, $scope.strict)
          .then(() => {
            Notification.push({content: "Class '" + nameClass + "' dropped."});
            $location.path("/database/" + $scope.database.getName() + "/schema");
          }).catch((err) => {
          Notification.push({content: FormatErrorPipe.transform(err.json()), error: true});
        })
      }

    });

  }
  $scope.refreshPage = function () {
    $scope.database.refreshMetadata($routeParams.database);
  }

  $scope.listClasses = $scope.database.listNameOfClasses();


  $scope.indexes = null;
  $scope.indexes = Database.listIndexesForClass(clazz);

  $scope.rename = function (props) {


    //modal
    var modalScope = $scope.$new(true);
    modalScope.what = 'property';
    modalScope.tmpName = props.name;
    var modalPromise = $modal({template: 'views/database/changeNameModal.html', scope: modalScope, show: false});

    modalScope.rename = function (name) {
      if (name != props.name) {
        SchemaService.alterProperty($routeParams.database, {
          clazz: $scope.class2show,
          name: props.name,
          entry: "name",
          value: name
        }, $scope.strict).then(() => {
          let noti = S("The Property {{name}} has been renamed to {{newName}}").template({
            name: props.name,
            newName: name
          }).s;
          Notification.push({content: noti});
          props.name = name;
        }).catch((err) => {
          Notification.push({content: FormatErrorPipe.transform(err.json()), error: true});
        });
      }
    }
    modalPromise.$promise.then(modalPromise.show);

  }

  $scope.getEngine = function (index) {
    var engine = '';
    Database.getMetadata()["indexes"].forEach(function (e) {
      if (index.name == e.name) {
        engine = e.configuration.algorithm;
      }
    });

    return engine;
  }

  $scope.queryText = ""
  $scope.modificati = new Array;
  $scope.listTypes = ['BINARY', 'BOOLEAN', 'BYTE', 'EMBEDDED', 'EMBEDDEDLIST', 'EMBEDDEDMAP', 'EMBEDDEDSET', 'DECIMAL', 'FLOAT', 'DATE', 'DATETIME', 'DOUBLE', 'INTEGER', 'LINK', 'LINKLIST', 'LINKMAP', 'LINKSET', 'LONG', 'SHORT', 'STRING'];
  $scope.collateTypes = ['Case Insensitive', 'default'];
  $scope.modificato = function (result, prop) {
    let key = result['name'];
    if ($scope.modificati[result['name']] == undefined) {
      $scope.modificati[result['name']] = new Array(prop);
    }
    else {
      var elem = $scope.modificati[result['name']]
      var already = false;
      for (i in elem) {
        if (prop == elem[i]) {
          already = true
        }
      }
      if (already == false) {
        elem.push(prop);
      }
    }
  }
  $scope.addIndexFromExt = function (newIndex) {
    $scope.indexes.push(newIndex);
  }
  $scope.newIndex = function () {
    let modalScope = $scope.$new(true);
    modalScope.db = $scope.database;
    modalScope.classInject = clazz;
    modalScope.parentScope = $scope;
    modalScope.propertiesName = $scope.propertyNames;
    var modalPromise = $modal({template: 'views/database/newIndex.html', scope: modalScope, show: false});
    modalPromise.$promise.then(modalPromise.show);

  };
  $scope.refreshWindow = function () {
    $window.location.reload();
  }
  $scope.newProperty = function () {
    let modalScope = $scope.$new(true);
    modalScope.db = $scope.database;
    modalScope.classInject = clazz;
    modalScope.parentScope = $scope;
    var modalPromise = $modal({
      template: 'views/database/newProperty.html',
      animation: 'am-fade-and-slide-top',
      scope: modalScope,
      show: false,
      placement: 'bottom'
    });
    modalPromise.$promise.then(modalPromise.show);

  };
  $scope.addProperties = function (prop) {
    $scope.property.push(prop);
  }
  $scope.saveProperty = function (properties) {


    for (let result in properties) {

      var keyName = properties[result]['name'];
      var arrayToUpdate = $scope.modificati[keyName];

      if (arrayToUpdate) {
        arrayToUpdate.forEach(function (v) {
          var val = properties[result][v];
          if (val == 'Case Insensitive')
            val = 'ci';

          if (!val) {
            val = null;
          }
          var idx = result;
          if (properties[result].type === "DATE" || properties[result].type === "DATETIME") {
            if (v === 'min' || v === 'max') {
              val = "'" + val + "'";
            }
          }
          SchemaService.alterProperty($routeParams.database, {
            clazz: $scope.class2show,
            name: keyName,
            entry: v,
            value: val
          }).then(function (data) {
            let noti = S("The {{prop}} value of the property {{name}} has been modified to {{newVal}}").template({
              name: keyName,
              prop: v,
              newVal: val
            }).s;
            Notification.push({content: noti});
          }).catch((err) => {
            $scope.property[idx][v] = $scope.clonedProperty[idx][v];
            Notification.push({content: FormatErrorPipe.transform(err.json()), error: true});
          });
        });
      }

    }
    $scope.modificati = new Array;
    $scope.database.refreshMetadata($routeParams.database);
  }

  $scope.dropIndex = function (nameIndex) {

    Utilities.confirm($scope, $modal, $q, {

      title: 'Warning!',
      body: 'You are dropping index ' + nameIndex.name + '. Are you sure?',
      success: function () {
        SchemaService.dropIndex($routeParams.database, {
          name: nameIndex.name
        }, $scope.strict).then(() => {
          var index = $scope.indexes.indexOf(nameIndex)
          $scope.indexes.splice(index, 1);
          $scope.indexes.splice();
          Notification.push({content: "Index '" + nameIndex.name + "' dropped."})
        }).catch((err) => {

          Notification.push({content: FormatErrorPipe.transform(err.json()), error: true});
        })
      }
    });
  }
  $scope.dropProperty = function (result, name) {
    Utilities.confirm($scope, $modal, $q, {
      title: 'Warning!',
      body: 'You are dropping property  ' + name + '. Are you sure?',
      success: function () {
        SchemaService.dropProperty($routeParams.database, {
          clazz,
          name
        }, $scope.strict)
          .then(() => {
            for (var entry in $scope.property) {
              if ($scope.property[entry]['name'] == name) {
                var index = $scope.property.indexOf($scope.property[entry])
                $scope.property.splice(index, 1)
                Notification.push({content: "Property '" + name + "' successfully dropped."})
              }
            }
          }).catch((err) => {
          Notification.push({content: FormatErrorPipe.transform(err.json()), error: true});
        })
      }
    });
  }
  $scope.checkDisable = function (res, entry) {
    if (res[entry] == null || res[entry] == undefined || res[entry] == "") {
      return false;
    }
    return true;
  }
  $scope.checkTypeEdit = function (res) {

    var occupato = $scope.checkDisable(res, 'linkedClass');
    if (occupato) {
      res['linkedType'] = null;
      return true;
    }
    if (res['type'] == 'EMBEDDEDLIST' || res['type'] == 'EMBEDDEDSET' || res['type'] == 'EMBEDDEDMAP') {
      return false;
    }
    res['linkedType'] = null;
    return true;
  }
  $scope.checkClassEdit = function (res) {

    var occupatoType = $scope.checkDisable(res, 'linkedType');
    if (occupatoType) {
      res['linkedClass'] = null;
      return true;
    }
    if (res['type'] == 'LINK' || res['type'] == 'LINKLIST' || res['type'] == 'LINKSET' || res['type'] == 'LINKMAP' || res['type'] == 'EMBEDDED' || res['type'] == 'EMBEDDEDLIST' || res['type'] == 'EMBEDDEDSET' || res['type'] == 'EMBEDDEDMAP') {
      return false;
    }
    res['linkedClass'] = null;
    return true;
  }
  $scope.refreshPage = function () {
    $scope.database.refreshMetadata($routeParams.database);
    $route.reload();
  }
  $scope.rebuildIndex = function (indexName) {
    var sql = 'REBUILD INDEX ' + indexName;
    Spinner.start();
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: sql,
      limit: $scope.limit,
      verbose: false
    }, function (data) {
      Spinner.stopSpinner();
      Notification.push({content: "Index '" + indexName + "' rebuilded."})
    }, function (err) {
      Spinner.stopSpinner();
      Notification.push({content: err, error: true});
    });
  }
}]);
schemaModule.controller("IndexController", ['$scope', '$routeParams', '$route', '$location', 'Database', 'CommandApi', '$modal', '$q', 'Spinner', 'Notification', 'SchemaService', 'FormatErrorPipe', function ($scope, $routeParams, $route, $location, Database, CommandApi, $modal, $q, Spinner, Notification, SchemaService, FormatErrorPipe) {

  $scope.listTypeIndex = ['DICTIONARY', 'FULLTEXT', 'UNIQUE', 'NOTUNIQUE', 'DICTIONARY_HASH_INDEX', 'FULLTEXT_HASH_INDEX', 'UNIQUE_HASH_INDEX', 'NOTUNIQUE_HASH_INDEX'];
  $scope.newIndex = {"name": "", "type": "", "fields": ""}
  $scope.engine = ["LUCENE", "SBTREE"];
  $scope.prop2add = new Array;
  $scope.nameIndexToShow = $scope.classInject + '.';
  $scope.db.refreshMetadata($routeParams.database);
  $scope.property = Database.listPropertiesForClass($scope.classInject);


  $scope.strict = Database.isStrictSql();

  $scope.propertyNames = new Array;

  for (let inn in $scope.property) {
    $scope.propertyNames.push($scope.property[inn]['name'])
  }
  $scope.namesProp = $scope.propertyNames;
  $scope.addedField = function (nameField) {
    var index = $scope.prop2add.indexOf(nameField);

    if (index == -1) {
      $scope.prop2add.push(nameField)
    }
    else {
      $scope.prop2add.splice(index, 1)
    }
    var first = true;
    $scope.nameIndexToShow = $scope.classInject + '.';

    for (let entry in $scope.prop2add) {
      if (first) {
        $scope.nameIndexToShow = $scope.nameIndexToShow + $scope.prop2add[entry];
        first = !first
      }
      else {
        $scope.nameIndexToShow = $scope.nameIndexToShow + '_' + $scope.prop2add[entry];
      }
    }
  }
  $scope.saveNewIndex = function () {

    if ($scope.nameIndexToShow == undefined || $scope.nameIndexToShow == "" || $scope.nameIndexToShow == null)
      return;
    if ($scope.newIndex['type'] == undefined || $scope.newIndex['type'] == "" || $scope.newIndex['type'] == null)
      return;

    let name = $scope.nameIndexToShow;
    let clazz = $scope.classInject;
    let type = $scope.newIndex['type'];
    let props = $scope.prop2add;
    let engine = $scope.newIndex['engine'];
    let metadata = $scope.newIndex['metadata'];

    SchemaService.createIndex($routeParams.database, {
      name,
      clazz,
      props,
      type,
      engine,
      metadata
    }, $scope.strict).then(() => {

      $scope.$hide();

      Spinner.stopSpinnerPopup();

      Database.refreshMetadata($routeParams.database, function () {
        $route.reload();
      });
      Notification.push({content: `Index '${name}' created.`})

    }).catch((err) => {

      console.log(err);
      Spinner.stopSpinnerPopup();

      $scope.testMsgClass = 'alert alert-danger';
      $scope.testMsg = FormatErrorPipe.transform(err.json());
      Spinner.stopSpinnerPopup();

    })

  }
}]);

schemaModule.controller("PropertyController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', 'Spinner', 'Notification', 'SchemaService', 'FormatErrorPipe', '$route', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, Spinner, Notification, SchemaService, FormatErrorPipe, $route) {


  $scope.property = {
    "name": "",
    "type": "",
    "linkedType": "",
    "linkedClass": "",
    "mandatory": "false",
    "readonly": "false",
    "notNull": "false",
    "min": null,
    "max": null
  }
  $scope.listTypes = ['BINARY', 'BOOLEAN', 'BYTE', 'EMBEDDED', 'EMBEDDEDLIST', 'EMBEDDEDMAP', 'EMBEDDEDSET', 'DECIMAL', 'FLOAT', 'DATE', 'DATETIME', 'DOUBLE', 'INTEGER', 'LINK', 'LINKLIST', 'LINKMAP', 'LINKSET', 'LONG', 'SHORT', 'STRING'];
  $scope.database = Database;
  $scope.listClasses = $scope.database.listNameOfClasses();


  $scope.strict = Database.isStrictSql();

  $scope.salvaProperty = function () {

    let prop = $scope.property;

    let handleResponse = (payload) => {
      Spinner.stopSpinnerPopup();
      $scope.$hide();
      $scope.database.refreshMetadata($routeParams.database, function () {
        $route.reload();
      });
      Notification.push(payload);
    }
    let clazz = $scope.classInject;
    let name = $scope.property['name'];
    let type = $scope.property['type'];
    let linkedType = $scope.property['linkedType'];
    let linkedClass = $scope.property['linkedClass'];

    Spinner.startSpinnerPopup();

    let exclude = ["name", "type"];
    SchemaService.createProperty($routeParams.database, {
      clazz,
      name,
      type,
      linkedType,
      linkedClass
    }, $scope.strict).then(() => {
      let promises = Object.keys(prop).filter((p) => {
        return prop[p] != null && (exclude.indexOf(p) == -1);
      }).map((k) => {
        let entry = k;
        let value = prop[k];
        if (type === "DATE" || type === "DATETIME") {
          if (entry === 'min' || entry === 'max') {
            value = "'" + value + "'";
          }
        }
        return SchemaService.alterProperty($routeParams.database, {
          clazz,
          name,
          entry,
          value
        })
      })
      let errors = 0;
      let success = 0;
      let errorMsg = '';
      promises.forEach((p) => {
        p.then(() => {
          success++;
          if (success + errors == promises.length) {
            if (errorMsg === '') {
              handleResponse({content: "Property created."});
            } else {
              handleResponse({
                content: "Property created with warning : " + errorMsg,
                warning: true,
                sticky: true
              })
            }
          }
        }).catch((err) => {
          errors++;
          errorMsg += "<br>" + FormatErrorPipe.transform(err.json());
          if (success + errors == promises.length) {
            handleResponse({
              content: "Property created with warning :" + errorMsg,
              warning: true,
              sticky: true
            })
          }
        });
      });
    })

  }

  $scope.checkDisable = function (entry) {
    if ($scope.property[entry] == null || $scope.property[entry] == undefined || $scope.property[entry] == "") {
      return false;
    }
    return true;
  }
  $scope.checkDisableLinkedType = function (entry) {

    var occupato = $scope.checkDisable('linkedClass');
    if (occupato) {
      $scope.property['linkedType'] = null;
      return true;
    }
    if ($scope.property['type'] == 'EMBEDDEDLIST' || $scope.property['type'] == 'EMBEDDEDSET' || $scope.property['type'] == 'EMBEDDEDMAP') {
      return false;
    }
    $scope.property['linkedType'] = null;
    return true;
  }
  $scope.checkDisableLinkedClass = function (entry) {

    var occupatoType = $scope.checkDisable('linkedType');

    if (occupatoType) {
      $scope.property['linkedClass'] = null;
      return true;
    }
    if ($scope.property['type'] == 'LINK' || $scope.property['type'] == 'LINKLIST' || $scope.property['type'] == 'LINKSET' || $scope.property['type'] == 'LINKMAP' || $scope.property['type'] == 'EMBEDDED' || $scope.property['type'] == 'EMBEDDEDLIST' || $scope.property['type'] == 'EMBEDDEDSET' || $scope.property['type'] == 'EMBEDDEDMAP') {
      return false;
    }

    $scope.property['linkedClass'] = null;
    return true;
  }
}]);
schemaModule.controller("NewClassController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', 'Notification', '$translate', '$filter', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route, Notification, $translate, $filter) {

  $scope.property = {"name": "", "alias": null, "superclass": null, "abstract": false}
  $scope.database = Database;
  $scope.listClasses = $scope.database.listNameOfClasses();


  $scope.select2Options = {
    'multiple': true,
    'simple_tags': true,
    'tags': $scope.listClasses
  };
  $scope.links = {
    linkClusters: Database.getOWikiFor("Tutorial-Clusters.html")
  }
  $translate("class.clusters", $scope.links).then(function (data) {
    $scope.hint = data;
  });


  $scope.saveNewClass = function () {
    var sql = 'CREATE CLASS `' + $scope.property['name'] + "`";
    var abstract = $scope.property['abstract'] ? ' ABSTRACT ' : '';
    var alias = $scope.property['alias'] == null || $scope.property['alias'] == '' ? null : $scope.property['alias'];
    var supercl = $scope.property['superclass'] != null ? ' extends ' + $scope.property['superclass'] : '';
    var arrSuper = $scope.property['superClasses'];

    var superClasses = '';
    if ((arrSuper != null && arrSuper.length > 0)) {
      superClasses = ' extends ' + $filter('formatArray')($scope.property['superClasses'].map((c) => {
          return "`" + c + "`"
        }));
    }
    sql = sql + superClasses + abstract;


    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: sql,
      limit: $scope.limit,
      verbose: false
    }, function (data) {
      if (alias != null) {
        sql = 'ALTER CLASS ' + $scope.property['name'] + ' SHORTNAME `' + alias + "`";
        CommandApi.queryText({
          database: $routeParams.database,
          language: 'sql',
          text: sql,
          limit: $scope.limit
        }, function (data) {
          $scope.$hide();
          Notification.push({content: "Class '" + $scope.property['name'] + "' correctly created."})
          $scope.parentScope.refreshPage();
        }, function (error) {
          $scope.testMsg = error;
          $scope.testMsgClass = 'alert alert-danger'
        });
      }
      else {
        $scope.$hide();
        Notification.push({content: "Class '" + $scope.property['name'] + "' correctly created."})
        $scope.parentScope.refreshWindow();

      }
    }, function (error) {
      $scope.testMsgClass = 'alert alert-danger'

      $scope.testMsg = $filter('formatError')(error);
    });
  }
}]);
schemaModule.controller("IndexesController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', 'Spinner', 'Notification', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route, Spinner, Notification) {

  $scope.indexes = Database.getMetadata()["indexes"];

  $scope.links = {
    type: Database.getOWikiFor("Indexes.html#index-types"),
    engine: Database.getOWikiFor("Indexes.html")
  }
  $scope.rebuildIndex = function (indexName) {
    var sql = 'REBUILD INDEX ' + indexName;
    Spinner.start();
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: sql,
      limit: $scope.limit,
      verbose: false
    }, function (data) {
      Spinner.stopSpinner();
      Notification.push({content: "Index '" + indexName + "' rebuilded."})
    }, function (err) {
      Spinner.stopSpinner();
      Notification.push({content: err, error: true});
    });
  }

  $scope.dropIndex = function (nameIndex) {

    Utilities.confirm($scope, $modal, $q, {

      title: 'Warning!',
      body: 'You are dropping index ' + nameIndex.name + '. Are you sure?',
      success: function () {
        var sql = 'DROP INDEX ' + nameIndex.name;

        CommandApi.queryText({
          database: $routeParams.database,
          language: 'sql',
          text: sql,
          limit: $scope.limit,
          verbose: false
        }, function (data) {
          var index = $scope.indexes.indexOf(nameIndex)
          $scope.indexes.splice(index, 1);
          $scope.indexes.splice();
          Notification.push({content: "Index '" + nameIndex.name + "' dropped."})
        });
      }
    });
  }
  $scope.getFields = function (definition) {
    var fields = "";
    if (definition) {
      if (definition.indexDefinitions) {
        fields += " [";
        definition.indexDefinitions.forEach(function (elem, idx, array) {
          if (idx > 0) fields += ","
          fields += elem.field;
        });
        fields += "]";
      } else {
        fields += "[" + definition.field + "]";
      }
    }
    return fields;
  }
}]);


export default schemaModule.name;
