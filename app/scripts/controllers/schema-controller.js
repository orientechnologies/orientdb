var schemaModule = angular.module('schema.controller', ['database.services']);
schemaModule.controller("SchemaController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'ClassAlterApi', '$modal', '$q', '$route', '$window', 'Spinner', 'Notification', '$popover', 'GraphConfig', 'DocumentApi', function ($scope, $routeParams, $location, Database, CommandApi, ClassAlterApi, $modal, $q, $route, $window, Spinner, Notification, $popover, GraphConfig, DocumentApi) {

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
  $scope.clusterStrategies = ['round-robin', "default", "balanced", "local"];
  $scope.database = Database;
  $scope.database.refreshMetadata($routeParams.database);
  $scope.database = Database;
  $scope.listClassesTotal = $scope.database.listClasses();

  $scope.numberOfPage = new Array(Math.ceil($scope.listClassesTotal.length / $scope.countPage));
  $scope.listClasses = $scope.listClassesTotal.slice(0, $scope.countPage);

  $scope.colors = d3.scale.category20();


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
    var modalPromise = $modal({template: 'views/database/changeNameModal.html', scope: modalScope, show: false});

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
        var sql = 'DROP CLASS ' + nameClass['name'];

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
    $location.path("/database/" + $scope.database.getName() + "/browse/select * from " + className);
  }
  $scope.createRecord = function (className) {
    $location.path("/database/" + $scope.database.getName() + "/browse/create/" + className);
  }
  $scope.allIndexes = function () {
    $location.path("/database/" + $scope.database.getName() + "/schema/indexes");
  }
  $scope.createNewClass = function () {
    modalScope = $scope.$new(true);
    modalScope.db = database;

    modalScope.parentScope = $scope;
    var modalPromise = $modal({template: 'views/database/newClass.html', scope: modalScope, show: false});
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
schemaModule.controller("ClassEditController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', '$window', 'DatabaseApi', 'Spinner', 'PropertyAlterApi', 'Notification', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route, $window, DatabaseApi, Spinner, PropertyAlterApi, Notification) {
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

  $scope.clonedProperty = angular.copy($scope.property);
  $scope.propertyNames = new Array;

  for (inn in $scope.property) {
    $scope.propertyNames.push($scope.property[inn]['name'])
  }
  $scope.createNewRecord = function (className) {
    $location.path("/database/" + $scope.database.getName() + "/browse/create/" + className);
  }
  $scope.queryAll = function (className) {
    $location.path("/database/" + $scope.database.getName() + "/browse/select * from " + className);
  }
  $scope.canDrop = function (clazz) {

    return clazz != "V" && clazz != "E";
  }
  $scope.dropClass = function (nameClass) {

    Utilities.confirm($scope, $modal, $q, {

      title: 'Warning!',
      body: 'You are dropping class ' + nameClass + '. Are you sure?',
      success: function () {
        var sql = 'DROP CLASS ' + nameClass;

        CommandApi.queryText({
          database: $routeParams.database,
          language: 'sql',
          text: sql,
          limit: $scope.limit
        }, function (data) {
          Database.setMetadata(null);
          $location.path("/database/" + $scope.database.getName() + "/schema");

        });

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
        PropertyAlterApi.changeProperty($routeParams.database, {
          clazz: $scope.class2show,
          property: props.name,
          name: "name",
          value: name
        }).then(function (data) {
          var noti = S("The Property {{name}} has been renamed to {{newName}}").template({
            name: props.name,
            newName: name
          }).s;
          Notification.push({content: noti});
          props.name = name;
        }, function err(data) {
          Notification.push({content: data, error: true});
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
    var key = result['name'];
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
    modalScope = $scope.$new(true);
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
    modalScope = $scope.$new(true);
    modalScope.db = database;
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


    for (result in properties) {

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
          PropertyAlterApi.changeProperty($routeParams.database, {
            clazz: $scope.class2show,
            property: keyName,
            name: v,
            value: val
          }).then(function (data) {
            var noti = S("The {{prop}} value of the property {{name}} has been modified to {{newVal}}").template({
              name: keyName,
              prop: v,
              newVal: val
            }).s;
            Notification.push({content: noti});
          }, function (data) {
            $scope.property[idx][v] = $scope.clonedProperty[idx][v];
            Notification.push({content: data, error: true});
          });
        });
      }

    }
    $scope.modificati = new Array;
    $scope.database.refreshMetadata($routeParams.database);
  }
  $scope.recursiveSaveProperty = function (arrayToUpdate, clazz, properties, result, keyName) {

    if (arrayToUpdate != undefined && arrayToUpdate.length > 0) {

      var prop = arrayToUpdate[0];
      var newValue = properties[result][prop] != '' ? properties[result][prop] : null;
      if (newValue == 'Case Insensitive')
        newValue = 'ci';
      var sql = 'ALTER PROPERTY ' + clazz + '.' + keyName + ' ' + prop + ' ' + newValue;
      CommandApi.queryText({
        database: $routeParams.database,
        language: 'sql',
        text: sql,
        limit: $scope.limit
      }, function (data) {
        if (data) {
          var index = arrayToUpdate.indexOf(prop);
          arrayToUpdate.splice(index, 1);
          $scope.recursiveSaveProperty(arrayToUpdate, clazz);
        }
      }, function (error) {
        if (error) {
          return false;

        }
      });

    }
    return true;
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
          limit: $scope.limit
        }, function (data) {
          var index = $scope.indexes.indexOf(nameIndex)
          $scope.indexes.splice(index, 1);
          $scope.indexes.splice();
          Notification.push({content: "Index '" + nameIndex.name + "' dropped."})
        });
      }
    });
  }
  $scope.dropProperty = function (result, elementName) {
    Utilities.confirm($scope, $modal, $q, {
      title: 'Warning!',
      body: 'You are dropping property  ' + elementName + '. Are you sure?',
      success: function () {
        var sql = 'DROP PROPERTY ' + clazz + '.' + elementName;


        CommandApi.queryText({
          database: $routeParams.database,
          language: 'sql',
          text: sql,
          limit: $scope.limit
        }, function (data) {
          for (entry in $scope.property) {
            if ($scope.property[entry]['name'] == elementName) {
              // ($scope.property[entry])
              var index = $scope.property.indexOf($scope.property[entry])
              $scope.property.splice(index, 1)
              Notification.push({content: "Property '" + elementName + "' successfully dropped."})
            }
          }
        });
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
schemaModule.controller("IndexController", ['$scope', '$routeParams', '$route', '$location', 'Database', 'CommandApi', '$modal', '$q', 'Spinner', 'Notification', function ($scope, $routeParams, $route, $location, Database, CommandApi, $modal, $q, Spinner, Notification) {

  $scope.listTypeIndex = ['DICTIONARY', 'FULLTEXT', 'UNIQUE', 'NOTUNIQUE', 'DICTIONARY_HASH_INDEX', 'FULLTEXT_HASH_INDEX', 'UNIQUE_HASH_INDEX', 'NOTUNIQUE_HASH_INDEX'];
  $scope.newIndex = {"name": "", "type": "", "fields": ""}
  $scope.engine = ["LUCENE", "SBTREE"];
  $scope.prop2add = new Array;
  $scope.nameIndexToShow = $scope.classInject + '.';
  $scope.db.refreshMetadata($routeParams.database);
  $scope.property = Database.listPropertiesForClass($scope.classInject);


  $scope.propertyNames = new Array;

  for (inn in $scope.property) {
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

    for (entry in $scope.prop2add) {
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
    if ($scope.prop2add.length == 0)
      return;
    var proppps = '';
    var first = true
    for (entry in $scope.prop2add) {
      if (first) {
        proppps = proppps + $scope.prop2add[entry];
        first = !first
      }
      else {
        proppps = proppps + ',' + $scope.prop2add[entry];
      }

    }
    var nameInddd = proppps;
    nameInddd.replace(')', '');
    var sql = 'CREATE INDEX ' + $scope.nameIndexToShow + ' ON ' + $scope.classInject + ' ( ' + proppps + ' ) ' + $scope.newIndex['type'];

    if ($scope.newIndex['engine'] == 'LUCENE') {
      sql += ' ENGINE LUCENE';

      if ($scope.newIndex['metadata']) {
        sql += ' METADATA ' + $scope.newIndex['metadata'];
      }
    }
    $scope.newIndex['name'] = $scope.nameIndexToShow;
    $scope.newIndex['fields'] = proppps.split(",");
    Spinner.startSpinnerPopup();
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: sql,
      limit: $scope.limit,
      verbose: false
    }, function (data) {
      $scope.$hide();


      Spinner.stopSpinnerPopup();
      Notification.push({content: "Index '" + $scope.newIndex['name'] + "' created."})
      $scope.db.refreshMetadata($routeParams.database, function () {
        $scope.parentScope.addIndexFromExt($scope.newIndex);
      });
    }, function (error) {
      $scope.testMsgClass = 'alert alert-danger';
      $scope.testMsg = error;
      Spinner.stopSpinnerPopup();
    });
  }
}]);

schemaModule.controller("PropertyController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', 'Spinner', 'Notification', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, Spinner, Notification) {


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


  $scope.$watch("property['type']", function (data) {

  });
  $scope.salvaProperty = function () {

    var prop = $scope.property;
    var propName = $scope.property['name'];
    var propType = $scope.property['type'];
    if (propName == undefined || propType == undefined)
      return;
    var linkedType = prop['linkedType'] != null ? prop['linkedType'] : '';
    var linkedClass = prop['linkedClass'] != null ? prop['linkedClass'] : '';
    var sql = 'CREATE PROPERTY ' + $scope.classInject + '.' + propName + ' ' + propType + ' ' + linkedType + ' ' + linkedClass;
    Spinner.startSpinnerPopup();
    var allCommand = $q.when();

    var addCommandToExecute = function (sql, i, len) {
      allCommand = allCommand.then(function () {
        return executeCommand(sql, i, len);
      });
    }
    var executeCommand = function (sql, i, len) {
      var deferred = $q.defer();
      CommandApi.queryText({
        database: $routeParams.database,
        language: 'sql',
        text: sql,
        limit: $scope.limit,
        verbose: false
      }, function (data) {
        if (i == len) {
          $scope.database.refreshMetadata($routeParams.database, function () {
            $scope.parentScope.addProperties(prop);
            $scope.parentScope.indexes = Database.listIndexesForClass($scope.classInject);
          });
          Spinner.stopSpinnerPopup();
          $scope.$hide();
          Notification.push({content: "Property created."});

        }
        deferred.resolve(data);

      });
      return deferred.promise;
    }
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: sql,
      limit: $scope.limit,
      verbose: false
    }, function (data) {

      var len = Object.keys(prop).length;
      for (entry in prop) {
        if (prop[entry] == null) {
          len--;
          delete prop[entry];
        }
      }
      var i = 1;
      for (entry in prop) {


        var val = prop[entry];

        if (propType === "DATE" || propType === "DATETIME") {


          if (entry === 'min' || entry === 'max') {
            val = "'" + val + "'";
          }
        }
        var sql = 'ALTER PROPERTY ' + $scope.classInject + '.' + propName + ' ' + entry + ' ' + val;
        addCommandToExecute(sql, i, len);
        i++;
      }
    }, function (error) {
      Spinner.stopSpinnerPopup();
      Notification.push({content: error, error: true});
      $scope.$hide();
    });


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
    var sql = 'CREATE CLASS ' + $scope.property['name'];
    var abstract = $scope.property['abstract'] ? ' ABSTRACT ' : '';
    var alias = $scope.property['alias'] == null || $scope.property['alias'] == '' ? null : $scope.property['alias'];
    var supercl = $scope.property['superclass'] != null ? ' extends ' + $scope.property['superclass'] : '';
    var arrSuper = $scope.property['superClasses'];
    var superClasses = (arrSuper != null && arrSuper.length > 0) ? ' extends ' + $filter('formatArray')($scope.property['superClasses']) : ''
    sql = sql + superClasses+ abstract;


    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: sql,
      limit: $scope.limit,
      verbose: false
    }, function (data) {
      if (alias != null) {
        sql = 'ALTER CLASS ' + $scope.property['name'] + ' SHORTNAME ' + alias;
        CommandApi.queryText({
          database: $routeParams.database,
          language: 'sql',
          text: sql,
          limit: $scope.limit
        }, function (data) {
          $scope.$hide();
          Notification.push({content: "Class '" + $scope.property['name'] + "' correclty created."})
          $scope.parentScope.refreshPage();
        }, function (error) {
          $scope.testMsg = error;
          $scope.testMsgClass = 'alert alert-danger'
        });
      }
      else {
        $scope.$hide();
        Notification.push({content: "Class '" + $scope.property['name'] + "' correclty created."})
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
          if (idx > 0)fields += ","
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
