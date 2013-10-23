var schemaModule = angular.module('schema.controller', ['database.services']);
schemaModule.controller("SchemaController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', '$window', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route, $window) {

    //for pagination
    $scope.countPage = 10;
    $scope.countPageOptions = [10, 20, 50, 100];
    $scope.currentPage = 1;

    $scope.database = Database;
    $scope.database.refreshMetadata($routeParams.database);
    $scope.database = Database;
    $scope.listClassesTotal = $scope.database.listClasses();

    $scope.numberOfPage = new Array(Math.ceil($scope.listClassesTotal.length / $scope.countPage));
    $scope.listClasses = $scope.listClassesTotal.slice(0, $scope.countPage);

    $scope.headers = ['name', 'superClass', 'alias', 'abstract', 'clusters', 'defaultCluster', 'records'];

    $scope.refreshPage = function () {
        $scope.database.refreshMetadata($routeParams.database);
        $route.reload();
    }
    $scope.refreshPage();
    $scope.setClass = function (clazz) {
        $scope.classClicked = clazz;
    }
    $scope.openClass = function (clazz) {
        $location.path("/database/" + $scope.database.getName() + "/browse/editclass/" + clazz.name);
    }
    $scope.refreshWindow = function () {
        $window.location.reload();
    }
    Database.setWiki("https://github.com/orientechnologies/orientdb-studio/wiki/Schema");

    $scope.$watch("countPage", function (data) {
        if ($scope.listClassesTotal) {
            $scope.listClasses = $scope.listClassesTotal.slice(0, $scope.countPage);
            $scope.currentPage = 1;
            $scope.numberOfPage = new Array(Math.ceil($scope.listClassesTotal.length / $scope.countPage));
        }
    });
    $scope.dropClass = function (nameClass) {

        Utilities.confirm($scope, $modal, $q, {

            title: 'Warning!',
            body: 'You are dropping class ' + nameClass['name'] + '. Are you sure?',
            success: function () {
                var sql = 'DROP CLASS ' + nameClass['name'];

                CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {

                    var elem = $scope.listClassesTotal.indexOf(nameClass);
                    console.log(elem);
                    $scope.listClassesTotal.splice(elem, 1)
                    $scope.listClassesTotal.splice();
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
    $scope.createNewClass = function () {
        modalScope = $scope.$new(true);
        modalScope.db = database;

        modalScope.parentScope = $scope;
        var modalPromise = $modal({template: 'views/database/newClass.html', scope: modalScope});
        $q.when(modalPromise).then(function (modalEl) {
            modalEl.modal('show');
        });
    }
    $scope.rebuildAllIndexes = function () {
        var sql = 'REBUILD INDEX *';
        CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
        });
    }
}]);
schemaModule.controller("ClassEditController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', '$window', 'DatabaseApi', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route, $window, DatabaseApi) {
    Database.setWiki("https://github.com/orientechnologies/orientdb-studio/wiki/Class");
    var clazz = $routeParams.clazz;
    $scope.class2show = clazz;
    $scope.database = Database;
    $scope.database.refreshMetadata($routeParams.database);
    $scope.modificati = undefined;
    $scope.limit = 20;
    $scope.queries = new Array;
    $scope.classClickedHeaders = ['name', 'type', 'linkedType', 'linkedClass', 'mandatory', 'readonly', 'notNull', 'min', 'max', 'Actions'];
    $scope.property = null;
    $scope.property = Database.listPropertiesForClass(clazz);
    $scope.propertyNames = new Array;

    for (inn in $scope.property) {
        $scope.propertyNames.push($scope.property[inn]['name'])
    }

    $scope.queryAll = function (className) {
        $location.path("/database/" + $scope.database.getName() + "/browse/select * from " + className);
    }

    $scope.dropClass = function (nameClass) {

        Utilities.confirm($scope, $modal, $q, {

            title: 'Warning!',
            body: 'You are dropping class ' + nameClass + '. Are you sure?',
            success: function () {
                var sql = 'DROP CLASS ' + nameClass;

                CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
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

    $scope.queryText = ""
    $scope.modificati = new Array;
    $scope.listTypes = ['BINARY', 'BOOLEAN', 'EMBEDDED', 'EMBEDDEDLIST', 'EMBEDDEDMAP', 'EMBEDDEDSET', 'DECIMAL', 'FLOAT', 'DATE', 'DATETIME', 'DOUBLE', 'INTEGER', 'LINK', 'LINKLIST', 'LINKMAP', 'LINKSET', 'LONG', 'SHORT', 'STRING'];

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
        modalScope.db = database;
        modalScope.classInject = clazz;
        modalScope.parentScope = $scope;
        modalScope.propertiesName = $scope.propertyNames;
        var modalPromise = $modal({template: 'views/database/newIndex.html', scope: modalScope});
        $q.when(modalPromise).then(function (modalEl) {
            modalEl.modal('show');
        });
    };
    $scope.refreshWindow = function () {
        $window.location.reload();
    }
    $scope.newProperty = function () {
        modalScope = $scope.$new(true);
        modalScope.db = database;
        modalScope.classInject = clazz;
        modalScope.parentScope = $scope;
        modalScope.propertiesName = $scope.propertyNames;
        var modalPromise = $modal({template: 'views/database/newProperty.html', scope: modalScope});
        $q.when(modalPromise).then(function (modalEl) {
            modalEl.modal('show');
        });
    };
    $scope.addProperties = function (prop) {
        $scope.property.push(prop);
    }
    $scope.saveProperty = function (properties) {

        for (result in properties) {

            var keyName = properties[result]['name'];
            var arrayToUpdate = $scope.modificati[keyName];

            if (!$scope.recursiveSaveProperty(arrayToUpdate, clazz, properties, result, keyName)) {

                return;
            }
        }
        $scope.modificati = undefined;
        $scope.database.refreshMetadata($routeParams.database);
    }
    $scope.recursiveSaveProperty = function (arrayToUpdate, clazz, properties, result, keyName) {

        if (arrayToUpdate != undefined && arrayToUpdate.length > 0) {

            var prop = arrayToUpdate[0];
            var newValue = properties[result][prop] != '' ? properties[result][prop] : null;
            var sql = 'ALTER PROPERTY ' + clazz + '.' + keyName + ' ' + prop + ' ' + newValue;

            CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
                if (data) {
                    var index = arrayToUpdate.indexOf(prop);
                    arrayToUpdate.splice(index, 1);
                    $scope.recursiveSaveProperty(arrayToUpdate, clazz);
                }
            }, function (error) {
                if (error) {
                    console.log('error')
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

                CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
                    var index = $scope.indexes.indexOf(nameIndex)
                    $scope.indexes.splice(index, 1);
                    $scope.indexes.splice();
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


                CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
                    for (entry in $scope.property) {
                        if ($scope.property[entry]['name'] == elementName) {
                            // console.log($scope.property[entry])
                            var index = $scope.property.indexOf($scope.property[entry])
                            $scope.property.splice(index, 1)
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
        CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
        });
    }
}]);
schemaModule.controller("IndexController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q) {

    $scope.listTypeIndex = [ 'DICTIONARY', 'FULLTEXT', 'UNIQUE', 'NOTUNIQUE', 'DICTIONARY_HASH_INDEX', 'FULLTEXT_HASH_INDEX', 'UNIQUE_HASH_INDEX', 'NOTUNIQUE_HASH_INDEX' ];
    $scope.newIndex = {"name": "", "type": "", "fields": "" }
    $scope.namesProp = $scope.propertiesName;
    $scope.prop2add = new Array;
    $scope.nameIndexToShow = $scope.classInject + '.';

    $scope.addedField = function (nameField) {
        var index = $scope.prop2add.indexOf(nameField);

        if (index == -1) {
            $scope.prop2add.push(nameField)
        }
        else {
            $scope.prop2add.splice(index, 1)
        }
        var first = true;
        $scope.nameIndexToShow = $scope.classInject + '_';

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
        $scope.newIndex['name'] = $scope.nameIndexToShow;
        $scope.newIndex['fields'] = proppps;

        CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
            $scope.hide();
            $scope.parentScope.addIndexFromExt($scope.newIndex);
        }, function (error) {
            $scope.testMsgClass = 'alert alert-error'
            $scope.testMsg = error;
        });
    }
}]);

schemaModule.controller("PropertyController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q) {


    $scope.property = {"name": "", "type": "", "linkedType": "", "linkedClass": "", "mandatory": "false", "readonly": "false", "notNull": "false", "min": null, "max": null}
    $scope.listTypes = ['BINARY', 'BOOLEAN', 'EMBEDDED', 'EMBEDDEDLIST', 'EMBEDDEDMAP', 'EMBEDDEDSET', 'DECIMAL', 'FLOAT', 'DATE', 'DATETIME', 'DOUBLE', 'INTEGER', 'LINK', 'LINKLIST', 'LINKMAp', 'LINKSET', 'LONG', 'SHORT', 'STRING'];
    $scope.database = Database;
    $scope.listClasses = $scope.database.listNameOfClasses();

    $scope.salvaProperty = function () {

        var prop = $scope.property;
        var propName = $scope.property['name'];
        var propType = $scope.property['type'];
        if (propName == undefined || propType == undefined)
            return;
        var linkedType = prop['linkedType'] != null ? prop['linkedType'] : '';
        var linkedClass = prop['linkedClass'] != null ? prop['linkedClass'] : '';
        var sql = 'CREATE PROPERTY ' + $scope.classInject + '.' + propName + ' ' + propType + ' ' + linkedType + ' ' + linkedClass;
        console.log(sql);
        CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {

        });

        var i = 1;
        for (entry in prop) {
            var sql = 'ALTER PROPERTY ' + $scope.classInject + '.' + propName + ' ' + entry + ' ' + prop[entry];
            CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
                i++;
                if (i == 5) {
                    $scope.database.refreshMetadata($routeParams.database, function () {
                        $scope.parentScope.addProperties(prop);
                    });
                    $scope.hide();
                }
            });
        }

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
        if ($scope.property['type'] == 'LINKLIST' || $scope.property['type'] == 'LINKSET' || $scope.property['type'] == 'LINKMAP' || $scope.property['type'] == 'EMBEDDED' || $scope.property['type'] == 'EMBEDDEDLIST' || $scope.property['type'] == 'EMBEDDEDSET' || $scope.property['type'] == 'EMBEDDEDMAP') {
            return false;
        }

        $scope.property['linkedClass'] = null;
        return true;
    }
}]);
schemaModule.controller("NewClassController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$modal', '$q', '$route', function ($scope, $routeParams, $location, Database, CommandApi, $modal, $q, $route) {

    $scope.property = {"name": "", "alias": null, "superclass": null, "abstract": false}
    $scope.database = Database;
    $scope.listClasses = $scope.database.listNameOfClasses();

    $scope.saveNewClass = function () {
        var sql = 'CREATE CLASS ' + $scope.property['name'];
        var abstract = $scope.property['abstract'] ? ' ABSTRACT ' : '';
        var alias = $scope.property['alias'] == null || $scope.property['alias'] == '' ? null : $scope.property['alias'];
        sql = sql + abstract;
        var supercl = $scope.property['superclass'] != null ? ' extends ' + $scope.property['superclass'] : '';
        sql = sql + supercl;

        CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
            if (alias != null) {
                sql = 'ALTER CLASS ' + $scope.property['name'] + ' SHORTNAME ' + alias;
                CommandApi.queryText({database: $routeParams.database, language: 'sql', text: sql, limit: $scope.limit}, function (data) {
                    $scope.hide();
                    $scope.parentScope.refreshPage();
                }, function (error) {
                    $scope.testMsg = error;
                    $scope.testMsgClass = 'alert alert-error'
                });
            }
            else {
                console.log('reload');
                $scope.parentScope.refreshWindow();
                $scope.hide();
            }
        }, function (error) {
            $scope.testMsgClass = 'alert alert-error'
            $scope.testMsg = error;
        });
    }
}]);
