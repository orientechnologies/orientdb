import {API} from '../constants';
let bookmark = angular.module('bookmarks.services', []);


bookmark.factory('Bookmarks', function ($resource, DocumentApi, $http, $q, Database) {

  var resource = $resource(API + 'database/:database');
  var CLAZZ = "_studio"
  resource.CLAZZ = CLAZZ;
  var TYPE = "Bookmark"
  resource.changed = false;
  resource.getAll = function (database) {
    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
    var query = "select * from {{clazz}} where type = '{{type}}' order by name";
    query = S(query).template({clazz: CLAZZ, type: TYPE}).s;
    $http.post(text, query).success(function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }

  resource.init = function (database) {
    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/20?format=rid,type,version,class,graph';
    var query = "CREATE CLASS {{clazz}}";
    query = S(query).template({clazz: CLAZZ}).s;
    $http.post(text, query).success(function (data) {
      Database.refreshMetadata(database, function () {
        deferred.resolve(data);
      });


    });
    return deferred.promise;
  }

  resource.addBookmark = function (database, item) {
    var deferred = $q.defer();
    item.type = TYPE;
    DocumentApi.createDocument(database, item['@rid'], item, function (data) {
      deferred.resolve(data);
    });

    return deferred.promise;
  }
  resource.refresh = function () {
    resource.tags = null;
    resource.changed = true;
    resource.changed = false;
  }
  resource.remove = function (database, bk) {
    var deferred = $q.defer();
    DocumentApi.deleteDocument(database, bk['@rid'], function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }
  resource.update = function (database, bk) {
    var deferred = $q.defer();
    DocumentApi.updateDocument(database, bk['@rid'], bk, function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }
  resource.getTags = function (database) {
    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
    var query = "select distinct(value) as value from ( select expand(tags)  from {{clazz}} where type = '{{type}}')";
    query = S(query).template({clazz: CLAZZ, type: TYPE}).s;
    $http.post(text, query).success(function (data) {
      var model = [];
      angular.forEach(data.result, function (v, index) {
        model.push(v.value);
      });
      deferred.resolve(model);
    });
    return deferred.promise;
  }
  return resource;
});
bookmark.factory('History', function ($resource, localStorageService, DocumentApi, $http, $q) {

  var resource = $resource(API + 'database/:database');
  var CLAZZ = "_studio_history"
  resource.CLAZZ = CLAZZ;

  return resource;
});
bookmark.factory('GraphConfig', function ($resource, localStorageService, DocumentApi, $http, $q, Database) {

  var resource = $resource(API + 'database/:database');
  var CLAZZ = "_studio"
  var TYPE = "GraphConfig"

  resource.CLAZZ = CLAZZ;

  resource.init = function () {
    var database = Database.getName();


    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/20?format=rid,type,version,class,graph';
    var query = "CREATE CLASS {{clazz}}";
    query = S(query).template({clazz: CLAZZ}).s;
    $http.post(text, query).success(function (data) {
      Database.refreshMetadata(database, function () {
        deferred.resolve(data);
      });


    });
    return deferred.promise;
  }
  resource.get = function () {

    var database = Database.getName();
    var username = Database.currentUser();

    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
    var query = "select * from {{clazz}} where user.name = '{{username}}' and type = '{{type}}'";
    query = S(query).template({clazz: CLAZZ, username: username, type: TYPE}).s;
    $http.post(text, query).success(function (data) {
      deferred.resolve(data.result[0]);
    });
    return deferred.promise;
  }
  resource.set = function (config) {
    var deferred = $q.defer();
    var database = Database.getName();
    var username = Database.currentUser();

    if (DocumentApi.isNew(config)) {

      config.type = TYPE;
      var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
      var query = "select * from OUser where name = '{{username}}'";
      query = S(query).template({username: username}).s;
      $http.post(text, query).success(function (data) {
        config.user = data.result[0];
        DocumentApi.createDocument(database, config['@rid'], config, function (data) {
          deferred.resolve(data);
        });
      });

    }
    else {
      DocumentApi.updateDocument(database, config['@rid'], config, function (data) {
        deferred.resolve(data);
      });
    }
    return deferred.promise;
  }
  return resource;
});

export default bookmark.name;
