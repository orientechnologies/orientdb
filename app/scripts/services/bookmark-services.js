var breadcrumb = angular.module('bookmarks.services', []);


breadcrumb.factory('Bookmarks', function ($resource, DocumentApi, $http, $q) {

    var resource = $resource(API + 'database/:database');
    var CLAZZ = "_studio_bookmark"
    resource.CLAZZ = CLAZZ;
    resource.changed = false;
    resource.getAll = function (database) {
        var deferred = $q.defer();
        var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
        var query = "select * from {{clazz}} order by name";
        query = S(query).template({clazz: CLAZZ}).s;
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
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    resource.addBookmark = function (database, item) {
        var deferred = $q.defer();
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
        var query = 'select distinct(value) as value from ( select expand(tags)  from {{clazz}})';
        query = S(query).template({clazz: CLAZZ}).s;
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
breadcrumb.factory('History', function ($resource, localStorageService, DocumentApi, $http, $q) {

    var resource = $resource(API + 'database/:database');
    var CLAZZ = "_studio_history"
    resource.CLAZZ = CLAZZ;

    return resource;
});