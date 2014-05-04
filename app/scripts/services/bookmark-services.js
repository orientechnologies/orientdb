var breadcrumb = angular.module('bookmarks.services', []);


breadcrumb.factory('Bookmarks', function ($resource, DocumentApi, $http, $q) {

    var resource = $resource(API + 'database/:database');


    resource.getAll = function (database) {
        var deferred = $q.defer();
        var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
        var query = "select * from StudioBookmarks";
        $http.post(text, query).success(function (data) {
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    resource.init = function (database) {
        var deferred = $q.defer();
        var text = API + 'command/' + database + '/sql/-/20?format=rid,type,version,class,graph';
        var query = "CREATE CLASS StudioBookmarks";
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
    }
    resource.getTags = function (database) {
        var deferred = $q.defer();
        var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
        var query = 'select distinct(value) as value from ( select expand(tags)  from StudioBookmarks)';
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