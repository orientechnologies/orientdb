var breadcrumb = angular.module('bookmarks.services', []);


breadcrumb.factory('Bookmarks', function ($resource, DocumentApi, $http, $q) {

    var resource = $resource(API + 'database/:database');


    resource.getAll = function (database) {
        var deferred = $q.defer();
        var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,shallow,graph';
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
        var doc = DocumentApi.createNewDoc("StudioBookmarks");

        DocumentApi.createDocument(database, doc['@rid'], doc, function (data) {

        });
    }
    return resource;
});