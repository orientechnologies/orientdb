var browseConfig = angular.module('browse.services', []);

breadcrumb.factory('BrowseConfig', function ($rootScope, Database, $location, localStorageService, $timeout) {


  var config = {
    limit: localStorageService.get('limit') || 20,
    hideSettings: true,
    keepLimit: '10',
    selectedContentType: 'JSON',
    selectedRequestType: 'COMMAND',
    selectedRequestLanguage: 'SQL',
    set: function (name, val) {
      this[name] = val;
      localStorageService.add(name, val);
    },
    get: function () {

    },
    storageSize: function (ms, cb) {
      $timeout(function () {
        cb(unescape(encodeURIComponent(JSON.stringify(localStorage))).length);
      }, ms);
    }
  };


  return config;
});
