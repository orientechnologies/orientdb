'use strict';


angular.module('webappApp').factory("BreadCrumb", function ($rootScope) {

  var header = {
    title: ""
  }
  $rootScope.$on('$routeChangeStart', function (next, current) {
    header.title = '';
  });
  return header;
});



