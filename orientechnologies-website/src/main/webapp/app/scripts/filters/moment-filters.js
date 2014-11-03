angular.module('ngMoment', []).filter('fromNow', function () {

  return function (input, args) {
    return moment(input).fromNow();
  };
});
