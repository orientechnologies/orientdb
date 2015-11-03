angular.module('ngMoment', []).filter('fromNow', function () {

  return function (input, args) {
    if (!(input instanceof Date)) {
      input = new Date(parseInt(input));
    }
    return moment(input).format('MMMM Do YYYY, H:mm');;
    //return moment(input).fromNow();
  };
}).filter('formatDate',function(){
  return function (input, args) {
    if (!(input instanceof Date)) {
      input = new Date(parseInt(input));
    }
    return moment(input).format('MMMM Do YYYY, H:mm');;
  };
});
