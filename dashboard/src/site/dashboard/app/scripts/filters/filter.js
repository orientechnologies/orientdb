var Ofilter = angular.module('OFilter', []);
Ofilter.filter('nopound', function () {

    return function (input, args) {
        return input ? input.replace("#", "") : input;
    };
});
Ofilter.filter('ctype', function () {

    return function (input, args) {
        var index = input.indexOf(".");
        return input.substring(0, index);
    };
});
Ofilter.filter('cname', function () {

  return function (input, args) {
    var index = input.indexOf(".");
    return input.substring(index + 1, input.length);
  };
});
Ofilter.filter('tconfig', function () {

    return function (input, args) {
        if (input) {
            delete input["@type"];
            delete input["@version"];
        }
        return input;
    };
});

Ofilter.filter('mvalue', function () {

    return function (input, arg1, arg2) {
        var fields = ['entries', 'min', 'max', 'average', 'total'];
        var params = ['value'];
        return arg1[arg2] == 'CHRONO' ? fields : params;
    };
});
