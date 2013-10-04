var Ofilter = angular.module('OFilter', []);
Ofilter.filter('nopound', function () {

    return function (input, args) {
        return input ? input.replace("#","") : input;
    };
});
Ofilter.filter('ctype', function () {

    return function (input, args) {
        var index = input.indexOf(".");
        return input.substring(0,index);
    };
});
Ofilter.filter('cname', function () {

    return function (input, args) {
        var index = input.indexOf(".");
        return input.substring(index+1,input.length);
    };
});