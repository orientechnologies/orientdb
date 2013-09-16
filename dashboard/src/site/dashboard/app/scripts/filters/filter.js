angular.module('OFilter', []).filter('nopound', function () {

    return function (input, args) {
        return input ? input.replace("#","") : input;
    };
});