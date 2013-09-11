angular.module('filters', []).filter('capitalize',function () {
    return function (input, scope) {
        if (input != null)
            return input.substring(0, 1).toUpperCase() + input.substring(1);
    }
}).filter('checkmark',function () {
        return function (input) {
            return input ? '\u2713' : '\u2718';
        };
    }).filter('sizeFormat', function () {

        return function (size) {

            if (size) {
                if (size > 1000000000) {
                    return Math.round(size / 10000000) + " Tb";
                }
                else if (size > 1000000) {
                    return Math.round(size / 1000000) + " Mb";
                }
                else if (size > 1000) {

                    return Math.round(size / 1000) + " Kb";
                }
                return size + " b";
            }
            return size;
        };
    });
