angular.module('ngUtilFilters', []).filter('toRgbString', function () {

  return function hexToRgb(hex, alpha) {
    var bigint = parseInt(hex, 16);
    var r = (bigint >> 16) & 255;
    var g = (bigint >> 8) & 255;
    var b = bigint & 255;

    if (alpha) {
      return "rgba(" + r + "," + g + "," + b + "," + alpha + ")";
    } else {
      return "rgb(" + r + "," + g + "," + b + ")";
    }
  }
});

