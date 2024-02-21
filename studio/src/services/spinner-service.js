/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 26/08/13
 * Time: 13.55
 * To change this template use File | Settings | File Templates.
 */

import Spinner from 'spin.js';

let spinner = angular.module('spinner.services', []);

spinner.factory('Spinner', function () {
  var spinnerOpts = {
    lines: 9, // The number of lines to draw
    length: 7, // The length of each line
    width: 7, // The line thickness
    radius: 19, // The radius of the inner circle
    corners: 1, // Corner roundness (0..1)
    rotate: 0, // The rotation offset
    direction: 1, // 1: clockwise, -1: counterclockwise
    color: [
      'rgb(0,0,0)',
      'rgb(255,102,0)'
    ], // #rgb or #rrggbb or array of colors
    speed: 1, // Rounds per second
    trail: 60, // Afterglow percentage
    shadow: false, // Whether to render a shadow
    hwaccel: false, // Whether to use hardware acceleration
    className: 'spinner', // The CSS class to assign to the spinner
    zIndex: 2e9, // The z-index (defaults to 2000000000)
    top: 'auto', // Top position relative to parent in px
    left: 'auto' // Left position relative to parent in px
  };
  var spinner = new Spinner(spinnerOpts);

  spinner.start = function (cb) {
    var target = document.getElementById('spinner');

    //spinner.spin(target);
    $("#spinner-circle").removeClass("circle-stop")
    $("#spinner-circle").addClass("circle-start")


    if (cb) {
      $("#interrupter-container").removeClass("circle-interrupt-stop");
      $("#interrupter-container").addClass("circle-interrupt");
      $("#interrupter").click(cb);
    } else {
      $("#interrupter").addClass("invisible");
    }
    //$("#spinner-container").addClass('spinner-start')
  }
  spinner.stopSpinner = function () {
    //spinner.stop();
    $("#spinner-circle").removeClass("circle-start")
    $("#spinner-circle").addClass("circle-stop")
    $("#interrupter").unbind("click");
    $("#interrupter").removeClass("invisible");
    $("#interrupter-container").removeClass("circle-interrupt");
    $("#interrupter-container").addClass("circle-interrupt-stop")
    //$("#spinner-container").removeClass('spinner-start');

  }
  spinner.startSpinnerPopup = function () {
    var target = document.getElementById('spinner-popup');
    spinner.spin(target);
  }
  spinner.stopSpinnerPopup = function () {
    spinner.stop();
  }
  return spinner;
});


export default spinner.name;
