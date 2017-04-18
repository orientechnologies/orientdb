"use strict";
var API = (function () {
    var m = window.location.pathname.match(/(.*\/)studio\/index.html/);
    return m && m[1] ? m[1] : '/api/';
})();
exports.API = API;
var STUDIO_VERSION = "2.2";
exports.STUDIO_VERSION = STUDIO_VERSION;
var POLLING = 5000;
exports.POLLING = POLLING;
