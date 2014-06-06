var API = (function() {
    var m = window.location.pathname.match(/(.*\/)studio\/index.html/);
    return m && m[1] ? m[1] : '/';
})();

var STUDIO_VERSION = "1.7";
