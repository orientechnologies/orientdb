var aside = angular.module('graph.services', []);

aside.factory('Graph', function () {

    var data = [];
    return {
        query: "",
        data: data,
        add: function (d) {
            this.data = this.data.concat(d);
        },
        remove: function (d) {
            this.params.cls = "";
        }
    }
})
;