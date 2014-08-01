var icon = angular.module('icon.services', []);

icon.factory('Icon', function ($http, $q) {


    var icons = {

        icons: function () {
            var self = this;
            var deferred = $q.defer();
            if (!this.iconset) {
                $http.get('config/config.json').success(function (data) {
                    self.iconset = data['glyphs'];
                    deferred.resolve(data['glyphs']);
                });
            }
            return deferred.promise;
        }
    }


    return icons;
});