let IconServices = angular.module('icon.services', []);

import Config from '../config/config.json';


IconServices.factory('Icon', function ($http, $q, $timeout) {


  var icons = {

    icons: function () {
      var self = this;
      var deferred = $q.defer();
      if (!this.iconset) {


        this.iconset = Config['glyphs'];
        var newData = Config['glyphs'].map(function (d) {
          d.css = 'icon-' + d.css;
          return d;
        });
        deferred.resolve(newData);

      } else {
        $timeout(function () {
          deferred.resolve(self.iconset);
        });
      }
      return deferred.promise;
    }
  }


  return icons;
});

export default IconServices.name;
