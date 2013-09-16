define([ 'marionette', 'utils/locale', 'landing/templates' ], function(Marionette, Locale, templates) {
  "use strict";

  return Marionette.Layout.extend({
    template : templates.profile_layout,
    
    ui: {
      selectServer: "#selectServer",
      selectDatabase: "#selectDatabase"
    },
    
    triggers: {
      'change #selectServer' : 'profiler:serverChanged',
      'change #selectDatabase' : 'profiler:databaseChanged'
    },
    
    regions : {
      queriestab : "#queries-tab",
      metricstab : "#metrics-tab",
      south : "#south"
    },
    
    onRender : function() {
      Marionette.Layout.prototype.onRender.apply(this);
      var self = this;
      
      var regionsMap = _.invert(this.regions);
      $('a[data-toggle="tab"]', this.el).on('shown', function (e) {
        self.trigger("profiler:" + regionsMap[e.target.hash]);
      })
    },
    
    serializeData : function() {
      var data = Marionette.Layout.prototype.serializeData.apply(this, arguments);
      data.servers = this.options.servers;
      data.databases = this.options.databases;
      data.serverId = this.options.serverId;
      data.database = this.options.database;
      return data;
    }
  });
});