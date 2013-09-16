define([ 'backbone', 'underscore', 'landing/models', 'livecollection' ], function(Backbone, _, Models, LiveCollection) {
  'use strict';

  var Base = LiveCollection.extend({
    limit : -1,
    initialize : function(models, options) {
      if (options) {
        var self = this;
        _.each(options, function(value, key) {
          self[key] = value;
        });
      }
    },
    url : function() {
      var url = '/query/monitor/sql/select';
      if (this.fields) {
        url += ' ' + this.fields;
      }
      url += ' from ' + this.oclass;
      if (this.filters) {
        var idx = 0;
        _.each(this.filters, function(value, key) {
          url += (idx === 0 ? ' where ' : ' and ');
          url += ' ' + key + ' = ';
          if (_.isString(value)) {
            url += '%27' + value + '%27';
          } else {
            url += value;
          }
          idx++;
        });
      }
      if (this.order) {
        url += ' order by ' + this.order;
        if (this.direction) {
          url += ' ' + this.direction;
        }
      }
      if (this.limit) {
        url += '/' + this.limit;
      }
      return url;
    },
    parse : function(response) {
      return response.result;
    }
  });

  var Servers = Base.extend({
    model : Models.Server,
    oclass : 'Server'
  });

  var Logs = Base.extend({
    model : Models.ORecord,
    oclass : 'Log',
    order : 'date',
    direction : 'desc'
  });

  var Dictionary = Base.extend({
    model : Models.ORecord,
    oclass : 'Dictionary',
    order : 'name'
  });

  var Metrics = Base.extend({
    model : Models.ORecord,
    oclass : 'Metric'
  });

  var RealtimeMetrics = Base.extend({
    model : Backbone.Model,
    url : function() {
      return '/metrics/monitor/' + this.server + '/realtime/' + this.kind + '/' + this.name;
    },
    parse : function(response) {
      var self = this;
      var metrics;
      if (response.result && response.result.length) {
        metrics = [];
        for ( var i = 0; i < response.result.length; i++) {
          _.each(response.result[i], function(value, key) {
            if (key != self.name) {
              key = key.replace(new RegExp("^" + self.name.escapeRegExp()), '');
            }
            metrics.push({
              name : key,
              value : value
            });
          });
        }
      }
      return metrics;
    },
    clean : function() {
      $.ajax({
        type : 'DELETE',
        url : '/metrics/monitor/' + this.server + '/realtime/' + this.name,
        contentType : "application/json; charset=utf-8",
        async : false
      });
    }
  });

  return {
    Servers : Servers,
    Dictionary : Dictionary,
    Logs : Logs,
    Metrics : Metrics,
    RealtimeMetrics : RealtimeMetrics
  };
});
