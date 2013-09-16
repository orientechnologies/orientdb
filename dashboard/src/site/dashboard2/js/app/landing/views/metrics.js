define([ 'jquery', 'marionette', 'd3', 'nvd3', 'landing/templates', 'daterangepicker' ], function($, Marionette, d3, nv, templates) {
  "use strict";

  // The grid view
  return Marionette.ItemView.extend({
    template : templates.metrics,

    ui : {
      selectMetric : "#selectMetric"
    },

    triggers : {
      "change #selectMetric" : "metrics:metricchanged"
    },

    onShowCalled : function() {
      Marionette.ItemView.prototype.onShowCalled.apply(this);

      $('#reportrange', this.el).daterangepicker({
        ranges : {
          'Today' : [ 'today', 'today' ],
          'Yesterday' : [ 'yesterday', 'yesterday' ],
          'Last 7 Days' : [ Date.today().add({
            days : -6
          }), 'today' ],
          'Last 30 Days' : [ Date.today().add({
            days : -29
          }), 'today' ],
          'This Month' : [ Date.today().moveToFirstDayOfMonth(), Date.today().moveToLastDayOfMonth() ],
          'Last Month' : [ Date.today().moveToFirstDayOfMonth().add({
            months : -1
          }), Date.today().moveToFirstDayOfMonth().add({
            days : -1
          }) ]
        },
        opens : 'left'
      }, function(start, end) {
        $('#reportrange span', this.el).html(start.toString('MMMM d, yyyy') + ' - ' + end.toString('MMMM d, yyyy'));
      });

      if (this.collection.length) {
        this.renderChart(this.setupData(this.collection.toJSON()));
      }
    },

    setupData : function(data) {
      var result = [];
      if (data && data.length) {
        result.push({key : 'max', values : []});
        result.push({key : 'average', values : []});
        result.push({key : 'min', values : []});

        for ( var i = 0; i < data.length; i++) {
          for ( var z = 0; z < result.length; z++) {
            var point = {
              x : new Date(data[i].dateTo).getTime(),
              y : data[i][result[z].key]
            };
            result[z].values.push(point);
          }
        }
      }
      return result;
    },

    renderChart : function(data) {
      var self = this;
      if (data && data.length) {
        nv.addGraph(function() {
          var chart = nv.models.lineWithFocusChart();
          
          var div = $("#chart", self.el);
          var width = $(div).width();

          chart.xAxis.tickFormat(function(d) {
            return d3.time.format('%x')(new Date(d));
          });
          
          chart.x2Axis.tickFormat(function(d) {
            return d3.time.format('%x')(new Date(d));
          });

          chart.yAxis.tickFormat(d3.format(',.2f'));
          
          chart.y2Axis.tickFormat(d3.format(',.2f'));
          
          d3.select(self.el).select("#chart svg")
              .attr("width", width)
              .attr("height", width / 2)
              .datum(data).transition().duration(500).call(chart);

          nv.utils.windowResize(chart.update);

          return chart;
        });
      }
    },

    serializeData : function() {
      var data = Marionette.CompositeView.prototype.serializeData.apply(this, arguments);
      data.dictionary = this.options.dictionary;
      data.metric = this.options.metric;
      return data;
    }
  });
});