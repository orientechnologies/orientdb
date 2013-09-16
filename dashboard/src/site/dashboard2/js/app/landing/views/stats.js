define([ 'marionette', 'underscore', 'landing/templates', 'd3' ], function(Marionette, _, templates) {
  "use strict";

  return Marionette.ItemView.extend({
    template : templates.stats,
    onRender : function() {
      // prepare the data
      var states = this.collection.pluck('status');
      var dataset = [];
      for ( var i = 0; i < states.length; i++) {
        var idx = _.pluck(dataset, 'category').indexOf(states[i]);
        if (idx < 0) {
          dataset.push({
            category : states[i],
            measure : 1
          });
        } else {
          dataset[idx].measure = dataset[idx].measure + 1;
        }
      }
      this.renderPie(dataset);
    },
    renderPie : function(dataset) {
      var formatAsPercentage = d3.format("%");
      var width = 300, height = 300, radius = Math.min(width, height) / 2, outerRadius = radius - 20, innerRadius = radius - 100,
      // for animation
      innerRadiusFinal = outerRadius * .5, innerRadiusFinal3 = outerRadius * .45, color = d3.scale.category20();

      var vis = d3.select(this.el).select("#chart").append("svg:svg").data([ dataset ]).attr("width", width).attr("height", height).append(
          "svg:g").attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

      var arc = d3.svg.arc().outerRadius(outerRadius).innerRadius(innerRadius);

      // for animation
      var arcFinal = d3.svg.arc().innerRadius(innerRadiusFinal).outerRadius(outerRadius);
      var arcFinal3 = d3.svg.arc().innerRadius(innerRadiusFinal3).outerRadius(outerRadius);

      var pie = d3.layout.pie().value(function(d) {
        return d.measure;
      });

      var arcs = vis.selectAll("g.slice").data(pie).enter().append("svg:g").attr("class", "slice").on("mouseover", mouseover).on(
          "mouseout", mouseout);

      arcs.append("svg:path").attr("fill", function(d, i) {
        if (d.data.category === "ONLINE") {
          return '#468847';
        } else if (d.data.category === "OFFLINE") {
          return '#d62728';
        }
        return color(i);
      }).attr("d", arc).append("svg:title").text(function(d) {
        return d.data.category + ": " + formatAsPercentage(d.data.measure);
      });

      d3.selectAll("g.slice").selectAll("path").transition().duration(750).delay(10).attr("d", arcFinal);

      arcs.filter(function(d) {
        return d.endAngle - d.startAngle > 0.2;
      }).append("svg:text").attr("dy", ".35em").attr("text-anchor", "middle").attr("transform", function(d) {
        return "translate(" + arcFinal.centroid(d) + ")rotate(" + angle(d) + ")";
      }).text(function(d) {
        return d.data.category;
      });

      // Computes the label angle of an arc, converting
      // from radians to degrees.
      function angle(d) {
        var a = (d.startAngle + d.endAngle) * 90 / Math.PI - 90;
        return a > 90 ? a - 180 : a;
      }

      // Pie chart title
      //vis.append("svg:text").attr("dy", ".35em").attr("text-anchor", "middle").text("Servers Health").attr("class", "title");
      function mouseover() {
        d3.select(this).select("path").transition().duration(750).attr("d", arcFinal3);
      }

      function mouseout() {
        d3.select(this).select("path").transition().duration(750).attr("d", arcFinal);
      }

    }
  });
});