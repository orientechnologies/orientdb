export class MetersAggregator {
  metrics: any;
  oldMetrics: {};
  constructor(metrics) {
    this.metrics = metrics;
  }

  next(stats) {
    let metrics = [];

    if (!this.oldMetrics) {
      this.oldMetrics = {};
      this.metrics.forEach(m => {
        this.oldMetrics[m.name] = this.countOps(m.regex, stats);
      });
    }

    this.metrics.forEach(m => {
      metrics.push({
        name: m.name,
        label : m.label,
        values: [this.countOps(m.regex, stats) - this.oldMetrics[m.name]]
      });
    });

    this.metrics.forEach(m => {
      this.oldMetrics[m.name] = this.countOps(m.regex, stats);
    });

    return metrics;
  }
  countOps(regex, meters) {
    return Object.keys(meters)
      .filter(k => {
        return k.match(regex) != null;
      })
      .map(k => {
        return meters[k].count;
      })
      .reduce((a, b) => {
        return a + b;
      }, 0);
  }
}
