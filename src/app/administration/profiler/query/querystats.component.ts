import {
  Component,
  OnInit,
  OnDestroy,
  Input,
  OnChanges,
  SimpleChange,
  SimpleChanges
} from "@angular/core";
import { ProfilerService, MetricService } from "../../../core/services";

@Component({
  selector: "query-stats",
  templateUrl: "./querystats.component.html",
  styles: [""]
})
export class QueryStatsComponent implements OnInit, OnDestroy, OnChanges {
  queries: any = [];
  handle: any;

  @Input()
  params: any;
  constructor(private metrics: MetricService) {}

  ngOnInit(): void {
    this.handle = setInterval(() => {
      this.fetchQueries();
    }, 5000);
  }

  fetchQueries() {
    if (this.params.server) {
      this.metrics.getMetrics().then(response => {
        let histo = response.clusterStats[this.params.server].histograms;
        this.queries = Object.keys(histo)
          .filter(k => {
            return k.match(/db.*query./g) != null;
          })
          .map(k => {
            return Object.assign({}, histo[k], {
              query: k.substring(k.indexOf(".query.") + 7, k.length)
            });
          });
      });
    }
  }
  ngOnChanges(simpleChange: SimpleChanges) {
    this.fetchQueries();
  }
  ngOnDestroy(): void {
    clearInterval(this.handle);
  }
}
