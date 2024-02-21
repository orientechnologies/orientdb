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
  server: string;
  @Input()
  database: string;
  constructor(private metrics: MetricService) {}

  ngOnInit(): void {
    this.handle = setInterval(() => {
      this.fetchQueries();
    }, 5000);
  }

  fetchQueries() {
    if (this.server) {
      this.metrics.getMetrics().then(response => {
        let histo = response.clusterStats[this.server].histograms;
        this.queries = Object.keys(histo)
          .filter(k => {
            return k.match(/db.*query./g) != null;
          })
          .filter(k => {
            let db = k.substring(k.indexOf("db.") + 3, k.indexOf(".query."));
            return this.database ? this.database === db : true;
          })
          .map(k => {
            let statement = k.substring(k.indexOf(".query.") + 7, k.length);
            let language = statement.substring(0, statement.indexOf("."));
            let query = statement.substring(
              statement.indexOf(".")+1,
              statement.length
            );
            let db = k.substring(k.indexOf("db.") + 3, k.indexOf(".query."));
            return Object.assign({}, histo[k], {
              query: query,
              language: language,
              database: db
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
