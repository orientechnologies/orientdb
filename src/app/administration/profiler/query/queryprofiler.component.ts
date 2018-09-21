import { Component, OnInit } from "@angular/core";
import { ProfilerService, MetricService } from "../../../core/services";

@Component({
  selector: "query-profiler",
  templateUrl: "./queryprofiler.component.html",
  styles: [""]
})
export class QueryProfilerComponent implements OnInit {
  tab = "running";

  params = { database: null, server: null };
  databases: any;
  servers: string[];
  constructor(private metrics: MetricService) {}

  ngOnInit(): void {
    Promise.all([this.metrics.listDatabases(), this.metrics.getMetrics()]).then(
      response => {
        this.databases = response[0].databases;
        this.servers = Object.keys(response[1].clusterStats);
        this.params.server = this.servers[0];
      }
    );
  }
}
