import {
  Component,
  Input,
  ElementRef,
  NgZone,
  OnDestroy,
  ViewChild,
  OnInit,
  ViewContainerRef,
  OnChanges,
  SimpleChanges
} from "@angular/core";
import { MetricService } from "../../../../core/services";

@Component({
  selector: "cluster-management-overview",
  templateUrl: "./clusteroverview.component.html",
  styles: [""]
})
class ClusterOverviewComponent implements OnInit, OnChanges {
  @Input()
  private stats;
  handle: any;

  

  constructor(private metrics: MetricService) {}

  ngOnChanges(changes: SimpleChanges): void {}

  ngOnInit(): void {
    this.handle = setInterval(() => {
      this.fetchMetrics();
    }, 5000);
    this.fetchMetrics();
  }

  ngOnDestroy(): void {
    clearInterval(this.handle);
  }

  fetchMetrics() {
    this.metrics.getMetrics().then(data => {
      this.stats = data;
    });
  }
}

export { ClusterOverviewComponent };
