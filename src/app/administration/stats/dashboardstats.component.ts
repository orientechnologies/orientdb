import {
  Component,
  Input,
  ElementRef,
  NgZone,
  OnDestroy,
  ViewChild,
  OnInit,
  ViewContainerRef
} from "@angular/core";
import { downgradeComponent } from "@angular/upgrade/static";
import { MetricService, NotificationService } from "../../core/services";

declare const angular: any;

@Component({
  selector: "dashboard-stats",
  templateUrl: "./dashboardstats.component.html",
  styles: [""]
})
class DashboardStatsComponent implements OnInit, OnDestroy {
  private servers = [];
  private clusterStats = {};
  private serversClass = "";
  private handle;
  constructor(
    private metrics: MetricService,
    private noti: NotificationService,
    private zone: NgZone
  ) {}

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
    this.metrics
      .getMetrics()
      .then(data => {
        this.zone.run(() => {
          this.servers = Object.keys(data.clusterStats);
          let dim = 12 / this.servers.length;
          this.serversClass = "col-md-" + (dim < 4 ? 4 : dim);
          this.clusterStats = data.clusterStats;
        });
      })
      .catch(response => {
        this.noti.push({ content: "Error retrieving metrics", error: true });
        clearInterval(this.handle);
      });
  }
}

angular
  .module("dashboard.components", [])
  .directive(
    `dashboard-stats`,
    downgradeComponent({ component: DashboardStatsComponent })
  );

export { DashboardStatsComponent };
