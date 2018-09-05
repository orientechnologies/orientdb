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
import { downgradeComponent } from "@angular/upgrade/static";
import { MetricService } from "../../../../core/services";

@Component({
  selector: "server-management",
  templateUrl: "./servermanagement.component.html",
  styles: [""]
})
class ServerManagementComponent implements OnInit, OnChanges {
  @Input()
  private name;
  @Input()
  private stats;
  handle: any;
  clusterStats: any;
  servers: string[];
  server: any = {};
  selectedServer: string;

  constructor(private metrics: MetricService) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;
  }

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
      console.log(data);
      this.servers = Object.keys(data.clusterStats);
      this.selectedServer = this.servers[0];
      this.clusterStats = data.clusterStats;
      let gauges = data.clusterStats[this.selectedServer].gauges;
      this.server = {
        status: "ONLINE",
        javaVersion: gauges["server.info.javaVersion"].value,
        javaVendor: gauges["server.info.javaVendor"].value,
        osName: gauges["server.info.osName"].value,
        osArch: gauges["server.info.osArch"].value,
        osVersion: gauges["server.info.osVersion"].value,
        cpus: gauges["server.info.cpus"].value,
        version: gauges["server.info.version"].value
      };
    });
  }

  formatAddress(addresses) {
    if (addresses) {
      var address = "";
      var ports = " [";
      addresses.listeners.forEach(function(l, idx, arr) {
        if (idx == 0) {
          address += l.listen.split(":")[0];
        }
        ports += l.listen.split(":")[1];
        if (idx < arr.length - 1) {
          ports += ",";
        }
      });
      ports += "]";
      return address + ports;
    }
  }
}

export { ServerManagementComponent };
