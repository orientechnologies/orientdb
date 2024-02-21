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
import { MetricService, AgentService } from "../../../../core/services";

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
  tab = "overview";
  currentStats: any;
  private ee = true;

  constructor(private metrics: MetricService, private agent: AgentService) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;
  }

  ngOnInit(): void {
    this.ee = this.agent.active;

    if (this.ee) {
      this.handle = setInterval(() => {
        this.fetchMetrics();
      }, 5000);
      this.fetchMetrics();
    } else {
      this.servers = ["orientdb"];
      this.selectedServer = this.servers[0];
    }
  }

  ngOnDestroy(): void {
    if (this.handle) {
      clearInterval(this.handle);
    }
  }
  fetchMetrics() {
    this.metrics.getMetrics().then(data => {
      this.servers = Object.keys(data.clusterStats);
      this.selectedServer = data.nodeName;
      this.clusterStats = data.clusterStats;
      let gauges = data.clusterStats[this.selectedServer].gauges;
      this.currentStats = data.clusterStats[this.selectedServer];
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
