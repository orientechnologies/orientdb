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
  SimpleChanges,
  EventEmitter
} from "@angular/core";
import { downgradeComponent } from "@angular/upgrade/static";
import { MetricService } from "../../../../core/services";
import { ValueAggObserbable } from "../../server/valueAggregatorObservable";

@Component({
  selector: "cluster-stats",
  templateUrl: "./clusterstats.component.html",
  styles: [""]
})
class ClusterStatsComponent implements OnInit, OnChanges {
  @Input()
  private stats;
  private servers = [];

  // Network Requests
  networkRequests: ValueAggObserbable;
  networkRequestEmitter: EventEmitter<number> = new EventEmitter();

  // CRUD Operations
  operations: ValueAggObserbable;
  opsEmitter: EventEmitter<number> = new EventEmitter();

  private cpuPercent;
  maxDiskCache: any;
  totalDiskCache: any;
  diskCachePercent: number;
  maxMemory: any;
  usedMemoy: any;
  ramPercent: string;
  diskPercent: number;
  status: string;
  sessions: any;
  constructor(private metrics: MetricService) {
    this.operations = new ValueAggObserbable(this.opsEmitter);
    this.networkRequests = new ValueAggObserbable(this.networkRequestEmitter);
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;

    if (this.stats) {
      this.servers = Object.keys(this.stats.clusterStats);

      let stats: any = this.servers
        .map(s => {
          return this.metrics.calculateGauges(this.stats.clusterStats[s]);
        })
        .reduce(
          (prev, current) => {
            return Object.keys(prev).reduce((p, k) => {
              p[k] = prev[k] + current[k];
              return p;
            }, {});
          },
          {
            maxDiskCache: 0,
            totalDiskCache: 0,
            maxMemory: 0,
            usedMemoy: 0,
            totalDisk: 0,
            usableDisk: 0
          }
        );

      this.cpuPercent = (100 * (stats.cpuValue / this.servers.length)).toFixed(
        2
      );
      this.maxDiskCache = stats.maxDiskCache;
      this.totalDiskCache = stats.totalDiskCache;
      this.diskCachePercent = Math.floor(
        (stats.totalDiskCache * 100) / stats.maxDiskCache
      );
      this.maxMemory = stats.maxMemory;
      this.usedMemoy = stats.usedMemoy;
      this.ramPercent = (100 * (stats.ramUsage / this.servers.length)).toFixed(
        2
      );
      this.diskPercent = Math.floor(
        100 - (stats.usableDisk * 100) / stats.totalDisk
      );

      this.status = "ONLINE";
      this.sessions = this.servers.reduce((prev, current) => {
        return (
          prev +
          this.stats.clusterStats[current]["gauges"]["server.network.sessions"]
            .value
        );
      }, 0);

      this.networkRequestEmitter.emit(
        this.servers.reduce((prev, current) => {
          return (
            prev +
            this.stats.clusterStats[current]["meters"][
              "server.network.requests"
            ].count
          );
        }, 0)
      );

      this.opsEmitter.emit(
        this.servers.reduce((prev, current) => {
          return (
            prev + this.countOps(this.stats.clusterStats[current]["meters"])
          );
        }, 0)
      );
    }
  }

  countOps(meters) {
    return Object.keys(meters)
      .filter(k => {
        return k.match(/db.*Ops/g) != null;
      })
      .map(k => {
        return meters[k].count;
      })
      .reduce((a, b) => {
        return a + b;
      }, 0);
  }
  ngOnInit(): void {}
}

export { ClusterStatsComponent };
