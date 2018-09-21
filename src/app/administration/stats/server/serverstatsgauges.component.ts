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
import { MetricService } from "../../../core/services";

@Component({
  selector: "server-stats-gauges",
  templateUrl: "./serverstatsgauges.component.html",
  styles: [""]
})
class ServerStatsGauges implements OnInit, OnChanges {
  @Input()
  private stats;

  private cpuPercent;
  maxDiskCache: any;
  totalDiskCache: any;
  diskCachePercent: number;
  maxMemory: any;
  usedMemoy: any;
  ramPercent: string;
  diskPercent: number;

  constructor(private zone: NgZone, private metrics: MetricService) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;
    this.changeStats();
  }

  changeStats() {
    // CPU

    let {
      cpuPercent,
      maxDiskCache,
      totalDiskCache,
      diskCachePercent,
      maxMemory,
      usedMemoy,
      ramPercent,
      diskPercent
    } = this.metrics.calculateGauges(this.stats);

    this.cpuPercent = cpuPercent;
    this.maxDiskCache = maxDiskCache;
    this.totalDiskCache = totalDiskCache;
    this.diskCachePercent = diskCachePercent;
    this.maxMemory = maxMemory;
    this.usedMemoy = usedMemoy;
    this.ramPercent = ramPercent;
    this.diskPercent = diskPercent;
  }
  ngOnInit(): void {
    this.changeStats();
  }
}

export { ServerStatsGauges };
