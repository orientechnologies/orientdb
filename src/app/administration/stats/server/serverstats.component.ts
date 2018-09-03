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

@Component({
  selector: "server-stats",
  templateUrl: "./serverstats.component.html",
  styles: [""]
})
class ServerStatsComponent implements OnInit, OnChanges {
  @Input()
  private name;
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

  constructor(private zone: NgZone) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;
    this.changeStats();
  }

  changeStats() {
    // CPU
    this.cpuPercent = (
      100 * parseFloat(this.stats["gauges"]["server.runtime.cpu"].value)
    ).toFixed(2);

    // DISK CACHE
    this.maxDiskCache = this.stats["gauges"][
      "server.runtime.diskCache.total"
    ].value;

    this.totalDiskCache = this.stats["gauges"][
      "server.runtime.diskCache.used"
    ].value;

    this.diskCachePercent = Math.floor(
      (this.totalDiskCache * 100) / this.maxDiskCache
    );

    // RAM

    this.maxMemory = this.stats["gauges"][
      "server.runtime.memory.heap.max"
    ].value;

    this.usedMemoy = this.stats["gauges"][
      "server.runtime.memory.heap.used"
    ].value;

    this.ramPercent = (
      100 * this.stats["gauges"]["server.runtime.memory.heap.usage"].value
    ).toFixed(2);

    // DISK

    let totalDisk = this.stats["gauges"]["server.disk.space.totalSpace"].value;
    let usableDisk = this.stats["gauges"]["server.disk.space.usableSpace"]
      .value;
    this.diskPercent = Math.floor(100 - (usableDisk * 100) / totalDisk);
  }
  ngOnInit(): void {
    this.changeStats();
  }
}

export { ServerStatsComponent };
