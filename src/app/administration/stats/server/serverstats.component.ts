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
  }

  ngOnInit(): void {}
}

export { ServerStatsComponent };
