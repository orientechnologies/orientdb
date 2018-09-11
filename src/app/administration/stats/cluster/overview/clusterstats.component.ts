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
  selector: "cluster-stats",
  templateUrl: "./clusterstats.component.html",
  styles: [""]
})
class ClusterStatsComponent implements OnInit, OnChanges {
  @Input()
  private stats;
  private servers = [];
  constructor() {}

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;

    if (this.stats) {
      this.servers = Object.keys(this.stats.clusterStats);
    }
  }

  ngOnInit(): void {}
}

export { ClusterStatsComponent };
