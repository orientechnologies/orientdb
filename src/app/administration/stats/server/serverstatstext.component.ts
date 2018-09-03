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
import { ValueAggObserbable } from "./valueAggregatorObservable";

@Component({
  selector: "server-stats-text",
  templateUrl: "./serverstatstext.component.html",
  styles: [""]
})
class ServerStatsText implements OnInit, OnChanges {
  @Input()
  private stats;
  sessions: any;
  // networkRequests: number = 0;

  status: any;

  // Network Requests
  networkRequests: ValueAggObserbable;
  networkRequestEmitter: EventEmitter<number> = new EventEmitter();

  // CRUD Operations
  operations: ValueAggObserbable;
  opsEmitter: EventEmitter<number> = new EventEmitter();

  constructor(private zone: NgZone) {
    this.operations = new ValueAggObserbable(this.opsEmitter);
    this.networkRequests = new ValueAggObserbable(this.networkRequestEmitter);
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;
    this.changeStats();
  }

  changeStats() {
    this.status = this.stats.status || "ONLINE";
    this.sessions = this.stats["gauges"]["server.network.sessions"].value;

    this.networkRequestEmitter.emit(
      this.stats["meters"]["server.network.requests"].count
    );

    this.opsEmitter.emit(this.countOps(this.stats["meters"]));
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
  ngOnInit(): void {
    this.changeStats();
  }
}

export { ServerStatsText };
