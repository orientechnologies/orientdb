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
  selector: "server-metrics",
  templateUrl: "./servermetrics.component.html",
  styles: [""]
})
class ServerMetricsComponent implements OnInit, OnChanges {
  @Input()
  private name;
  @Input()
  private stats;

  constructor(private zone: NgZone) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;
  }

  ngOnInit(): void {}
}

export { ServerMetricsComponent };
