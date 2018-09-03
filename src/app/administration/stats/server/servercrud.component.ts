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
import { MetersAggregator } from "../../../util";

declare const c3: any;

@Component({
  selector: "server-crud",
  templateUrl: "./servercrud.component.html",
  styles: [""]
})
class ServerCrudComponent implements OnInit, OnChanges {
  @Input()
  private name;
  @Input()
  private stats;

  private columns = [];
  chart: any;

  private metrics = [
    { name: "db.*.readOps", regex: /db.*readOps/g },
    { name: "db.*.createOps", regex: /db.*createOps/g },
    { name: "db.*.updateOps", regex: /db.*updateOps/g },
    { name: "db.*.deleteOps", regex: /db.*deleteOps/g },
    { name: "db.*.commitOps", regex: /db.*commitOps/g },
    { name: "db.*.rollbackOps", regex: /db.*rollbackOps/g }
  ];

  aggregator: MetersAggregator;

  constructor(protected el: ElementRef) {
    this.aggregator = new MetersAggregator(this.metrics);
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;

    this.columns = this.aggregator.next(this.stats["meters"]);
  }

  ngOnInit(): void {
    this.columns = this.aggregator.next(this.stats["meters"]);
  }
}

export { ServerCrudComponent };
