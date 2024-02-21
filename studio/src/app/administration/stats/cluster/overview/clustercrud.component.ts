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
import { MetersAggregator } from "../../../../util";

@Component({
  selector: "cluster-crud",
  templateUrl: "./clustercrud.component.html",
  styles: [""]
})
class ClusterCrudComponent implements OnInit, OnChanges {
  @Input()
  private name;
  @Input()
  private stats;

  private columns = [];
  chart: any;

  private metrics = [
    { name: "db.*.readOps", label: "Read", regex: /db.*readOps/g },
    { name: "db.*.createOps", label: "Create", regex: /db.*createOps/g },
    { name: "db.*.updateOps", label: "Update", regex: /db.*updateOps/g },
    { name: "db.*.deleteOps", label: "Delete", regex: /db.*deleteOps/g },
    { name: "db.*.commitOps", label: "Commit", regex: /db.*commitOps/g },
    { name: "db.*.rollbackOps", label: "Rollback", regex: /db.*rollbackOps/g }
  ];

  aggregator: MetersAggregator;

  constructor(protected el: ElementRef) {
    this.aggregator = new MetersAggregator(this.metrics);
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;

    if (this.stats) {
      let aggregates = this.createAggregates();

      this.columns = this.aggregator.next(aggregates);
    }
  }

  private createAggregates() {
    let servers = Object.keys(this.stats.clusterStats);
    let aggregates = servers
      .map(s => {
        return this.stats.clusterStats[s]["meters"];
      })
      .reduce((prev, current) => {
        if (Object.keys(prev).length == 0) {
          return {
            ...current
          };
        } else {
          Object.keys(current).forEach(k => {
            if (!prev[k]) {
              prev[k] = {
                ...current[k]
              };
            } else {
              prev[k].count += current[k].count;
            }
          });
          return prev;
        }
      }, {});

    return aggregates;
  }

  ngOnInit(): void {
    if (this.stats) {
      let aggregates = this.createAggregates();
      this.columns = this.aggregator.next(aggregates);
    }
  }
}

export { ClusterCrudComponent };
