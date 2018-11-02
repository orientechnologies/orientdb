import {
  Component,
  OnInit,
  ViewChild,
  ViewContainerRef,
  ComponentFactoryResolver,
  Injector,
  Input
} from "@angular/core";

@Component({
  selector: "metrics-list",
  templateUrl: "./metricslist.component.html",
  styles: [""]
})
export class MetricsListComponent implements OnInit {
  @Input()
  metrics: any[];

  fields = ["name", "description","unitOfMeasure"];

  constructor() {}

  ngOnInit(): void {}
}
