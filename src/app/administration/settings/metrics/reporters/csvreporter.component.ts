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
  selector: "csv-reporter-settings",
  templateUrl: "./csvreporter.component.html",
  styles: [""]
})
export class CSVReporterComponent {
  @Input()
  config: any;


  @Input()
  private canEdit : boolean

  constructor() {}
}
