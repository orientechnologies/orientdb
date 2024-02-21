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
  selector: "jmx-reporter-settings",
  templateUrl: "./jmxreporter.component.html",
  styles: [""]
})
export class JMXReporterComponent {
  @Input()
  config: any;


  @Input()
  private canEdit : boolean

  constructor() {}
}
