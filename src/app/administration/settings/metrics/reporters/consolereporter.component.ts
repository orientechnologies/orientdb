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
  selector: "console-reporter-settings",
  templateUrl: "./consolereporter.component.html",
  styles: [""]
})
export class ConsoleReporterComponent {
  @Input()
  config: any;

  constructor() {}
}
