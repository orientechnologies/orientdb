import {
  Component,
  OnInit,
  ViewChild,
  ViewContainerRef,
  ComponentFactoryResolver,
  Injector,
  Input
} from "@angular/core";
import { MetricService, NotificationService } from "../../../core/services";
import {
  JMXReporterComponent,
  CSVReporterComponent,
  ConsoleReporterComponent
} from "./reporters";

const subComponents = {
  jmx: JMXReporterComponent,
  csv: CSVReporterComponent,
  console: ConsoleReporterComponent
};
@Component({
  selector: "metrics-settings",
  templateUrl: "./metricsettings.component.html",
  styles: [""]
})
export class MetricsSettingsComponent implements OnInit {
  config: any;
  currentSelected: any;
  currentSelectedName = "jmx";

  @Input()
  private canEdit: boolean;

  @ViewChild("reporterContainer", { read: ViewContainerRef })
  reporterContainer: ViewContainerRef;
  metrics: { name: string; description: string }[];

  constructor(
    private metricService: MetricService,
    private resolver: ComponentFactoryResolver,
    private injector: Injector,
    private noti: NotificationService
  ) {}

  ngOnInit(): void {
    this.metricService.getConfig().then(config => {
      this.config = config;
      this.currentSelected = config.reporters[this.currentSelectedName];
      this.selectReporter(this.currentSelectedName);
    });

    this.metricService.list().then(result => {
      this.metrics = result.metrics.sort((a, b) => {
        if (a.name > b.name) {
          return 1;
        }
        if (a.name < b.name) {
          return -1;
        }
        return 0;
      });
    });
  }

  saveMetrics() {
    this.metricService
      .saveConfig(this.config)
      .then(() => {
        this.noti.push({
          content: "Metrics configuration reloaded.",
          autoHide: true
        });
      })
      .catch(err => {
        this.noti.push({ content: err.json(), error: true, autoHide: true });
      });
  }

  selectReporter(reporter) {
    this.currentSelectedName = reporter;
    this.currentSelected = this.config.reporters[reporter];

    let cmp = subComponents[reporter];

    if (cmp) {
      this.createComponent(cmp);
    }
  }

  createComponent(component) {
    this.reporterContainer.clear();
    if (component) {
      const factory = this.resolver.resolveComponentFactory(component);
      let componentRef = this.reporterContainer.createComponent(
        factory,
        0,
        this.injector
      );
      this.bindProperty(componentRef.instance, "config", this.currentSelected);
      this.bindProperty(componentRef.instance, "canEdit", this.canEdit);
    }
  }

  hasConfig(name) {
    return subComponents[name];
  }
  bindProperty(instance, name, property) {
    instance[name] = property;
  }
}
