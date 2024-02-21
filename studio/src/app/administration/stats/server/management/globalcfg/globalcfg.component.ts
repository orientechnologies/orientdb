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

import { MetricService, AgentService } from "../../../../../core/services";

@Component({
  selector: "global-configuration",
  templateUrl: "./globalcfg.component.html",
  styles: [""]
})
class GlobalCFGComponent implements OnInit, OnChanges {
  @Input()
  private name;
  private fields = ["key", "description"];

  private searchText;

  properties: any[];
  globalProperties: any[];

  constructor(
    private metrics: MetricService,
    private agentService: AgentService
  ) {}

  ngOnInit() {}

  fetchProperties() {
    this.metrics.getInfo(this.agentService.active).then(data => {
      this.properties = data.properties;
      this.globalProperties = data.globalProperties;
    });
  }
  ngOnChanges(changes: SimpleChanges): void {
    if (changes.name.currentValue) {
      this.name = changes.name.currentValue;
      this.fetchProperties();
    }
  }
}

export { GlobalCFGComponent };
