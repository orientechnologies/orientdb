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
import { MetricService, AgentService } from "../../../../../core/services";

@Component({
  selector: "server-management-monitoring",
  templateUrl: "./servermonitoring.component.html",
  styles: [""]
})
class ServerManagementMonitoringComponent implements OnInit, OnChanges {
  @Input()
  private name;
  private connections = [];
  private tab = "connections";

  constructor(private metrics: MetricService) {}

  ngOnChanges(changes: SimpleChanges): void {}

  ngOnInit(): void {
    
  }

  ngOnDestroy(): void {}
}

export { ServerManagementMonitoringComponent };
