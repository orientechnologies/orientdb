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
  selector: "server-management-overview",
  templateUrl: "./serveroverview.component.html",
  styles: [""]
})
class ServerManagementOverviewComponent implements OnInit, OnChanges {
  @Input()
  private name;
  @Input()
  private stats;
  private ee = true;

  
  constructor(private agent: AgentService) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.stats = changes.stats.currentValue;
  }

  ngOnInit(): void {
    this.ee = this.agent.active;
  }

  ngOnDestroy(): void {}
}

export { ServerManagementOverviewComponent };
