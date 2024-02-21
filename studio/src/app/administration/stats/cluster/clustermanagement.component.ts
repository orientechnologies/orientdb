  import {
  Component,
  Input,
  ElementRef,
  NgZone,
  OnDestroy,
  ViewChild,
  OnInit,
  ViewContainerRef
} from "@angular/core";
import { MetricService, AgentService } from "../../../core/services";

declare const angular: any;

@Component({
  selector: "cluster-management",
  templateUrl: "./clustermanagement.component.html",
  styles: [""]
})
class ClusterManagementComponent implements OnInit, OnDestroy {
  private tab = "overview";
  private handle;
  constructor(private metrics: MetricService, private agent: AgentService) {}

  private ee = true;
  private distributed = true;

  ngOnInit(): void {
    this.ee = this.agent.active;

    this.metrics.getMetrics().then(response => {
      this.distributed = response.distributed;
    });
  }

  ngOnDestroy(): void {}
}

export { ClusterManagementComponent };
