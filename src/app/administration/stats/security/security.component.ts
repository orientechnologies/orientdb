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
import { AgentService, MetricService } from "../../../core/services";
import { SecurityService } from "../../../core/services/security.service";

declare const angular: any;

@Component({
  selector: "security-management",
  templateUrl: "./security.component.html",
  styles: [""]
})
class SecurityManagerComponent implements OnInit, OnDestroy {
  ee: boolean;
  security: any;
  tab = "auditing";
  private databases: string[];
  constructor(
    private agent: AgentService,
    private securityService: SecurityService,
    private metrics: MetricService
  ) {}

  ngOnInit(): void {
    this.ee = this.agent.active;

    if (this.ee) {
      Promise.all([
        this.metrics.listDatabases(),
        this.securityService.getConfig()
      ]).then(response => {
        this.databases = response[0].databases;
        this.security = response[1];
      });
    }
  }

  ngOnDestroy(): void {}
}

export { SecurityManagerComponent };
