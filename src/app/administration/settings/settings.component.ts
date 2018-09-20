import { Component, OnInit } from "@angular/core";
import { AgentService, PermissionService } from "../../core/services";

@Component({
  selector: "studio-settings",
  templateUrl: "./settings.component.html",
  styles: [""]
})
export class StudioSettingsComponent implements OnInit {
  private tab = "metrics";
  ee: boolean;
  canViewMetrics: boolean = false;
  canEditMetrics: boolean = false;

  constructor(
    private agent: AgentService,
    private permissions: PermissionService
  ) {}

  ngOnInit(): void {
    this.ee = this.agent.active;
    this.canViewMetrics = this.permissions.isAllow("server.metrics");
    this.canEditMetrics = this.permissions.isAllow("server.metrics.edit");
  }
}
