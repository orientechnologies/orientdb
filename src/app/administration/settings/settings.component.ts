import { Component, OnInit } from "@angular/core";
import { AgentService } from "../../core/services";

@Component({
  selector: "studio-settings",
  templateUrl: "./settings.component.html",
  styles: [""]
})
export class StudioSettingsComponent implements OnInit {
  private tab = "metrics";
  ee: boolean;

  constructor(private agent: AgentService) {}

  ngOnInit(): void {
    this.ee = this.agent.active;
  }
}
