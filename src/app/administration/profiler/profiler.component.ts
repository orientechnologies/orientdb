import { Component, OnInit } from "@angular/core";
import { AgentService, PermissionService } from "../../core/services";

@Component({
  selector: "profiler",
  templateUrl: "./profiler.component.html",
  styles: [""]
})
export class ProfilerComponent implements OnInit {
  private tab = "query";
  ee: boolean;

  constructor(private agent: AgentService) {}

  ngOnInit(): void {
    this.ee = this.agent.active;
  }
}
