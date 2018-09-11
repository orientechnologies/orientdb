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
import { AgentService } from "../../../core/services";
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
  constructor(
    private agent: AgentService,
    private securityService: SecurityService
  ) {}

  ngOnInit(): void {
    this.ee = this.agent.active;

    if (this.ee) {
      this.securityService.getConfig().then(config => {
        this.security = config;
      });
    }
  }

  ngOnDestroy(): void {}
}

export { SecurityManagerComponent };
