import { Component, OnInit } from "@angular/core";
import { AgentService, PermissionService } from "../../../core/services";

@Component({
  selector: "permission-settings",
  templateUrl: "./permissionsettings.component.html",
  styles: [""]
})
export class PermissionSettingsComponent implements OnInit {
  permissions: any[] = [];

  constructor(private permissionService: PermissionService) {}

  ngOnInit(): void {
    this.permissionService.allPermissions().then(response => {
      let permissions = response.permissions;
      this.permissions = Object.keys(permissions).map(p => {
        return { name: p, description: permissions[p] };
      });
    });
  }
}
