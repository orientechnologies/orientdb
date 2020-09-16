import {
  Component,
  Input,
  ElementRef,
  NgZone,
  OnDestroy,
  ViewChild,
  OnInit,
  ViewContainerRef,
  ComponentFactoryResolver,
  ComponentFactory,
  ComponentRef,
  Injector
} from "@angular/core";
import { downgradeComponent } from "@angular/upgrade/static";
import { PermissionService, WikiService } from "../core/services";
import {
  DashboardStatsComponent,
  ServerManagementComponent,
  ClusterManagementComponent,
  BackupComponent
} from "./stats";
import { ImportManagerComponent } from "./importmanager";
import { SecurityManagerComponent } from "./stats/security";
import { StudioSettingsComponent } from "./settings";
import { ProfilerComponent } from "./profiler";
import { ServerCommandsComponent } from "./servercommands";

declare const angular: any;

const subComponents = {
  stats: DashboardStatsComponent,
  general: ServerManagementComponent,
  cluster: ClusterManagementComponent,
  backup: BackupComponent,
  importers: ImportManagerComponent,
  security: SecurityManagerComponent,
  settings: StudioSettingsComponent,
  servercommands: ServerCommandsComponent,
  profiler: ProfilerComponent
};

@Component({
  selector: "dashboard",
  templateUrl: "./dashboard.component.html",
  styles: [""]
})
class DashboardComponent implements OnInit {
  @Input()
  protected tab: string;
  protected title: string;
  protected wiki: string;

  @ViewChild("dashboardContainer", { read: ViewContainerRef })
  dashboardContainer: ViewContainerRef;

  constructor(
    private permission: PermissionService,
    private resolver: ComponentFactoryResolver,
    private injector: Injector,
    private wikiService: WikiService
  ) {}

  ngOnInit(): void {
    let cmp = this.permission.getCurrentComponent(this.tab);
    if (cmp && subComponents[cmp.name]) {
      this.title = cmp.title;

      this.wiki = this.wikiService.resolveWiki(cmp.wiki);
      this.createComponent(subComponents[cmp.name]);
    }
  }

  createComponent(component) {
    this.dashboardContainer.clear();
    if (component) {
      const factory = this.resolver.resolveComponentFactory(component);
      let componentRef = this.dashboardContainer.createComponent(
        factory,
        0,
        this.injector
      );
    }
  }
}

angular
  .module("dashboard.components", [])
  .directive(
    `dashboard`,
    downgradeComponent({ component: DashboardComponent, inputs: ["tab"] })
  );

export { DashboardComponent };
