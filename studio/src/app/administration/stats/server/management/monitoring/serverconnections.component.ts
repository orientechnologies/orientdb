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
  selector: "server-management-connections",
  templateUrl: "./serverconnections.component.html",
  styles: [""]
})
class ServerConnectionsComponent implements OnInit, OnChanges {
  @Input()
  private name;
  private connections = [];
  handle: any;

  private fields = ["db", "commandInfo", "protocol"];

  private searchText;

  constructor(
    private metrics: MetricService,
    private agentService: AgentService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {}

  ngOnInit(): void {
    this.handle = setInterval(() => {
      this.fetchConnections();
    }, 5000);
    this.fetchConnections();
  }

  fetchConnections() {
    this.metrics.getInfo(this.agentService.active).then(data => {
      this.connections = data.connections;
    });
  }
  ngOnDestroy(): void {
    clearInterval(this.handle);
  }
}

export { ServerConnectionsComponent };
