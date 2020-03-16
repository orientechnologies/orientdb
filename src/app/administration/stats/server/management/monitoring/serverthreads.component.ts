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
  selector: "server-management-thread-dump",
  templateUrl: "./serverthreads.component.html",
  styles: [""]
})
class ServerThreadDumpComponent implements OnInit, OnChanges {
  @Input()
  private name;

  private threadDump = "";
  dumpDate: Date;

  private editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    readOnly: true
  };
  ee: boolean;
  constructor(
    private metrics: MetricService,
    private agentService: AgentService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {}

  ngOnInit(): void {
    this.ee = this.agentService.active;
    this.fetchDump();
  }

  fetchDump() {
    if (this.ee) {
      this.metrics.threadDumps().then(data => {
        this.dumpDate = new Date();
        this.threadDump = data.threadDump;
      });
    }
  }
  ngOnDestroy(): void {}
}

export { ServerThreadDumpComponent };
