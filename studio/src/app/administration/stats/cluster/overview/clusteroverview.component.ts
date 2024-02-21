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
import { MetricService } from "../../../../core/services";

@Component({
  selector: "cluster-management-overview",
  templateUrl: "./clusteroverview.component.html",
  styles: [""]
})
class ClusterOverviewComponent implements OnInit, OnChanges {
  @Input()
  private stats;
  handle: any;
  messages: any[];
  members = [];
  totalMessages: {};
  totalMessagesServer: {};
  total: number;
  latenciesTotal: {};
  totalLatency: number;

  constructor(private metrics: MetricService) {}

  ngOnChanges(changes: SimpleChanges): void {}

  ngOnInit(): void {
    this.handle = setInterval(() => {
      this.fetchMetrics();
    }, 5000);
    this.fetchMetrics();
  }

  ngOnDestroy(): void {
    clearInterval(this.handle);
  }

  fetchMetrics() {
    this.metrics.getMetrics().then(data => {
      this.stats = data;
      this.refreshDistributedStats();
    });
  }

  refreshDistributedStats() {
    this.members = this.stats.members;

    let messages = [];

    let totalMessages = {};
    let totalMessagesServer = {};
    let total = 0;

    let totalLatency = 0;
    let latenciesTotal = {};

    this.members.forEach(function(m) {
      messages = messages.concat(
        Object.keys(m.messages).filter(function(k) {
          if (!totalMessages[k]) {
            totalMessages[k] = 0;
          }
          if (!totalMessagesServer[m.name]) {
            totalMessagesServer[m.name] = 0;
          }
          totalMessagesServer[m.name] += m.messages[k];
          total += m.messages[k];
          totalMessages[k] += m.messages[k];
          return messages.indexOf(k) == -1;
        })
      );
      if (!latenciesTotal[m.name]) {
        latenciesTotal[m.name] = {};
        latenciesTotal[m.name].in = 0;
        latenciesTotal[m.name].out = 0;
      }
      Object.keys(m.latencies).forEach(function(n) {
        if (!latenciesTotal[n]) {
          latenciesTotal[n] = {};
          latenciesTotal[n].in = 0;
          latenciesTotal[n].out = 0;
        }
        if (n !== m.name) {
          totalLatency += m.latencies[n].entries;
          latenciesTotal[m.name].out += m.latencies[n].entries;
          latenciesTotal[n].in += m.latencies[n].entries;
        }
      });
    });

    this.messages = messages;
    this.totalMessages = totalMessages;
    this.totalMessagesServer = totalMessagesServer;
    this.total = total;
    this.latenciesTotal = latenciesTotal;
    this.totalLatency = totalLatency;
  }
}

export { ClusterOverviewComponent };
