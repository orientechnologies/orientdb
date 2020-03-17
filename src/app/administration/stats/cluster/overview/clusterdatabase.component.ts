import {
  Component,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges
} from "@angular/core";
import {
  MetricService,
  WikiService,
  DistributedService,
  NotificationService
} from "../../../../core/services";

@Component({
  selector: "cluster-management-database",
  templateUrl: "./clusterdatabase.component.html",
  styles: [""]
})
class ClusterDatabaseComponent implements OnInit, OnChanges {
  @Input()
  private stats;
  handle: any;

  private databases: string[] = [];

  private database: string;

  private serverDatabases;

  private singleDBServers = [];

  private servers = [];

  private writeQuorum = "majority";

  private roles = ["MASTER", "REPLICA"];

  private quorum = ["majority", "all"];

  private wikiConfiguration;
  private databaseConfig;

  constructor(
    private metrics: MetricService,
    private wikiService: WikiService,
    private distributed: DistributedService,
    private notification: NotificationService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {}

  ngOnInit(): void {
    this.handle = setInterval(() => {
      this.fetchMetrics();
    }, 5000);
    this.fetchMetrics();

    this.wikiConfiguration = this.wikiService.resolveWiki(
      "Distributed-Configuration.html#default-distributed-db-configjson"
    );
  }

  ngOnDestroy(): void {
    clearInterval(this.handle);
  }

  changeDatabase() {
    this.fetchDatabaseConfig(this.database);
  }
  changeQuorum() {
    try {
      let parsed = parseInt(this.writeQuorum);
      if (isNaN(parsed)) {
        this.databaseConfig.writeQuorum = this.writeQuorum;
      } else {
        this.databaseConfig.writeQuorum = parsed;
      }
    } catch (e) {
      this.databaseConfig.writeQuorum = this.writeQuorum;
    }
  }

  fetchDatabaseConfig(name) {
    this.distributed.getDatabaseConfig(name).then(data => {
      this.databaseConfig = data;

      this.writeQuorum = this.databaseConfig.writeQuorum
        ? this.databaseConfig.writeQuorum.toString()
        : this.writeQuorum;
      let { quorums, servers } = this.calculateQuorumWitServers(
        name,
        this.serverDatabases[name] || [],
        this.stats.databasesStatus,
        this.databaseConfig
      );

      this.quorum = this.quorum.concat(quorums);
      this.singleDBServers = servers;
    });
  }
  saveConfiguration() {
    this.distributed
      .saveDatabaseConfig(this.database, this.databaseConfig)
      .then(() => {
        this.notification.push({
          content: "Distributed Configuration correctly saved.",
          autoHide: true
        });
      })
      .catch(err => {
        this.notification.push({
          content: err.data,
          error: true,
          autoHide: true
        });
      });
  }
  fetchMetrics() {
    this.metrics.getMetrics().then(data => {
      this.stats = data;
      if (this.stats.databasesStatus) {
        this.databases = Object.keys(this.stats.databasesStatus);
      }

      if (!this.database && this.databases.length > 0) {
        this.database = this.databases[0];
        this.fetchDatabaseConfig(this.database);
      }

      if (!this.serverDatabases) {
        this.serverDatabases = {};
        this.stats.members.forEach(s => {
          s.databases.forEach(db => {
            if (!this.serverDatabases[db]) {
              this.serverDatabases[db] = [];
            }
            this.serverDatabases[db].push(s);
          });
        });
      }
    });
  }

  calculateQuorumWitServers(name, servers, statuses, config) {
    let quorums = [];
    let uniqueServers = [];

    Object.keys(config.clusters).forEach(function(c) {
      if (config.clusters[c].servers) {
        config.clusters[c].servers.forEach(function(s) {
          if (uniqueServers.indexOf(s) == -1) {
            uniqueServers.push(s);
          }
        });
      }
    });

    uniqueServers = uniqueServers.filter(function(f) {
      var found = false;
      servers.forEach(function(s) {
        if (s.name === f) {
          found = true;
        }
      });
      return f != "<NEW_NODE>" && !found;
    });
    uniqueServers.forEach(function(s) {
      var status = "OFFLINE";

      if (statuses[name][s.name]) {
        status = statuses[name][s.name];
      }
      servers.push({ name: s, status: status });
    });

    servers.forEach(function(s, idx, arr) {
      if (statuses[name][s.name]) {
        s.status = statuses[name][s.name];
      }
      quorums.push((idx + 1).toString());
    });
    return { quorums, servers };
  }

  getOwnership(cluster, node) {
    let tmp = this.databaseConfig.clusters[cluster];
    if (!tmp.servers) return "";

    if (tmp.owner && tmp.owner != "") {
      return tmp.owner === node ? "X" : "o";
    }
    return tmp.servers.indexOf(node) == 0 ? "X" : "o";
  }
}

export { ClusterDatabaseComponent };
