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
import {
  AgentService,
  BackupService,
  MetricService
} from "../../../core/services";

declare const angular: any;

@Component({
  selector: "backup-management",
  templateUrl: "./backup.component.html",
  styles: [""]
})
class BackupComponent implements OnInit, OnDestroy {
  private ee = true;

  private db: string;
  private backup: any;
  private backups: any[];
  private databases: string[];

  constructor(
    private agent: AgentService,
    private backupService: BackupService,
    private metrics: MetricService
  ) {}

  onChange() {
    this.backup = this.backups[this.db] || {
      dbName: this.db,
      enabled : true,
      retentionDays : -1,
      modes: { FULL_BACKUP: { when: "0 0/10 * * * ?" } }
    };
  }
  ngOnInit(): void {
    this.ee = this.agent.active;

    this.metrics.listDatabases().then(({ databases }) => {
      this.backupService.getConfig().then(response => {
        this.backups = response.backups.reduce((acc, current) => {
          acc[current.dbName] = current;
          return acc;
        }, {});
        let backupDb = response.backups
          .filter(b => databases.indexOf(b.dbName) === -1)
          .map(b => b.dbName);
        this.databases = [...databases, ...backupDb];
        this.db = this.databases.length > 0 ? this.databases[0] : null;
        this.backup = this.backups[this.db] || {
          dbName: this.db,
          modes: { FULL_BACKUP: { when: "0 0/10 * * * ?" } }
        };
      });
    });
  }

  ngOnDestroy(): void {}
  fetchBackups() {}
}

export { BackupComponent };
