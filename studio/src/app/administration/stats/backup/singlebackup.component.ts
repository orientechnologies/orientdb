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
  SimpleChange,
  SimpleChanges,
  ComponentFactoryResolver,
  Injector
} from "@angular/core";
import {
  AgentService,
  BackupService,
  NotificationService,
  PermissionService
} from "../../../core/services";
import { ModalComponent } from "ng2-bs3-modal";
import { BackupFinishedEvent, GenericBackupEvent } from "./events";

declare const angular: any;

const subComponents = {
  BACKUP_FINISHED: BackupFinishedEvent
};
@Component({
  selector: "single-backup-management",
  templateUrl: "./singlebackup.component.html",
  styles: [""]
})
class SingleBackupComponent implements OnInit, OnDestroy, OnChanges {
  private ee = true;
  @Input()
  private backup: any;

  private canEdit = false;

  @ViewChild("backupModal", { read: ViewContainerRef })
  dashboardContainer: ViewContainerRef;

  private eventsType = [
    {
      name: "Backup Finished",
      type: "BACKUP_FINISHED",
      clazz: "log-finished-icon"
    },
    {
      name: "Restore Finished",
      type: "RESTORE_FINISHED",
      clazz: "log-restore-finished-icon"
    },
    {
      name: "Backup Scheduled",
      type: "BACKUP_SCHEDULED",
      clazz: "log-scheduled-icon"
    },
    {
      name: "Backup Started",
      type: "BACKUP_STARTED",
      clazz: "log-started-icon"
    },
    {
      name: "Restore Started",
      type: "RESTORE_STARTED",
      clazz: "log-restore-started-icon"
    },
    { name: "Backup Error", type: "BACKUP_ERROR", clazz: "log-error-icon" },
    { name: "Restore Error", type: "RESTORE_ERROR", clazz: "log-error-icon" }
  ];
  private selectedEvents = [
    "BACKUP_FINISHED",
    "BACKUP_ERROR",
    "RESTORE_FINISHED"
  ];
  private mode = "2";
  private modes = {
    "1": "Incremental Backup",
    "2": "Full Backup",
    "3": "Full + Incremental Backup"
  };
  private range: any;
  private logs = [];
  private currentUnitLogs = [];

  constructor(
    private agent: AgentService,
    private backupService: BackupService,
    private notification: NotificationService,
    private resolver: ComponentFactoryResolver,
    private injector: Injector,
    private permissionService: PermissionService
  ) {}

  createComponent(component, event, backup) {
    this.dashboardContainer.clear();
    if (component) {
      const factory = this.resolver.resolveComponentFactory(component);
      let componentRef = this.dashboardContainer.createComponent(
        factory,
        0,
        this.injector
      );
      this.bind(componentRef.instance, "event", event);
      this.bind(componentRef.instance, "backup", this.backup);
      this.bind(componentRef.instance, "canEdit", this.canEdit);

      this.subscribeOnChange(componentRef.instance);
    }
  }

  subscribeOnChange(component) {
    if (component.onChange) {
      component.onChange.subscribe(() => {
        this.fetchBackups(this.backup.uuid, this.range);
      });
    }
  }
  bind(component, name, event) {
    component[name] = event;
  }
  ngOnInit(): void {
    this.ee = this.agent.active;
    this.canEdit = this.permissionService.isAllow("server.backup.edit");

      if(this.backup.modes["INCREMENTAL_BACKUP"] && this.backup.modes["FULL_BACKUP"]){
          this.mode = "3";
        } else if(this.backup.modes["INCREMENTAL_BACKUP"]){
          this.mode = "1";
        } else {
          this.mode = "2";
        }
  }

  ngOnChanges(changes: SimpleChanges) {
    if (this.range && changes.backup.currentValue) {
      this.fetchBackups(changes.backup.currentValue.uuid, this.range);
    }
  }

  onChange(mode, event) {
    let key = mode === "1" ? "INCREMENTAL_BACKUP" : "FULL_BACKUP";
    if (!this.backup.modes[key]) {
      this.backup.modes[key] = { when: event };
    }
    this.backup.modes[key].when = event;
  }

  changeRange(range) {
    this.range = range;

    this.fetchBackups(this.backup.uuid, this.range);
  }

  selectEvent(event) {
    let cmp = subComponents[event._source.op];
    if (!cmp) {
      cmp = GenericBackupEvent;
    }
    this.createComponent(cmp, event, this.backup);
  }
  saveBackup() {
    this.backupService
      .save(this.backup)
      .then(bck => {
        this.backup = bck;
        this.notification.push({ content: "Backup saved", autoHide: true });
      })
      .catch(err => {
        this.notification.push({
          content: err,
          error: true,
          autoHide: true
        });
      });
  }
  changeMode(mode) {
    if (mode) {
      switch (mode) {
        case "1":
          if (!this.backup.modes["INCREMENTAL_BACKUP"]) {
            this.backup.modes["INCREMENTAL_BACKUP"] = {
              when: "0 0/1 * * * ?"
            };
          }
          delete this.backup.modes["FULL_BACKUP"];
          break;
        case "2":
          if (!this.backup.modes["FULL_BACKUP"]) {
            this.backup.modes["FULL_BACKUP"] = { when: "0 0/1 * * * ?" };
          }
          delete this.backup.modes["INCREMENTAL_BACKUP"];
          break;
        case "3":
          if (!this.backup.modes["FULL_BACKUP"]) {
            this.backup.modes["FULL_BACKUP"] = { when: "0 0/1 * * * ?" };
          }
          if (!this.backup.modes["INCREMENTAL_BACKUP"]) {
            this.backup.modes["INCREMENTAL_BACKUP"] = {
              when: "0 0/1 * * * ?"
            };
          }
          break;
      }
    }
  }
  onEventClick(t) {
    var idx = this.selectedEvents.indexOf(t.type);
    if (idx == -1) {
      this.selectedEvents.push(t.type);
    } else {
      this.selectedEvents.splice(idx, 1);
    }
    this.currentUnitLogs = formatLogs(this.logs, this.selectedEvents);
  }
  getClazz(t) {
    return (
      (this.selectedEvents.indexOf(t.type) == -1
        ? "fa-circle-thin "
        : "fa-circle ") + t.clazz
    );
  }

  ngOnDestroy(): void {}
  fetchBackups(id, params) {
    this.backupService.logs(id, params).then(data => {
      this.logs = data.logs;
      this.currentUnitLogs = formatLogs(data.logs, this.selectedEvents);
    });
  }
}

function formatLogs(logs, selectedEvents) {
  return logs
    .filter(e => {
      return selectedEvents.indexOf(e.op) != -1;
    })
    .map((e, idx, arr) => {
      var date = new Date(e.timestamp);
      return {
        id: idx,
        title: info(e),
        _source: e,
        _template: "views/server/backup/" + e.op.toLowerCase() + ".html",
        start: date,
        end: date,
        className: clazz(e)
      };
    });
}

function clazz(event) {
  let clazz = "basic-log";
  switch (event.op) {
    case "BACKUP_FINISHED":
      clazz += " log-finished";
      break;
    case "BACKUP_SCHEDULED":
      clazz += " log-scheduled";
      break;
    case "BACKUP_STARTED":
      clazz += " log-started";
      break;
    case "BACKUP_ERROR":
      clazz += " log-error";
      break;
    case "RESTORE_FINISHED":
      clazz += " log-restore-finished";
      break;
    case "RESTORE_STARTED":
      clazz += " log-restore-started";
      break;
    case "RESTORE_ERROR":
      clazz += " log-error";
      break;
  }
  return clazz;
}

function info(event) {
  var info = modeToString(event.mode);
  switch (event.op) {
    case "BACKUP_FINISHED":
      info += " executed";
      break;
    case "BACKUP_ERROR":
      info += " error";
      break;
    case "BACKUP_SCHEDULED":
      info += " scheduled.";
      break;
    case "BACKUP_STARTED":
      info += " started";
      break;
    case "RESTORE_FINISHED":
      info = "Restore finished";
      break;
    case "RESTORE_STARTED":
      info = "Restore started";
      break;
    case "RESTORE_ERROR":
      info = "Restore error";
      break;
  }
  return info;
}

function modeToString(mode) {
  switch (mode) {
    case "INCREMENTAL_BACKUP":
      return "Incremental backup";
    case "FULL_BACKUP":
      return "Full backup";
  }
}
export { SingleBackupComponent };
