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

declare const angular: any;

@Component({
  selector: "single-backup-management",
  templateUrl: "./singlebackup.component.html",
  styles: [""]
})
class SingleBackupComponent implements OnInit, OnDestroy {
  private ee = true;
  @Input()
  private backup: string;

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

  constructor(private agent: AgentService) {}

  ngOnInit(): void {
    this.ee = this.agent.active;
  }

  onChange(mode, event) {
    console.log(mode, event);
  }
  onEventClick(t) {
    var idx = this.selectedEvents.indexOf(t.type);
    if (idx == -1) {
      this.selectedEvents.push(t.type);
    } else {
      this.selectedEvents.splice(idx, 1);
    }
    // $scope.refreshEvents();
  }
  getClazz(t) {
    return (
      (this.selectedEvents.indexOf(t.type) == -1
        ? "fa-circle-thin "
        : "fa-circle ") + t.clazz
    );
  }

  ngOnDestroy(): void {}
  fetchBackups() {}
}

export { SingleBackupComponent };
