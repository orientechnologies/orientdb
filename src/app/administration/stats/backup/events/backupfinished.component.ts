import {
  Component,
  Input,
  ElementRef,
  NgZone,
  OnDestroy,
  ViewChild,
  OnInit,
  ViewContainerRef,
  Output,
  OnChanges,
  SimpleChange,
  SimpleChanges,
  EventEmitter
} from "@angular/core";
import { ModalComponent } from "ng2-bs3-modal";
import { BackupService, NotificationService } from "../../../../core/services";

@Component({
  selector: "backup-finished-event",
  templateUrl: "./backupfinished.component.html",
  styles: [""]
})
class BackupFinishedEvent implements OnInit, OnDestroy, OnChanges {
  @Input()
  private event: any = {};

  @Input()
  private backup;

  @Output()
  private onChange = new EventEmitter<any>();

  private restored: any = { log: {} };

  private unitLogs = [];

  @ViewChild("backupModal")
  backupModal: ModalComponent;

  @ViewChild("restoreBackupModal")
  restoreBackupModal: ModalComponent;

  @ViewChild("removeBackupModal")
  removeBackupModal: ModalComponent;

  constructor(
    private backupService: BackupService,
    private noti: NotificationService
  ) {}

  ngOnInit(): void {
    this.backupModal.open();
  }

  ngOnDestroy(): void {}

  onRestore() {
    this.backupService
      .restore(this.backup.uuid, this.restored)
      .then(() => {
        this.noti.push({
          content:
            "Restore procedure in progress into database " +
            this.restored.target,
          autoHide: true
        });
        this.restoreBackupModal.close();
        this.onChange.emit();
      })
      .catch(err => {
        this.noti.push({
          content: err.data,
          error: true,
          autoHide: true
        });
      });
  }
  onRemove() {
    this.backupService
      .remove(this.backup.uuid, this.restored)
      .then(() => {
        this.noti.push({
          content: "Backcup files removed",
          autoHide: true
        });
        this.removeBackupModal.close();
        this.onChange.emit();
      })
      .catch(err => {
        this.noti.push({
          content: err.data,
          error: true,
          autoHide: true
        });
      });
  }
  restoreDatabase() {
    this.backupModal.close();

    this.backupService
      .unitLogs(this.backup.uuid, this.event._source.unitId, {
        op: this.event._source.op
      })
      .then(response => {
        this.unitLogs = response.logs;
        this.restored.log = this.event._source;
        this.restored.unitId = this.event._source.unitId;
        this.restoreBackupModal.open();
      });
  }
  removeBackup() {
    this.backupModal.close();

    this.backupService
      .unitLogs(this.backup.uuid, this.event._source.unitId, {
        op: this.event._source.op
      })
      .then(response => {
        this.unitLogs = response.logs;
        this.restored.log = this.event._source;
        this.restored.unitId = this.event._source.unitId;
        this.removeBackupModal.open();
      });
  }
  ngOnChanges(changes: SimpleChanges): void {}

  fetchBackups() {}
}

export { BackupFinishedEvent };
