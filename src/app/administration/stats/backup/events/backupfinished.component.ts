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
import { BackupService } from "../../../../core/services";

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

  private restoring = false;

  private restored = {};

  @ViewChild("backupModal")
  backupModal: ModalComponent;

  @ViewChild("restoreBackupModal")
  restoreBackupModal: ModalComponent;

  @ViewChild("removeBackupModal")
  removeBackupModal: ModalComponent;

  constructor(private backupService: BackupService) {}

  ngOnInit(): void {
    this.backupModal.open();
  }

  ngOnDestroy(): void {}

  restoreBackup() {
    this.backupModal.close();

    this.backupService
      .unitLogs(this.backup.uuid, this.event._source.unitId, {
        op: this.event._source.op
      })
      .then(logs => {
        console.log(logs);
        this.restoreBackupModal.open();
      });
  }
  removeBackup() {}
  ngOnChanges(changes: SimpleChanges): void {}

  fetchBackups() {}
}

export { BackupFinishedEvent };
