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

import { BackupService, NotificationService } from "../../../../core/services";
import { ModalComponent } from "ng2-bs3-modal";

@Component({
  selector: "backup-generic-event",
  templateUrl: "./genericevent.component.html",
  styles: [""]
})
class GenericBackupEvent implements OnInit, OnDestroy, OnChanges {
  @Input()
  private event: any = {};

  @ViewChild("eventModal")
  eventModal: ModalComponent;

  constructor(
    private backupService: BackupService,
    private noti: NotificationService
  ) {}

  ngOnInit(): void {
    this.eventModal.open();
  }

  ngOnDestroy(): void {}

  ngOnChanges(changes: SimpleChanges): void {}
}

export { GenericBackupEvent };
