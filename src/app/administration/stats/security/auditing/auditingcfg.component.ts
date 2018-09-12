import { Component, OnDestroy, OnInit, Input, ViewChild } from "@angular/core";
import {
  AuditingService,
  DBService,
  SecurityService,
  NotificationService
} from "../../../../core/services";
import { ModalComponent } from "ng2-bs3-modal";

declare const angular: any;

@Component({
  selector: "auditing-cfg-management",
  templateUrl: "./auditingcfg.component.html",
  styles: [""]
})
class AuditingConfigComponent implements OnInit, OnDestroy {
  @Input()
  private security: any;

  private globalAuditing: any;

  private database: string;

  private config: any;

  @ViewChild("addClassModal")
  backupModal: ModalComponent;

  @Input()
  private databases: string[];

  private classes: any[] = [];

  constructor(
    private auditingService: AuditingService,
    private dbService: DBService,
    private securityService: SecurityService,
    private noti: NotificationService
  ) {}

  ngOnInit(): void {
    this.globalAuditing = this.security.auditing;

    if (this.databases.length > 0) {
      this.database = this.databases[0];
      this.loadConfig(this.database);
    }
  }

  onDatabaseChange() {
    this.loadConfig(this.database);
  }

  saveAuditing() {
    this.securityService
      .reload({
        module: "auditing",
        config: this.globalAuditing
      })
      .then(() => {
        return this.auditingService.saveConfig(this.database, this.config);
      })
      .then(() => {
        this.noti.push({
          content: "Auditing configuration saved correctly.",
          autoHide: true
        });
      });
  }

  addClass() {
    this.dbService.listClasses(this.database).then(cls => {
      this.backupModal.open();
      this.classes = cls;
    });
  }

  confirmClass(cls) {
    this.backupModal.close();
    let newClasses = Object.assign({}, this.config.classes);
    newClasses[cls] = {
      polymorphic: true,
      onCreateEnabled: false,
      onCreateMessage: "",
      onReadEnabled: false,
      onReadMessage: "",
      onUpdateEnabled: false,
      onUpdateMessage: "",
      onDeleteEnabled: false,
      onDeleteMessage: ""
    };

    this.config.classes = newClasses;
  }

  deleteClass(cls) {
    let newClasses = Object.assign({}, this.config.classes);
    delete newClasses[cls];
    this.config.classes = newClasses;
  }
  addCommand() {
    if (!this.config.commands) {
      this.config.commands = [];
    }
    this.config.commands.push({
      regex: "",
      message: ""
    });
  }
  loadConfig(db) {
    this.auditingService.getConfig(db).then(config => {
      if (!config.schema) {
        config.schema = {};
      }
      this.config = config;
    });
  }

  ngOnDestroy(): void {}
}

export { AuditingConfigComponent };
