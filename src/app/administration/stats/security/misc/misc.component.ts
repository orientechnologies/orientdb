import { Component, Input, OnInit } from "@angular/core";
import {
  SecurityService,
  NotificationService
} from "../../../../core/services";

@Component({
  selector: "security-misc-management",
  templateUrl: "./misc.component.html",
  styles: [""]
})
export class SecurityMiscComponent implements OnInit {
  @Input()
  private security: any;

  passwordValidator: any;
  serverSecurity: any;

  constructor(
    private securityService: SecurityService,
    private noti: NotificationService
  ) {}

  ngOnInit(): void {
    this.serverSecurity = this.security.server;
    this.passwordValidator = this.security.passwordValidator || {
      class:
        "com.orientechnologies.security.password.ODefaultPasswordValidator",
      minimumLength: 5,
      numberRegEx: "(?:[0-9].*){2}",
      uppercaseRegEx: "(?:[A-Z].*){3}",
      specialRegEx: "(?:[^a-zA-Z0-9 ].*){2}",
      enabled: false
    };
  }

  saveServerSecurity() {
    this.securityService
      .reload({
        module: "server",
        config: this.serverSecurity
      })
      .then(() => {
        this.noti.push({
          content: "Module Server saved correctly.",
          autoHide: true
        });
      })
      .catch(err => {
        this.noti.push({ content: err.data, error: true, autoHide: true });
      });
  }
  savePasswordValidator() {
    this.securityService
      .reload({
        module: "passwordValidator",
        config: this.passwordValidator
      })
      .then(() => {
        this.noti.push({
          content: "Module password validator saved correctly.",
          autoHide: true
        });
      })
      .catch(err => {
        this.noti.push({ content: err.data, error: true, autoHide: true });
      });
  }
}
