import { Component, OnDestroy, OnInit, Input } from "@angular/core";

declare const angular: any;

@Component({
  selector: "password-authenticator",
  templateUrl: "./passwordauthenticator.component.html",
  styles: [""]
})
class PasswordAuthenticatorComponent implements OnInit, OnDestroy {
  @Input()
  private config: any;

  @Input()
  private canEdit: boolean;

  constructor() {}

  ngOnInit(): void {}

  addUser() {
    if (!this.config.users) {
      this.config.users = [];
    }
    this.config.users.push({
      username: "guest",
      resources: "connect,server.listDatabases,server.dblist"
    });
  }

  removeUser(idx) {
    this.config.users.splice(idx, 1);
  }
  ngOnDestroy(): void {}
}

export { PasswordAuthenticatorComponent };
