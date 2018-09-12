import { Component, OnDestroy, OnInit, Input } from "@angular/core";

declare const angular: any;

@Component({
  selector: "kerberos-authenticator",
  templateUrl: "./kerberosauthenticator.component.html",
  styles: [""]
})
class KerberosAuthenticatorComponent implements OnInit, OnDestroy {
  @Input()
  private config: any;

  constructor() {}

  ngOnInit(): void {}

  ngOnDestroy(): void {}
}

export { KerberosAuthenticatorComponent };
