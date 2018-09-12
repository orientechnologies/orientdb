import {
  Component,
  OnDestroy,
  OnInit,
  Input,
  ViewContainerRef,
  ViewChild,
  ComponentFactoryResolver,
  Injector
} from "@angular/core";
import {
  MetricService,
  SecurityService,
  NotificationService
} from "../../../../core/services";
import { PasswordAuthenticatorComponent } from "./authenticators/passwordauthenticator.component";
import { ModalComponent } from "ng2-bs3-modal";
import { KerberosAuthenticatorComponent } from "./authenticators";

declare const angular: any;

const suppertedAuthenticators = {
  "com.orientechnologies.orient.server.security.authenticator.ODefaultPasswordAuthenticator": {
    defaultVal: {
      name: "Password",
      class:
        "com.orientechnologies.orient.server.security.authenticator.ODefaultPasswordAuthenticator",
      enabled: true
    }
  },
  "com.orientechnologies.security.kerberos.OKerberosAuthenticator": {
    defaultVal: {
      name: "Kerberos",
      class: "com.orientechnologies.security.kerberos.OKerberosAuthenticator",
      enabled: true,
      debug: false,

      krb5_config: "/etc/krb5.conf",

      service: {
        ktname: "/etc/keytab/kerberosuser",
        principal: "kerberosuser/kerberos.domain.com@REALM.COM"
      },

      spnego: {
        ktname: "/etc/keytab/kerberosuser",
        principal: "HTTP/kerberos.domain.com@REALM.COM"
      },

      client: {
        ccname: null,
        ktname: null,
        useTicketCache: true,
        principal: "kerberosuser@REALM.COM",
        renewalPeriod: 300
      }
    }
  },
  "com.orientechnologies.orient.server.security.authenticator.OServerConfigAuthenticator": {
    defaultVal: {
      name: "ServerConfig",
      class:
        "com.orientechnologies.orient.server.security.authenticator.OServerConfigAuthenticator",
      enabled: true
    }
  },
  "com.orientechnologies.orient.server.security.authenticator.OSystemUserAuthenticator": {
    defaultVal: {
      name: "SystemAuthenticator",
      class:
        "com.orientechnologies.orient.server.security.authenticator.OSystemUserAuthenticator",
      enabled: true
    }
  }
};

const authenticators = {
  Password: PasswordAuthenticatorComponent,
  Kerberos: KerberosAuthenticatorComponent
};
@Component({
  selector: "authentication-management",
  templateUrl: "./authentication.component.html",
  styles: [""]
})
class AuthenticationComponent implements OnInit, OnDestroy {
  @Input()
  private security: any;

  @ViewChild("authenticatorModal")
  authenticatorModal: ModalComponent;

  @ViewChild("authenticator", { read: ViewContainerRef })
  authenticator: ViewContainerRef;
  currentAuthenticator: any;
  auths: any[] = [];
  selectedAuthenticator: any;

  constructor(
    private metrics: MetricService,
    private resolver: ComponentFactoryResolver,
    private injector: Injector,
    private securityService: SecurityService,
    private noti: NotificationService
  ) {}

  ngOnInit(): void {}

  moveUp(elem, index) {
    this.security.authentication.authenticators.splice(index, 1);
    this.security.authentication.authenticators.splice(index - 1, 0, elem);
  }
  moveDown(elem, index) {
    this.security.authentication.authenticators.splice(index, 1);
    this.security.authentication.authenticators.splice(index + 1, 0, elem);
  }

  createComponent(component, config) {
    this.authenticator.clear();
    if (component) {
      const factory = this.resolver.resolveComponentFactory(component);
      let componentRef = this.authenticator.createComponent(
        factory,
        0,
        this.injector
      );
      this.bind(componentRef.instance, "config", config);
    }
  }

  saveAuthentication() {
    this.securityService
      .reload({
        module: "authentication",
        config: this.security.authentication
      })
      .then(() => {
        this.noti.push({
          content: "Module Authentication saved correctly.",
          autoHide: true
        });
      })
      .catch(err => {
        this.noti.push({ content: err.data, error: true, autoHide: true });
      });
  }
  selectAuthenticator(idx) {
    let auth = this.security.authentication.authenticators[idx];
    this.currentAuthenticator = auth;
    let comp = authenticators[auth.name];

    if (comp) {
      this.createComponent(comp, auth);
    }
  }

  addAuthenticator() {
    this.auths = Object.keys(suppertedAuthenticators)
      .filter(e => {
        return (
          this.security.authentication.authenticators.filter(ev => {
            return ev.class == e;
          }).length == 0
        );
      })
      .map(function(e) {
        return suppertedAuthenticators[e].defaultVal;
      });
    this.authenticatorModal.open();
  }
  bind(component, name, event) {
    component[name] = event;
  }
  hasTemplate(name) {
    return authenticators[name];
  }
  removeAuth(idx) {
    this.security.authentication.authenticators.splice(idx, 1);
    this.currentAuthenticator = null;
  }

  confirmAuthenticator(auth) {
    if (auth) {
      this.security.authentication.authenticators.push(auth);
      this.selectedAuthenticator = null;
      this.authenticatorModal.close();
    }
  }
  ngOnDestroy(): void {}
}

export { AuthenticationComponent };
