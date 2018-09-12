import { Component, Input, OnInit } from "@angular/core";
import {
  SecurityService,
  NotificationService,
  DBService,
  MetricService
} from "../../../../core/services";

@Component({
  selector: "ldap-importer-management",
  templateUrl: "./ldapimporter.component.html",
  styles: [""]
})
export class LdapImporterComponent implements OnInit {
  @Input()
  private security: any;
  ldap: any;
  currentSelected: any;
  authenticators: any;
  currentDomain: any;
  databases: any[];

  constructor(
    private securityService: SecurityService,
    private noti: NotificationService,
    private metricService: MetricService
  ) {}

  ngOnInit(): void {
    this.metricService.listDatabases().then(data => {
      this.databases = data.databases;
    });
    this.ldap = this.security.ldapImporter || {
      enabled: false,
      class: "com.orientechnologies.security.ldap.OLDAPImporter",
      debug: false,
      period: 60,
      databases: []
    };

    if (this.security.authentication)
      this.authenticators = this.security.authentication.authenticators;

    if (this.ldap.databases.length > 0) {
      this.currentSelected = this.ldap.databases[0];

      if (this.currentSelected.domains.length > 0) {
        this.currentDomain = this.currentSelected.domains[0];
      }
    }
  }

  addDatabase() {
    this.currentSelected = {
      database: "",
      ignoreLocal: true
    };
    this.ldap.databases.push(this.currentSelected);
  }

  addDomain() {
    if (this.currentSelected) {
      if (!this.currentSelected.domains) {
        this.currentSelected.domains = [];
      }
      this.currentDomain = {
        domain: "",
        authenticator: ""
      };
      this.currentSelected.domains.push(this.currentDomain);
    }
  }

  removeDomain(idx) {
    this.currentSelected.domains.splice(idx, 1);
    this.currentDomain = null;
  }
  removeDatabase(idx) {
    this.ldap.databases.splice(idx, 1);
    this.currentSelected = null;
    this.currentDomain = null;
  };

  removeUser(idx) {
    this.currentDomain.users.splice(idx, 1);
  }
  removeServer(idx) {
    this.currentDomain.servers.splice(idx, 1);
  }
  addServer = function() {
    if (!this.currentDomain.servers) {
      this.currentDomain.servers = [];
    }
    this.currentDomain.servers.push({
      url: "ldap://alias.ad.domain.com:389",
      isAlias: true
    });
  };

  addUser() {
    if (!this.currentDomain.users) {
      this.currentDomain.users = [];
    }
    this.currentDomain.users.push({
      baseDN: "CN=Users,DC=ad,DC=domain,DC=com",
      filter:
        "(&(objectCategory=person)(objectclass=user)(memberOf=CN=ODBUser,CN=Users,DC=ad,DC=domain,DC=com))",
      roles: ["reader", "writer"]
    });
  }

  setCurrentDomain(d) {
    this.currentDomain = d;
  }

  setSelected(a) {
    this.currentSelected = a;
  }

  saveLdap() {
    this.securityService
      .reload({
        module: "ldapImporter",
        config: this.ldap
      })
      .then(() => {
        this.noti.push({
          content: "Module Ldap importer saved correctly.",
          autoHide: true
        });
      })
      .catch(err => {
        this.noti.push({ content: err.data, error: true, autoHide: true });
      });
  }
}
