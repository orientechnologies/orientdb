import { downgradeInjectable } from "@angular/upgrade/static";
import { Http } from "@angular/http";
import "rxjs/add/operator/toPromise";

import { API } from "../../../constants";
import { Injectable } from "@angular/core";
import { Headers } from "@angular/http";
import { DashboardStatsComponent } from "../../administration/stats";
declare var angular: any;

@Injectable()
class PermissionService {
  private data = { permissions: [] };
  constructor(private http: Http) {}

  myPermissions() {
    let url = API + "permissions/mine";
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      })
      .then(data => {
        this.data = data;
      })
      .catch(err => {
        this.data.permissions = ["server.metrics"];
      });
  }

  getOptions() {
    let headers = new Headers({
      Authorization: localStorage.getItem("SimpleAuth")
    });
    return {
      headers: headers
    };
  }
  allPermissions() {
    let url = API + "permissions/all";
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  public getCurrentComponent(tab) {
    if (tab) {
      return this.getSideMenu().filter(m => {
        return m.name === tab;
      })[0];
    } else {
      return this.getSideMenu()[0];
    }
  }
  public getSideMenu() {
    // {
    //   name: "alerts",
    //   title: "Alerts Management",
    //   template: "events",
    //   icon: "fa-bell"
    // },

    // {
    //   name: "cloud",
    //   title: "Cloud Management",
    //   template: "cloud",
    //   icon: "fa-cloud",
    //   wiki: "OrientDB-Cloud.html"
    // }

    let menus = [];

    if (this.isAllow("server.metrics")) {
      menus.push({
        name: "stats",
        title: "Dashboard",
        template: "stats",
        icon: "fa-dashboard",
        wiki: "Studio-Dashboard.html"
      });
    }
    if (this.isAllow("server.metrics")) {
      menus.push({
        name: "general",
        title: "Server Management",
        template: "general",
        icon: "fa-desktop",
        wiki: "Studio-Server-Management.html"
      });
    }
    if (this.isAllow("server.metrics") && this.isAllow("server.distributed")) {
      menus.push({
        name: "cluster",
        title: "Cluster Management",
        template: "distributed",
        icon: "fa-sitemap",
        wiki: "Studio-Cluster-Management.html"
      });
    }
    if (this.isAllow("server.backup")) {
      menus.push({
        name: "backup",
        title: "Backup Management",
        template: "backup",
        icon: "fa-clock-o",
        wiki: "Studio-Backup-Management.html"
      });
    }
    if (this.isAllow("server.metrics")) {
      menus.push({
        name: "profiler",
        title: "Server Profiler",
        template: "profiler",
        icon: "fa-rocket",
        wiki: "Studio-Query-Profiler.html"
      });
    }
    if (this.isAllow("server.security")) {
      menus.push({
        name: "security",
        title: "Security Management",
        template: "security",
        icon: "fa-lock",
        wiki: "Security-Config.html"
      });
    }
    if (this.isAllow("server.importers")) {
      menus.push({
        name: "importers",
        title: "Importer",
        template: "importersManager",
        icon: "fa-plug"
      });
    }

    menus.push({
      name: "servercommands",
      title: "Server Commands",
      template: "servercommands",
      icon: "fa-bolt"
    });

    menus.push({
      name: "settings",
      title: "Settings",
      icon: "fa-cog"
    });

    return menus;
  }

  isAllow(permission) {
    return this.data.permissions.indexOf(permission) != -1;
  }
}

angular
  .module("permissions.services", [])
  .factory(`PermissionService`, downgradeInjectable(PermissionService));

export { PermissionService };
