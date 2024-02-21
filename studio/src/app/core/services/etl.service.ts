import { downgradeInjectable } from "@angular/upgrade/static";
import { Http } from "@angular/http";
import "rxjs/add/operator/toPromise";

import { API } from "../../../constants";
import { Injectable } from "@angular/core";
import { Headers } from "@angular/http";
declare var angular: any;

@Injectable()
class EtlService {
  constructor(private http: Http) {}

  launch(params) {
    let url = API + "etl/job";
    return this.http
      .post(url, params, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  status() {
    let url = API + "etl/status";
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  saveConfiguration(params) {
    let url = API + "etl/save-config";
    return this.http
      .post(url, params, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  initDatabase2Configs() {
    let url = API + "etl/list-configs";
    return this.http
      .post(url, null, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  getOptions() {
    let headers = new Headers({
      "Authorization": localStorage.getItem("SimpleAuth")
    });
    return {
      headers: headers
    };
  }
}

angular
  .module("command.services", [])
  .factory(`EtlService`, downgradeInjectable(EtlService));

export { EtlService };
