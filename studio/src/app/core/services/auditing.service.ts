import { downgradeInjectable } from "@angular/upgrade/static";
import { Http } from "@angular/http";
import "rxjs/add/operator/toPromise";

import { Injectable } from "@angular/core";
declare var angular: any;

import { Headers } from "@angular/http";
import { API } from "../../../constants";

@Injectable()
class AuditingService {
  constructor(private http: Http) {}

  getConfig(database) {
    let url = `${API}auditing/${database}/config`;
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  saveConfig(db, config) {
    let url = API + "auditing/" + db + "/config";
    return this.http
      .post(url, config, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }
  query(params) {
    let url = API + "auditing/logs/query";
    return this.http
      .post(url, params, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  getOptions() {
    let headers = new Headers({
      Authorization: localStorage.getItem("SimpleAuth"),
      "X-Requested-With": "XMLHttpRequest"
    });
    return {
      headers: headers
    };
  }
}

export { AuditingService };
