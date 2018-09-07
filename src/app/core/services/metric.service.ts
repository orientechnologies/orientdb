import { downgradeInjectable } from "@angular/upgrade/static";
import { Http } from "@angular/http";
import "rxjs/add/operator/toPromise";

import { Injectable } from "@angular/core";
declare var angular: any;
import { Headers } from "@angular/http";
import { API } from "../../../constants";

@Injectable()
class MetricService {
  constructor(private http: Http) {}

  getMetrics(): any {
    let url = API + "metrics";
    return this.http
      .get(url, this.getOptions())
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

  getInfo(agent, name) {
    let url = API + (agent ? `/node/info?${name}` : "server");
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  listDatabases() {
    let url = API + "listDatabases";
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }
  threadDumps(name) {
    let url = API + `/node/threadDump?${name}`;
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }
}

angular
  .module("metric.services", [])
  .factory(`MetricService`, downgradeInjectable(MetricService));

export { MetricService };
