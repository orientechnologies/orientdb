import { downgradeInjectable } from "@angular/upgrade/static";
import { Http } from "@angular/http";
import "rxjs/add/operator/toPromise";

import { API } from "../../../constants";
import { Injectable } from "@angular/core";
import { Headers } from "@angular/http";
declare var angular: any;

@Injectable()
class ProfilerService {
  constructor(private http: Http) {}

  profilerData(params) {
    let url = API + "sqlProfiler/" + params.db;
    return this.http
      .get(url)
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  runningQueries(params) {
    let url = API + "sqlProfiler/running";
    if (params) {
      url += serialize(params);
    }
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
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

  reset(params) {
    let url = API + "sqlProfiler/" + params.db + "/reset";
    return this.http
      .get(url)
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  metadata() {
    let url = API + "profiler/metadata";
    return this.http
      .get(url)
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  realtime() {
    let url = API + "profiler/realtime";
    return this.http
      .get(url)
      .toPromise()
      .then(data => {
        return data.json();
      });
  }
}

function serialize(obj) {
  return (
    "?" +
    Object.keys(obj)
      .reduce(function(a, k) {
        if (obj[k] != null) {
          a.push(k + "=" + encodeURIComponent(obj[k]));
        }
        return a;
      }, [])
      .join("&")
  );
}
angular
  .module("command.services", [])
  .factory(`ProfilerService`, downgradeInjectable(ProfilerService));

export { ProfilerService };
