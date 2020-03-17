import { Injectable } from "@angular/core";
import { Headers, Http } from "@angular/http";
import { downgradeInjectable } from "@angular/upgrade/static";
import "rxjs/add/operator/toPromise";
import { API } from "../../../constants";

declare var angular: any;

@Injectable()
class DistributedService {
  constructor(private http: Http) {}

  getOptions() {
    let headers = new Headers({
      Authorization: localStorage.getItem("SimpleAuth"),
      "X-Requested-With": "XMLHttpRequest"
    });
    return {
      headers: headers
    };
  }

  getDatabaseConfig(name) {
    let url = API + `distributed/database/${name}`;
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  saveDatabaseConfig(name, config) {
    let url = API + `distributed/database/${name}`;
    return this.http
      .put(url, config, this.getOptions())
      .toPromise()
      .then(data => {
        return;
      });
  }
}

angular
  .module("distributed.services", [])
  .factory(`DistributedService`, downgradeInjectable(DistributedService));

export { DistributedService };
