import { downgradeInjectable } from "@angular/upgrade/static";
import { Http } from "@angular/http";
import "rxjs/add/operator/toPromise";

import { Injectable } from "@angular/core";
declare var angular: any;

import { Headers } from "@angular/http";
import { API } from "../../../constants";

@Injectable()
class SecurityService {
  constructor(private http: Http) {}

  getConfig() {
    let url = API + "security/config";
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
}

export { SecurityService };
