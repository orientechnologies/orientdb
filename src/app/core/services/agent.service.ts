import { downgradeInjectable } from "@angular/upgrade/static";
import { Http } from "@angular/http";
import "rxjs/add/operator/toPromise";

import { Injectable } from "@angular/core";
declare var angular: any;
import { API } from "../../../constants";

@Injectable()
class AgentService {
  private agent;
  public active = false;

  constructor(private http: Http) {
    this.agent = {
      active: null
    };
  }

  isActive(): any {
    if (this.agent.active == null) {
      let url = API + "isEE";
      return this.http
        .get(url)
        .toPromise()
        .then(data => {
          return data.json();
        })
        .then(response => {
          this.agent.active = response.enterprise;
          this.active = response.enterprise;
          return this.agent;
        });
    }
    return new Promise((resolve, reject) => {
      resolve(this.agent);
    });
  }
}

angular
  .module("agent.services", [])
  .factory(`AgentService`, downgradeInjectable(AgentService));

export { AgentService };
