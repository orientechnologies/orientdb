import { downgradeInjectable } from "@angular/upgrade/static";
import { Http } from "@angular/http";
import "rxjs/add/operator/toPromise";

import { Injectable } from "@angular/core";
declare var angular: any;
import { API } from "../../../constants";
import { Headers } from "@angular/http";

@Injectable()
class BackupService {
  constructor(private http: Http) {}

  getConfig() {
    let url = API + "backupManager";
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  save(backup) {
    let url = API + "backupManager";

    if (backup.uuid) {
      return this.http
        .put(url, backup, this.getOptions())
        .toPromise()
        .then(data => {
          return data.json();
        });
    } else {
      return this.http
        .post(url + "/" + backup.uuid, backup, this.getOptions())
        .toPromise()
        .then(data => {
          return;
        })
        .catch(err => {
          let body = {};
          try {
            body = err.json();
          } catch (e) {
            body = {
              errors: [
                {
                  reason: 500,
                  code: 500,
                  content: err.text()
                }
              ]
            };
          }
          throw body;
        });
    }
  }

  unitLogs(uuid, unitId, params) {
    let url = API + "backupManager/" + uuid + "/log/" + unitId;
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

  logs(uuid, params) {
    let url = API + `backupManager/${uuid}/log`;
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
      Authorization: localStorage.getItem("SimpleAuth"),
      "X-Requested-With": "XMLHttpRequest"
    });
    return {
      headers: headers
    };
  }
}
function serialize(obj) {
  return (
    "?" +
    Object.keys(obj)
      .reduce(function(a, k) {
        a.push(k + "=" + encodeURIComponent(obj[k]));
        return a;
      }, [])
      .join("&")
  );
}

export { BackupService };
