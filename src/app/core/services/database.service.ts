import { API } from "../../../constants";
import { Injectable } from "@angular/core";
import { Http, Headers } from "@angular/http";
import "rxjs/add/operator/toPromise";

@Injectable()
export class DBService {
  constructor(private http: Http) {}

  exportDB(db) {
    window.open(API + "export/" + db);
  }

  listClasses(db) {
    let url = `${API}database/${db}`;

    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      })
      .then(data => data["classes"]);
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
  getServerVersion() {
    let url = API + "server/version";
    return this.http
      .get(url)
      .toPromise()
      .then(data => {
        return data["_body"];
      });
  }
}
