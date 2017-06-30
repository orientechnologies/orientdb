import {API} from '../../../constants';
import {Injectable} from "@angular/core";
import {Http} from '@angular/http';
import 'rxjs/add/operator/toPromise';


@Injectable()
export class DBService {

  constructor(private http: Http) {
  }

  exportDB(db) {
    window.open(API + 'export/' + db);
  }

  getServerVersion() {
    let url = API + 'server/version';
    return this.http.get(url).toPromise().then((data) => {
      return data["_body"];
    });
  }
}

