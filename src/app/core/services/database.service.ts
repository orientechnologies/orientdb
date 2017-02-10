import {API} from '../../../constants';
import {Injectable} from "@angular/core";



@Injectable()
class DBService {
  exportDB(db) {
    window.open(API + 'export/' + db);
  }
}

export {DBService};
