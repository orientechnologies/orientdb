import {API} from '../../../constants';
import {Injectable} from "@angular/core";



@Injectable()
export class DBService {
  exportDB(db) {
    window.open(API + 'export/' + db);
  }
}

