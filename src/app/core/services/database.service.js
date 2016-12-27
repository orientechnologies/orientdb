import {API} from '../../../constants';


class DBService {
  exportDB(db) {
    window.open(API + 'export/' + db);
  }
}

export {DBService};
