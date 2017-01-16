import {API} from '../../../constants';


class TeleporterService {

  drivers() {
    window.open(API + 'teleporter/drivers');
  }

  launch(config) {
    window.open(API + 'teleporter/job');
  }

  test(config) {
    window.open(API + 'teleporter/test');
  }
}

export {TeleporterService};
