import {downgradeInjectable} from '@angular/upgrade/static';
import 'rxjs/add/operator/toPromise';
import * as _ from 'underscore';

import {Injectable} from "@angular/core";
declare var angular:any
declare var noty:any

@Injectable()
class NotificationService {

  private current;
  private timerID;

  constructor() {}

  push(notification) {

    var n;

    if (this.current) {
      this.current.close();
    }
    if (notification.error) {
      if (typeof notification.content != 'string') {
        notification.content = notification.content.errors[0].content;
      }
      n = noty({text: _.escape(notification.content), layout: 'bottom', type: 'error', theme: 'relax'});
    } else if (notification.warning) {
      n = noty({text: _.escape(notification.content.toString()), layout: 'bottom', type: 'warning', theme: 'relax'});
    } else {
      n = noty({text: _.escape(notification.content.toString()), layout: 'bottom', type: 'success', theme: 'relax'});
    }
    this.current = n;

    this.attachEvents();

    this.timerID = setTimeout(function () {
      if (n && !(n.options.type === 'error') && !(notification.sticky))
        n.close();
    }, 4000);
  }

  attachEvents() {
    (<any>$('[class="noty_message"]')).on({
        mouseover: () => {
          this.stopTimer();
        },
        mouseleave: () => {
          this.startTimer();
        }
      }
    );
  }

  startTimer() {
    this.timerID = setTimeout(() => {
      this.clear();
    }, 4000)
  }

  stopTimer() {
    if (this.timerID) {
      clearTimeout(this.timerID);
    }
  }

  clear() {
    if (this.current) {
      this.current.close();
    }
  }

}

angular.module('command.services', []).factory(
  `NotificationService`,
  downgradeInjectable(NotificationService));

export {NotificationService};
