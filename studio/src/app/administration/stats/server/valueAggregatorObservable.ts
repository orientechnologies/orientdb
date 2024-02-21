import { Observable } from "rxjs/Observable";
import { EventEmitter } from "@angular/core";
import { Subscriber } from "rxjs/Subscriber";

export class ValueAggObserbable extends Observable<number> {
  emitter: EventEmitter<number>;
  constructor(emitter) {
    super();
    this.emitter = emitter;
  }

  _subscribe(subscriber) {
    return new ValueSubscriber(subscriber, this.emitter);
  }
}

class ValueSubscriber extends Subscriber<number> {
  emitter: EventEmitter<number>;
  total: number;
  constructor(dest, emitter: EventEmitter<number>) {
    super();
    this.destination = dest;
    this.emitter = emitter;
    this.emitter.subscribe(val => {
      if (!this.total) {
        this.total = val;
      }
      this.next(val - this.total);
      this.total = val;
    });
  }

  next(e) {
    this.destination.next(e);
  }
}
