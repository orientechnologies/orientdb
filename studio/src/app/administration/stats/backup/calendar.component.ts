import {
  Component,
  Input,
  ElementRef,
  NgZone,
  OnDestroy,
  ViewChild,
  OnInit,
  ViewContainerRef,
  Output,
  OnChanges,
  SimpleChange,
  SimpleChanges,
  EventEmitter
} from "@angular/core";
import {
  AgentService,
  BackupService,
  MetricService
} from "../../../core/services";

declare var $: any;

import * as FC from "fullcalendar";

@Component({
  selector: "backup-calendar",
  template: "<div ></div>",
  styles: [""]
})
class BackupCalendarComponent implements OnInit, OnDestroy, OnChanges {
  @Input()
  private events = [];

  @Output()
  private onRange = new EventEmitter<any>();

  @Output()
  private onSelected = new EventEmitter<any>();
  calendar: FC.Calendar;

  constructor(private elem: ElementRef) {}

  ngOnInit(): void {
    var self = this;
    this.calendar = new FC.Calendar($(this.elem.nativeElement), {
      header: {
        left: "prev,next today",
        center: "title",
        right: "month,agendaWeek,agendaDay"
      },
      viewRender: function(view, element) {
        self.onRange.emit({
          from: view.start.format("x"),
          to: view.end.format("x")
        });
      },
      eventClick: function(calEvent, jsEvent, view) {
        self.onSelected.emit(calEvent);
      },
      defaultView: "agendaWeek",
      editable: true
    });

    this.calendar.render();
  }

  ngOnDestroy(): void {}
  ngOnChanges(changes: SimpleChanges): void {
    if (changes.events.currentValue && this.calendar) {
      this.calendar.removeEvents(null);
      this.calendar.addEventSource(changes.events.currentValue);
    }
  }

  fetchBackups() {}
}

export { BackupCalendarComponent };
