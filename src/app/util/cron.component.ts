import {
  Component,
  Input,
  OnInit,
  ViewChild,
  ElementRef,
  OnChanges,
  SimpleChanges,
  Output,
  EventEmitter
} from "@angular/core";

declare var $: any;

@Component({
  selector: "cron",
  template: "<div></div>"
})
export class CronComponent implements OnInit, OnChanges {
  cronElement: any;
  constructor(private elem: ElementRef) {}

  @Input()
  private cron;

  @Output()
  private changed: EventEmitter<any> = new EventEmitter();

  ngOnChanges(changes: SimpleChanges) {
    if (this.cronElement) {
      this.cronElement.cron("value", changes.cron.currentValue);
    }
  }
  ngOnInit(): void {
    let self = this;
    this.cronElement = $(this.elem.nativeElement).cron({
      onChange: function() {
        self.changed.emit($(this).cron("value"));
      },
      customValues: {
        "5 Minutes": "0 0/5 * * * ?",
        "10 Minutes": "0 0/10 * * * ?",
        "30 Minutes": "0 0/30 * * * ?"
      }
    });
  }
}
