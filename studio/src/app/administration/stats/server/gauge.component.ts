import {
  Component,
  Input,
  ElementRef,
  NgZone,
  OnDestroy,
  ViewChild,
  OnInit,
  ViewContainerRef,
  OnChanges,
  SimpleChanges,
  SimpleChange
} from "@angular/core";
import { downgradeComponent } from "@angular/upgrade/static";

declare const c3: any;

@Component({
  selector: "o-gauge",
  templateUrl: "./gauge.component.html",
  styles: [""]
})
class GaugeComponent implements OnInit, OnChanges {
  @Input()
  private name;
  @Input()
  private val;
  @Input()
  private height;

  private chart;

  constructor(protected el: ElementRef) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (this.chart) {
      let data = changes.val.currentValue;
      this.chart.load({
        columns: [["data", data]]
      });
    }
  }
  ngOnInit(): void {
    this.chart = c3.generate({
      bindto: this.el.nativeElement,
      data: {
        columns: [["data", this.val || 0]],
        type: "gauge"
      },
      color: {
        pattern: ["#60B044", "#F6C600", "#F97600", "#FF0000"], // the three color levels for the percentage values.
        threshold: {
          values: [30, 60, 90, 100]
        }
      },
      size: {
        height: 150
      }
    });
  }
}

export { GaugeComponent };
