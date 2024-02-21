import { Component, ElementRef, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";

declare const c3: any;

@Component({
  selector: "o-chart",
  templateUrl: "./chart.component.html",
  styles: [""]
})
class ChartComponent implements OnInit, OnChanges {
  @Input()
  private columns;

  @Input()
  private height;

  private chart;

  private limit = 20;

  private count = 0;

  constructor(protected el: ElementRef) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.columns = changes.columns.currentValue;

    if (this.chart) {
      let len = 0;
      if (this.count == this.limit) {
        this.count -= 1;
        len = 1;
      }
      let columns = [["x", new Date()]];
      this.columns.forEach(element => {
        columns.push([element.label, ...element.values]);
      });
      this.chart.flow({
        columns: columns,
        length: len,
        duration: 1500
      });
      this.count++;
    }
  }
  ngOnInit(): void {
    let columns = [["x", new Date()]];
    if (this.columns) {
      this.columns.forEach(element => {
        columns.push([element.label, ...element.values]);
      });
    }

    this.chart = c3.generate({
      bindto: this.el.nativeElement,
      data: {
        x: "x",
        columns: columns
      },
      point: {
        show: false
      },
      size: {
        height: this.height || 250
      },
      axis: {
        x: {
          type: "timeseries",
          tick: {
            culling: {
              max: 4 // the number of tick texts will be adjusted to less than this value
            },
            format: "%H:%M:%S"
          }
        },
        y: {}
      }
    });
  }
}

export { ChartComponent };

