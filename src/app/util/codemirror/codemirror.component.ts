import {
  Component,
  Input,
  OnInit,
  ViewChild,
  ElementRef,
  OnChanges,
  SimpleChanges
} from "@angular/core";

import * as Codemirror from "codemirror";

@Component({
  selector: "codemirror",
  templateUrl: "./codemirror.component.html"
})
export class CodemirrorComponent implements OnInit, OnChanges {
  @Input()
  private options;

  @ViewChild("codemirror", { read: ElementRef })
  textArea: ElementRef;

  @Input()
  private text;
  codemirror: any;

  ngOnChanges(changes: SimpleChanges): void {
    if (this.codemirror) {
      this.codemirror.setValue(changes.text.currentValue);
    }
  }
  ngOnInit(): void {
    this.options = this.options || {
      lineWrapping: true,
      lineNumbers: true,
      readOnly: true
    };
    this.codemirror = Codemirror.fromTextArea(
      this.textArea.nativeElement,
      this.options
    );
    this.codemirror.setValue(this.text);
  }
}
