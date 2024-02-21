import {
  Component,
  OnInit,
  OnDestroy,
  Input,
  OnChanges,
  SimpleChange,
  SimpleChanges
} from "@angular/core";
import { ProfilerService } from "../../../core/services";

@Component({
  selector: "running-query-profiler",
  templateUrl: "./runningqueryprofiler.component.html",
  styles: [""]
})
export class RunningQueryProfilerComponent
  implements OnInit, OnDestroy, OnChanges {
  queries: any;
  handle: any;

  @Input()
  server: string;
  @Input()
  database: string;

  constructor(private profiler: ProfilerService) {}

  ngOnInit(): void {
    this.handle = setInterval(() => {
      this.fetchQueries();
    }, 5000);
  }

  fetchQueries() {
    if (this.server) {
      this.profiler.runningQueries({ db: this.database }).then(response => {
        this.queries = response.result;
      });
    }
  }
  ngOnChanges(simpleChange: SimpleChanges) {
    this.fetchQueries();
  }
  ngOnDestroy(): void {
    clearInterval(this.handle);
  }
}
