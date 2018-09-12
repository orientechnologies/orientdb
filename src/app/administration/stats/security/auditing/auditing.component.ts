import { Component, OnDestroy, OnInit, Input } from "@angular/core";
import { MetricService } from "../../../../core/services";

declare const angular: any;

@Component({
  selector: "auditing-management",
  templateUrl: "./auditing.component.html",
  styles: [""]
})
class AuditingComponent implements OnInit, OnDestroy {
  @Input()
  private security: any;

  @Input()
  private databases: string[];

  private tab = "auditingLog";
  constructor(private metrics: MetricService) {}

  ngOnInit(): void {
    
  }

  ngOnDestroy(): void {}
}

export { AuditingComponent };
