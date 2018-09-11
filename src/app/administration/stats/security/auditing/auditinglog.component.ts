import { Component, OnDestroy, OnInit, Input } from "@angular/core";

declare const angular: any;

@Component({
  selector: "auditing-log-management",
  templateUrl: "./auditinglog.component.html",
  styles: [""]
})
class AuditingLogComponent implements OnInit, OnDestroy {
  @Input()
  private security: any;

  private query: any = {};

  constructor() {}

  ngOnInit(): void {}

  ngOnDestroy(): void {}
}

export { AuditingLogComponent };
