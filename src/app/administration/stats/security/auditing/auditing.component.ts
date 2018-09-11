import { Component, OnDestroy, OnInit, Input } from "@angular/core";

declare const angular: any;

@Component({
  selector: "auditing-management",
  templateUrl: "./auditing.component.html",
  styles: [""]
})
class AuditingComponent implements OnInit, OnDestroy {
  @Input()
  private security: any;

  private tab = "auditingLog";
  constructor() {}

  ngOnInit(): void {}

  ngOnDestroy(): void {}
}

export { AuditingComponent };
