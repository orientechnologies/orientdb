import { Component, OnDestroy, OnInit, Input } from "@angular/core";

declare const angular: any;

@Component({
  selector: "auditing-cfg-management",
  templateUrl: "./auditingcfg.component.html",
  styles: [""]
})
class AuditingConfigComponent implements OnInit, OnDestroy {
  @Input()
  private security: any;

  constructor() {}

  ngOnInit(): void {}

  ngOnDestroy(): void {}
}

export { AuditingConfigComponent };
