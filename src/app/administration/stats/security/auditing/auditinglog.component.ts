import { Component, OnDestroy, OnInit, Input } from "@angular/core";
import { AuditingService } from "../../../../core/services";

declare const angular: any;

@Component({
  selector: "auditing-log-management",
  templateUrl: "./auditinglog.component.html",
  styles: [""]
})
class AuditingLogComponent implements OnInit, OnDestroy {
  @Input()
  private security: any;

  @Input()
  private databases: string[];

  private logs: any[] = [];

  private query: any = { limit: 100 };

  constructor(private auditingService: AuditingService) {}

  ngOnInit(): void {
    this.searchLogs();
  }

  ngOnDestroy(): void {

  }

  resetFilter(){
    this.query = { limit : 100}
  }

  searchLogs(){
    this.auditingService.query(this.query).then(response => {
      this.logs = response.result;
    });
  }
}

export { AuditingLogComponent };
