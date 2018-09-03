import { ImportExportComponent } from "./dbconfiguration";

import {
  ImportManagerComponent,
  TeleporterComponent,
  GraphModelPanelComponent,
  DetailPanelComponent,
  SchedulerComponent,
  NewsBoxComponent,
  Neo4jImporterComponent,
  EtlComponent,
  DashboardComponent,
  DashboardStatsComponent,
  ServerStatsComponent,
  GaugeComponent,
  ServerStatsGauges,
  ServerStatsText,
  ChartComponent,
  ServerMetricsComponent,
  ServerCrudComponent
} from "./administration";

import { DualListComponent, GraphComponent, AddEdgeModal } from "./util";

export const APP_DECLARATIONS = [
  ImportExportComponent,
  ImportManagerComponent,
  TeleporterComponent,
  GraphModelPanelComponent,
  DetailPanelComponent,
  Neo4jImporterComponent,
  EtlComponent,
  SchedulerComponent,
  DualListComponent,
  GraphComponent,
  AddEdgeModal,
  NewsBoxComponent,
  DashboardComponent,
  DashboardStatsComponent,
  ServerMetricsComponent,
  ServerStatsComponent,
  ServerCrudComponent,
  GaugeComponent,
  ChartComponent,
  ServerStatsGauges,
  ServerStatsText
];
