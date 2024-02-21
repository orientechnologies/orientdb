import { PipeTransform, Pipe } from "@angular/core";

@Pipe({ name: "eventToString" })
export class EventToString implements PipeTransform {
  transform(value): any {
    return info(value);
  }
}

function info(event) {
  var info = modeToString(event.mode);
  switch (event.op) {
    case "BACKUP_FINISHED":
      info += " executed";
      break;
    case "BACKUP_ERROR":
      info += " error";
      break;
    case "BACKUP_SCHEDULED":
      info += " scheduled.";
      break;
    case "BACKUP_STARTED":
      info += " started";
      break;
    case "RESTORE_FINISHED":
      info = "Restore finished";
      break;
    case "RESTORE_STARTED":
      info = "Restore started";
      break;
    case "RESTORE_ERROR":
      info = "Restore error";
      break;
  }
  return info;
}

function modeToString(mode) {
  switch (mode) {
    case "INCREMENTAL_BACKUP":
      return "Incremental backup";
    case "FULL_BACKUP":
      return "Full backup";
  }
}
