import { PipeTransform, Pipe } from "@angular/core";

@Pipe({ name: "operation" })
export class OperationPipe implements PipeTransform {
  transform(value): any {
    switch (value) {
      case 0:
        return "Read";
      case 1:
        return "Update";
      case 2:
        return "Delete";
      case 3:
        return "Create";
      case 4:
        return "Command";
      case 5:
        return "Create Class";
      case 6:
        return "Drop Class";
      case 7:
        return "Config Changed";
      case 8:
        return "Node Joined";
      case 9:
        return "Node Left";
      case 10:
        return "Security Module";
      case 11:
        return "Security component reloaded";
    }
    return value;
  }
}
