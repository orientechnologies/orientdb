import { Pipe, PipeTransform } from "@angular/core";

const filterOne = (it, fields, val) => {
  return fields
    .map(f => {
      return it[f].toLowerCase().includes(val);
    })
    .reduce((acc, current) => acc || current, false);
};

@Pipe({
  name: "filter"
})
export class FilterPipe implements PipeTransform {
  transform(items: any[], searchText: string, fields: any[]): any[] {
    if (!items) return [];
    if (!searchText) return items;

    searchText = searchText.toLowerCase();
    if (!fields || fields.length == 0) {
      return items.filter(it => filterOne(it, Object.keys(it), searchText));
    }
    return items.filter(it => filterOne(it, fields, searchText));
  }
}
