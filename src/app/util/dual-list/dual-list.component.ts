import { Component, ChangeDetectorRef, DoCheck, EventEmitter, Input, IterableDiffers, OnChanges,
  Output, SimpleChange } from '@angular/core';

type compareFunction = (a:any, b:any) => number;

class BasicList {
  private _name:string;
  last:any;

  dragStart:boolean;
  dragOver:boolean;

  pick:Array<any>;
  list:Array<any>;

  constructor(name:string) {
    this._name = name;
    this.last = null;
    this.dragStart = false;
    this.dragOver = false;
    this.pick = new Array<any>();
    this.list = [];
  }

  get name() : string {
    return this._name;
  }
}

@Component({
  selector: 'dual-list',
  templateUrl: "./dual-list.component.html",
  styleUrls: []
})

export class DualListComponent implements DoCheck, OnChanges {
  static AVAILABLE_LIST_NAME = 'available';
  static CONFIRMED_LIST_NAME = 'confirmed';

  @Input() key:string = typeof this.key !== 'undefined' ? this.key : '_id';
  @Input() display:string = typeof this.display !== 'undefined' ? this.display : '_name';
  @Input() height:string = typeof this.height !== 'undefined' ? this.height : '100px';
  @Input() sort:boolean = typeof this.sort !== 'undefined' ? this.sort : false;
  @Input() compare:compareFunction = typeof this.compare !== 'undefined' ? this.compare : undefined;
  @Input() source:Array<any>; // = typeof this.source !== 'undefined' ? this.source : [];
  @Input() destination:Array<any>;
  @Input() headerLeft:string = typeof this.headerLeft !== 'undefined' ? this.headerLeft : 'Source';
  @Input() headerRight:string = typeof this.headerRight !== 'undefined' ? this.headerRight : 'Target';
  @Input() addBtn:string = typeof this.addBtn !== 'undefined' ? this.addBtn : 'Add';
  @Input() removeBtn:string = typeof this.removeBtn !== 'undefined' ? this.removeBtn : 'Remove';
  @Output() destinationChange = new EventEmitter();

  private available:BasicList;
  private confirmed:BasicList;

  private sourceDiffer:any;
  private destinationDiffer:any;

  private sorter = (a:any, b:any) => { return (a._name < b._name) ? -1 : ((a._name > b._name) ? 1 : 0); };


  constructor(private differs:IterableDiffers, private cdr:ChangeDetectorRef) {
  }

  ngOnChanges(changeRecord: {[key:string]:SimpleChange}) {
    if (changeRecord['sort']) {
      if (changeRecord['sort'].currentValue === true && this.compare === undefined) {
        this.compare = this.sorter;
      } else if (changeRecord['sort'].currentValue === false) {
        this.compare = undefined;
      }
    }

    if (changeRecord['source']) {
      this.available = new BasicList(DualListComponent.AVAILABLE_LIST_NAME);
      this.updatedSource();
      this.updatedDestination();
    }

    if (changeRecord['destination']) {
      this.confirmed = new BasicList(DualListComponent.CONFIRMED_LIST_NAME);
      this.updatedDestination();
      this.updatedSource();
    }
  }

  ngDoCheck() {
    let sourceChanges = this.sourceDiffer.diff(this.source);
    if (sourceChanges) {
      sourceChanges.forEachRemovedItem((r:any) => {
          let idx = this.findItemIndex(this.available.list, r.item, this.key);
          if (idx !== -1) {
            this.available.list.splice(idx, 1);
          }
        }
      );

      sourceChanges.forEachAddedItem((r:any) => {
          // Do not add duplicates even if source has duplicates.
          if (this.findItemIndex(this.available.list, r.item, this.key) === -1) {
            this.available.list.push( { _id: r.item[this.key], _name: this.makeName(r.item) });
          }
        }
      );

      if (this.compare !== undefined) {
        this.available.list.sort(this.compare);
      }
    }

    let destChanges = this.destinationDiffer.diff(this.destination);
    if (destChanges) {
      destChanges.forEachRemovedItem((r:any) => {
          let idx = this.findItemIndex(this.confirmed.list, r.item, this.key);
          if (idx !== -1) {
            if (!this.isItemSelected(this.confirmed.pick, this.confirmed.list[idx])) {
              this.selectItem(this.confirmed.pick, this.confirmed.list[idx]);
            }
            this.moveItem(this.confirmed, this.available, this.confirmed.list[idx]);
          }
        }
      );

      destChanges.forEachAddedItem((r:any) => {
          let idx = this.findItemIndex(this.available.list, r.item, this.key);
          if (idx !== -1) {
            if (!this.isItemSelected(this.available.pick, this.available.list[idx])) {
              this.selectItem(this.available.pick, this.available.list[idx]);
            }
            this.moveItem(this.available, this.confirmed, this.available.list[idx]);
          }
        }
      );

      if (this.compare !== undefined) {
        this.available.list.sort(this.compare);
      }
    }
  }

  updatedSource() {
    this.available.list.length = 0;
    this.available.pick.length = 0;

    if (this.source !== undefined) {
      this.sourceDiffer = this.differs.find(this.source).create(this.cdr);
    }
  }

  updatedDestination() {
    if (this.destination !== undefined) {
      this.destinationDiffer = this.differs.find(this.destination).create(this.cdr);
    }
  }

  dragEnd(list:BasicList = null) {
    if (list) {
      list.dragStart = false;
    } else {
      this.available.dragStart = false;
      this.confirmed.dragStart = false;
    }
    return false;
  }

  drag(event:DragEvent, item:any, list:BasicList) {
    if (!this.isItemSelected(list.pick, item)) {
      this.selectItem(list.pick, item);
    }
    list.dragStart = true;
    event.dataTransfer.setData('text', item[this.key]);
  }

  allowDrop(event:DragEvent, list:BasicList) {
    event.preventDefault();
    if (!list.dragStart) {
      list.dragOver = true;
    }
    return false;
  }

  dragLeave() {
    this.available.dragOver = false;
    this.confirmed.dragOver = false;
  }

  drop(event:DragEvent, list:BasicList) {
    event.preventDefault();
    this.dragLeave();
    this.dragEnd();

    let id = event.dataTransfer.getData('text');

    /* tslint:disable triple-equals */
    // Use coercion to filter.
    let mv = list.list.filter( (e:any) => e[this.key] == id );
    /* tslint:enable triple-equals */
    if (mv.length > 0) {
      for (let i = 0, len = mv.length; i < len; i += 1) {
        list.pick.push( mv[i] );
      }
    }
    if (list === this.available) {
      this.moveItem(this.available, this.confirmed);
    } else {
      this.moveItem(this.confirmed, this.available);
    }
  }

  trueUp() {
    let changed = false;

    // Clear removed items.
    let pos = this.destination.length;
    while ((pos -= 1) >= 0) {
      let mv = this.confirmed.list.filter( conf => {
        return conf._id === this.destination[pos][this.key];
      });

      if (mv.length === 0) {
        // Not found so remove.
        this.destination.splice(pos, 1);
        changed = true;
      }
    }

    // Push added items.
    for (let i = 0, len = this.confirmed.list.length; i < len; i += 1) {
      let mv = this.destination.filter( (d:any) => { return (d[this.key] === this.confirmed.list[i]._id); });

      if (mv.length === 0) {
        // Not found so add.
        mv = this.source.filter( (o:any) => { return (o[this.key] === this.confirmed.list[i]._id); });

        if (mv.length > 0) {
          this.destination.push(mv[0]);
          changed = true;
        }
      }
    }

    if (changed) {
      this.destinationChange.emit(this.destination);
    }
  }

  findItemIndex(list:Array<any>, item:any, key:any = '_id') {
    let idx = -1;

    // Assumption is that the arrays do not have duplicates.
    list.filter( (e:any) => {
      if (e._id === item[key]) {
        idx = list.indexOf(e);
        return true;
      }
      return false;
    });

    return idx;
  }

  moveItem(source:BasicList, target:BasicList, item:any = null) {
    let i = 0;
    let len = source.pick.length;

    if (item) {
      i = source.list.indexOf(item);
      len = i + 1;
    }

    for (; i < len; i += 1) {
      // Is the pick still in list?
      let mv:Array<any> = [];
      if (item) {
        let idx = this.findItemIndex(source.pick, item);
        if (idx !== -1) {
          mv[0] = source.pick[idx];
        }
      } else {
        mv = source.list.filter( src => {
          return (src._id === source.pick[i]._id);
        });
      }

      // Should only ever be 1
      if (mv.length === 1) {
        // Move if item wasn't already moved by drag-and-drop.
//				if (item && item[this.key] === mv[0][this.key]) {
        if (item && item._id === mv[0]._id) {
          target.list.push( mv[0] );
        } else {
          // see if it is already in target?
//					if ( target.list.filter( trg => { return trg[this.key] === mv[0][this.key]; }).length === 0) {
          if ( target.list.filter( trg => { return trg._id === mv[0]._id; }).length === 0) {
            target.list.push( mv[0] );
          }
        }

        // Make unavailable.
        let idx = source.list.indexOf( mv[0] );
        if (idx !== -1) {
          source.list.splice(idx, 1);
        }
      }
    }

    if (this.compare !== undefined) {
      target.list.sort(this.compare);
    }

    source.pick.length = 0;

    // Update destination
    this.trueUp();
  }


  isItemSelected(list:Array<any>, item:any) {
    if (list.filter( e => { return Object.is(e, item); }).length > 0) {
      return true;
    }
    return false;
  }

  shiftClick(event:MouseEvent, index:number, source:BasicList, item:any) {
    if (event.shiftKey && source.last && !Object.is(item, source.last)) {
      let idx = source.list.indexOf(source.last);
      if (index > idx) {
        for (let i = (idx + 1); i < index; i += 1) {
          this.selectItem(source.pick, source.list[i]);
        }
      } else if (idx !== -1) {
        for (let i = (index + 1); i < idx; i += 1)  {
          this.selectItem(source.pick, source.list[i]);
        }
      }
    }
    source.last = item;
  }

  selectItem(list:Array<any>, item:any) {
    let pk = list.filter( (e:any) => {
      return Object.is(e, item);
    });
    if (pk.length > 0) {
      // Already in list, so deselect.
      for (let i = 0, len = pk.length; i < len; i += 1) {
        let idx = list.indexOf(pk[i]);
        if (idx !== -1) {
          list.splice(idx, 1);
        }
      }
    } else {
      list.push(item);
    }
  }

  selectAll(source:BasicList) {
    source.pick.length = 0;
    source.pick = source.list.slice(0);
  }

  selectNone(source:BasicList) {
    source.pick.length = 0;
  }

  isAllSelected(source:BasicList) {
    if (source.list.length === 0 || source.list.length === source.pick.length) {
      return true;
    }
    return false;
  }

  isAnySelected(source:BasicList) {
    if (source.pick.length > 0) {
      return true;
    }
    return false;
  }

  // Allow for complex names by passing an array of strings.
  // Example: [display]="[ '_type.substring(0,1)', '_name' ]"
  makeName(item:any) : string {
    let str = '';

    if (this.display !== undefined) {
      if (Object.prototype.toString.call( this.display ) === '[object Array]' ) {

        for (let i = 0; i < this.display.length; i += 1) {
          if (str.length > 0) {
            str = str + '_';
          }

          if (this.display[i].indexOf('.') === -1) {
            // Simple, just add to string.
            str = str + item[this.display[i]];

          } else {
            // Complex, some action needs to be performed
            let parts = this.display[i].split('.');

            let s = item[parts[0]];
            if (s) {
              // Use brute force
              if (parts[1].indexOf('substring') !== -1) {
                let nums = (parts[1].substring(parts[1].indexOf('(') + 1, parts[1].indexOf(')'))).split(',');

                switch (nums.length) {
                  case 1:
                    str = str + s.substring(parseInt(nums[0], 10));
                    break;
                  case 2:
                    str = str + s.substring(parseInt(nums[0], 10), parseInt(nums[1], 10));
                    break;
                  default:
                    str = str + s;
                    break;
                }
              } else {
                // method not approved, so just add s.
                str = str + s;
              }
            }
          }
        }
        return str;
      } else {
        return item[this.display];
      }
    }

    switch (Object.prototype.toString.call(item)) {
      case '[object Number]':
        return item;
      case '[object String]':
        return item;
      default:
        if (item !== undefined) {
          return item[this.display];
        }
    }
  }
}
