import {Component, Input, Output, EventEmitter, OnChanges, AfterViewChecked, ViewChild} from '@angular/core';
import {Select2OptionData} from 'ng2-select2';
import {ModalComponent} from 'ng2-bs3-modal/ng2-bs3-modal';

import * as $ from "jquery"
import {downgradeComponent} from '@angular/upgrade/static';

declare var angular:any
import * as d3 from 'd3';
import {first} from "rxjs/operator/first";


@Component({
  selector: 'detail-panel',
  templateUrl: "./detailpanel.component.html",
  styles: [
    '.propAndColumnTable {max-width: 480px; table-layout: fixed}',
    '.propAndColumnTable td, .propAndColumnTable th {overflow-x:auto; height: 40px; text-align: center; vertical-align: middle;}',
    '.firstColumn {width: 50%;}',
    '.secondColumn {width: 25%;}',
    '.thirdColumn {width: 25%;}',

    '.hideOverflow {max-width: 100%; overflow:hidden; white-space:nowrap; text-overflow:ellipsis; display: inline-block}',

    '.OneNRelTable {width: 150px; empty-cells: show}',
    '.NNRelTable {width: 100px; empty-cells: show}',
    '.OneNRelInfoTable {width: 150px; empty-cells: show;table-layout:fixed;}',
    '.NNRelInfoTable {width: 100px; empty-cells: show; table-layout:fixed;}',
    '.OneNRelTableLabel {width: 150px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; display: inline-block;}',
    '.NNRelTableLabel {width: 100px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; display: inline-block;}',
    '.OneNRelInfoTable ul, .NNRelInfoTable ul {padding-left: 2px;}',
    '.OneNRelInfoTable li, .NNRelInfoTable li {padding-left: 2px;list-style-type: disc; list-style-position: inside; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }',

    '.actions-dropdown-item:hover {cursor: pointer}',
    'modal .control-label {text-align: right}'
  ]
})

class DetailPanelComponent implements OnChanges, AfterViewChecked {

  @Input() modellingConfig = this.modellingConfig !== 'undefined' ? this.modellingConfig : 'no config from parent.';
  @Output() onElementRenamed = new EventEmitter();

  @Input() selectedElement = this.selectedElement !== 'undefined' ? this.selectedElement : {name: undefined, properties: undefined};     // needed the second initialization because of renameModal

  private propertiesName;
  private edgeName;
  private selectedElementPropsUpdated:boolean = false;

  // 1-N Relationships Info
  private fromTableInfo;
  private toTableInfo;

  // N-N Relationship Info
  private leftTableInfo;
  private joinTableInfo;
  private rightTableInfo;

  // Modals' variables
  @ViewChild('renameModal') renameModal: ModalComponent;
  @ViewChild('addPropModal') addPropertyModal: ModalComponent;
  @ViewChild('editPropertyModal') editPropertyModal: ModalComponent;
  @ViewChild('dropModal') dropModal: ModalComponent;
  private tmpClassName;
  private tmpPropertyDefinition= {
    originalName: undefined,
    name: undefined,
    type: undefined,
    mandatory: false,
    readOnly: false,
    notNull: false,
    ordinalPosition: undefined
  };
  private fullListTypes = ['BINARY', 'BOOLEAN', 'BYTE', 'EMBEDDED', 'EMBEDDEDLIST', 'EMBEDDEDMAP', 'EMBEDDEDSET', 'DECIMAL', 'FLOAT', 'DATE', 'DATETIME', 'DOUBLE', 'INTEGER', 'LINK', 'LINKLIST', 'LINKMAP', 'LINKSET', 'LONG', 'SHORT', 'STRING'];
  private partialListTypes = ['BINARY', 'BOOLEAN', 'BYTE', 'DECIMAL', 'FLOAT', 'DATE', 'DATETIME', 'DOUBLE', 'INTEGER', 'LONG', 'SHORT', 'STRING'];
  private tmpIncludedPropertiesNames = [];
  private activeFormNewProp = '1';
  public excludedPropertiesName: Array<Select2OptionData>;
  public propsSelectOptions: Select2Options;
  public selectedPropertiesToInclude: string[];

  constructor() {
    this.edgeName = undefined;
    this.propertiesName = [];
    this.excludedPropertiesName = [];
    this.selectedPropertiesToInclude = [];
    this.propsSelectOptions = {
      multiple: true
    }
  }

  setSelectedElementPropsUpdated(updated:boolean) {
    this.selectedElementPropsUpdated = updated;
  }

  ngAfterViewChecked() {
    this.enablePopovers();

    // needed when properties of the current selected element are excluded through the checkbox
    if(this.selectedElementPropsUpdated) {
      this.updateSelectedElementInfo();
      this.selectedElementPropsUpdated = false;
    }
  }

  enablePopovers() {
    (<any>$('[data-toggle="popover"]')).popover();
    // (<any>$('[data-toggle="popover"]')).popover().css("max-width", "100%");

    // (<any>$('[data-toggle="popover"]')).on('shown.bs.popover', function () {
    //   $($(this)).css('max-width', 100);
    // });

    // $('[data-toggle="popover"]').on('shown.bs.popover', function () {
    //   console.log("Element:");
    //   console.log(this);
    //   console.log("Element next:");
    //   console.log($($(this).next()));
    //   console.log("Popover width:");
    //   console.log($($(this).next()).width());
    // });

    // $('[data-toggle="popover"]').on('shown.bs.popover', function () {
    //   var popoverID = $(this).attr('aria-describedby');
    //   $("#"+popoverID).css("max-width", "100%");
    //   console.log($(this));
    //   console.log($("#"+popoverID).width());
    // });
  }

  ngOnChanges(changes) {

    if(changes.selectedElement && this.selectedElement) {
      this.updateSelectedElementInfo();
    }
  }

  getEdgeNameFromSelectedElement() {

    var firstLevelProps = Object.keys(this.selectedElement);

    for(var i=0; i<firstLevelProps.length; i++) {
      if(firstLevelProps[i] !== 'source' && firstLevelProps[i] !== 'target') {
        return firstLevelProps[i];
      }
    }

    return undefined;

  }

  /**
   * Updates the set of properties to include back in a vertex or edge class when the input value changes.
   * @param e
   */
  updatedSelectedValue(e: any) {
    this.selectedPropertiesToInclude = e.value;
  }

  // called after the update of the current selected element: it updates all info about the selected element
  updateSelectedElementInfo() {

    if(this.selectedElement) {
      if (!this.selectedElement.name) {

        // when an edge is selected we store the edge name
        // this.edgeName = Object.keys(this.selectedElement)[0];
        this.edgeName = this.getEdgeNameFromSelectedElement();

        // filling properties array
        this.propertiesName = [];
        this.excludedPropertiesName = [];
        var props = this.selectedElement[this.edgeName].properties;
        var propsNames = Object.keys(props);

        for (var i = 0; i < propsNames.length; i++) {
          this.propertiesName.push(propsNames[i]);
          if (!props[propsNames[i]].include) {
            var currOption = {
              id: propsNames[i],
              text: propsNames[i]
            }
            this.excludedPropertiesName.push(currOption);
          }
        }
      }
      else {

        // when a vertex is selected we delete the edge name
        this.edgeName = undefined;

        // filling properties array
        this.propertiesName = [];
        this.excludedPropertiesName = [];
        var props = this.selectedElement.properties;
        var propsNames = Object.keys(props);

        for (var i = 0; i < propsNames.length; i++) {
          this.propertiesName.push(propsNames[i]);
          if (!props[propsNames[i]].include) {
            var currOption = {
              id: propsNames[i],
              text: propsNames[i]
            }
            this.excludedPropertiesName.push(currOption);
          }
        }
      }
    }
  }

  renderNNRelationshipInfo(relationship, direction) {

    if(direction === "left") {

      // deleting rightTableInfo
      this.rightTableInfo = undefined;

      this.leftTableInfo = {label: "ToColumns", columns: []};
      for (var i = 0; i < relationship.fromColumns.length; i++) {
        this.leftTableInfo.columns.push(relationship.fromColumns[i]);
      }
    }
    else if(direction === "right") {

      // deleting leftTableInfo
      this.leftTableInfo = undefined;

      this.rightTableInfo = {label: "ToColumns", columns: []};
      for (var i = 0; i < relationship.toColumns.length; i++) {
        this.rightTableInfo.columns.push(relationship.toColumns[i]);
      }
    }

    this.joinTableInfo = {label: "FromColumns", columns: []};

    if(direction === "left") {
      for (var i = 0; i < relationship.joinTable.fromColumns.length; i++) {
        this.joinTableInfo.columns.push(relationship.joinTable.fromColumns[i]);
      }
    }
    else if(direction === "right") {
      for (var i = 0; i < relationship.joinTable.toColumns.length; i++) {
        this.joinTableInfo.columns.push(relationship.joinTable.toColumns[i]);
      }
    }

  }

  changeSelectedElement(e) {

    // deleting info about Relationships
    this.leftTableInfo = undefined;
    this.joinTableInfo = undefined;
    this.rightTableInfo = undefined;
    this.fromTableInfo = undefined;
    this.toTableInfo = undefined;
  }

  render1NRelationshipInfo(relationship) {

    this.fromTableInfo = {label: "FromColumns", columns: []};
    for (var i = 0; i < relationship.fromColumns.length; i++) {
      this.fromTableInfo.columns.push(relationship.fromColumns[i]);
    }

    this.toTableInfo = {label: "ToColumns", columns: []};
    for (var i = 0; i < relationship.toColumns.length; i++) {
      this.toTableInfo.columns.push(relationship.toColumns[i]);
    }

  }

  /**
   * It calculates the next available property ordinal position for the current selected element.
   */
  calculateNextOrdinalPosition() {

    var maxOrdinalPosition = 0;

    if(this.selectedElement.properties) {
      var props = this.selectedElement.properties;
      var propsNames = Object.keys(props);

      for (var i = 0; i < propsNames.length; i++) {
        var currOrdinalPosition = this.selectedElement.properties[propsNames[i]].ordinalPosition;
        if (currOrdinalPosition > maxOrdinalPosition) {
          maxOrdinalPosition = currOrdinalPosition;
        }
      }

      return maxOrdinalPosition + 1;
    }
    else {
      return 1;
    }
  }

  /**
   * Sets the default values back for the temporary property definition
   */
  cleanTmpPropertyDefinition() {

    this.tmpPropertyDefinition = {
      originalName: undefined,
      name: undefined,
      type: undefined,
      mandatory: false,
      readOnly: false,
      notNull: false,
      ordinalPosition: undefined
    };
  }


  /**
   * ----------------------------------------------------------
   *
   *                      Modals' methods
   *
   * ----------------------------------------------------------
   */

  /**
   * It prepares and opens a drop modal.
   */
  prepareAndOpenDropModal(propertyName) {

    // setting the propertyName in the temporary property definition
    this.tmpPropertyDefinition.name = propertyName;

    // opening the modal
    this.dropModal.open();
  }

  /**
   * It's called when a drop modal closing or dismissing occurs.
   * It cleans the temporary property definition and close/dismiss the modal according to the action passed as param.
   */
  dismissOrCloseDropModal(action) {

    if(action === 'close') {

      this.dropPropertyFromSelectedElement();

      // closing the modal
      this.dropModal.close();
    }
    else if(action === 'dismiss') {
      // dismissing the modal
      this.dropModal.dismiss();
    }

    // setting default values back for the temporary property definition
    this.tmpPropertyDefinition = {
      originalName: undefined,
      name: undefined,
      type: undefined,
      mandatory: false,
      readOnly: false,
      notNull: false,
      ordinalPosition: undefined
    };

    this.cleanTmpPropertyDefinition();
  }

  /**
   * It drops a property from the selected element according to the property name of the temporary property definition.
   * Then it opens the modal.
   */
  dropPropertyFromSelectedElement() {

    var propertyNameToDrop = this.tmpPropertyDefinition.name;

    if(this.selectedElement.name) {     // dropping from a vertex class

      for (var currentPropName in this.selectedElement.properties) {
        if (currentPropName === propertyNameToDrop) {
          if (this.selectedElement.properties[currentPropName].mapping) {
            // removing a property present in the source table means excluding it
            this.selectedElement.properties[currentPropName].include = false;
          }
          else {
            // removing a property NOT present in the source table means deleting it
            delete this.selectedElement.properties[currentPropName];
          }
          break;
        }
      }
    }
    else {      // dropping from an edge class
      for (var currentPropName in this.selectedElement[this.edgeName].properties) {
        if (currentPropName === propertyNameToDrop) {
          if (this.selectedElement[this.edgeName].properties[currentPropName].mapping) {
            // removing a property present in the source table means excluding it
            this.selectedElement[this.edgeName].properties[currentPropName].include = false;
          }
          else {
            // removing a property NOT present in the source table means deleting it
            delete this.selectedElement[this.edgeName].properties[currentPropName];
          }
          break;
        }
      }
    }

    this.updateSelectedElementInfo();   // maybe not needed
  }

  /**
   * It prepares the 'rename class' modal
   */
  prepareAndOpenRenameClassModal(className) {

    // setting the class name in the temporary class name
    this.tmpClassName = className;

    // opening the modal
    this.renameModal.open();
  }

  renameSelectedVertexClass() {

    var oldVertexName = this.selectedElement.name;
    this.selectedElement.name = this.tmpClassName;
    this.onElementRenamed.emit({oldClassName: oldVertexName, newClassName: this.tmpClassName, classType: "vertexClass"});

    this.tmpClassName = undefined;    // maybe not needed, tmpClassName overwritten in prepareAndOpenRenameClassModal !!! check
    this.updateSelectedElementInfo();

    this.renameModal.close();
  }

  renameSelectedEdgeClass() {

    var oldEdgeName = Object.keys(this.selectedElement)[0];
    var edgeDef = this.selectedElement[oldEdgeName];
    delete this.selectedElement[oldEdgeName];

    this.selectedElement[this.tmpClassName] = edgeDef;
    this.onElementRenamed.emit({oldClassName: oldEdgeName, newClassName: this.tmpClassName, classType: "edgeClass"});

    this.tmpClassName = undefined;    // maybe not needed, tmpClassName overwritten in prepareAndOpenRenameClassModal !!!! check
    this.updateSelectedElementInfo();

    this.renameModal.close();
  }


  /**
   * It opens the 'add property' Modal
   */
  prepareAndOpenAddPropertyModal() {

    // opening the modal
    this.addPropertyModal.open();
  }


  addNewPropToSelectedElement() {

    if (this.activeFormNewProp === '1') {
      // property to be added in the selected element properties list
      var newProp = {include: true, type: undefined, mandatory: undefined, readOnly: undefined, notNull: undefined, ordinalPosition: undefined};
      newProp.type = this.tmpPropertyDefinition.type;
      newProp.mandatory = this.tmpPropertyDefinition.mandatory;
      newProp.readOnly = this.tmpPropertyDefinition.readOnly;
      newProp.notNull = this.tmpPropertyDefinition.notNull;
      newProp.ordinalPosition = this.calculateNextOrdinalPosition();

      if(this.selectedElement.name) {     // setting the newProp in a vertex class

        // if properties field is not present, it will be added
        if(!this.selectedElement.properties) {
          this.selectedElement.properties = {};
        }
        this.selectedElement.properties[this.tmpPropertyDefinition.name] = newProp;
      }
      else {      // setting the newProp in an edgeClass
        // if properties field is not present, it will be added
        if(!this.selectedElement[this.edgeName].properties) {
          this.selectedElement[this.edgeName].properties = {};
        }
        this.selectedElement[this.edgeName].properties[this.tmpPropertyDefinition.name] = newProp;
      }
      this.cleanTmpPropertyDefinition();
    }
    else if(this.activeFormNewProp === '2') {
      for(var currProp of this.excludedPropertiesName) {

        if(this.selectedPropertiesToInclude.indexOf(currProp.id) >= 0) {       // if current excluded properties is contained in the selected properties to include again then...
          if (this.selectedElement.name) {     // including the prop in a vertex class
            this.selectedElement.properties[currProp.id].include = true;
          }
          else {      // including the prop in an edge class
            this.selectedElement[this.edgeName].properties[currProp.id].include = true;
          }
        }
      }
      // the just included prop will be removed from excludedProperties during the updateSelectElementInfo() method execution

      // setting default input value
      this.selectedPropertiesToInclude = [];
    }

    //setting again the default choose for the modal
    this.activeFormNewProp = '1';

    // setting default values back for the temporary property definition
    this.cleanTmpPropertyDefinition();

    this.updateSelectedElementInfo();

    // closing modal
    this.addPropertyModal.close();
  }

  dismissPropertyAdding() {

    // setting again the default choose for the modal
    this.activeFormNewProp = '1';

    // setting default input value
    this.selectedPropertiesToInclude = [];

    // setting default values back for the temporary property definition
    this.cleanTmpPropertyDefinition();

    // dismissing modal
    this.addPropertyModal.dismiss();
  }

  /**
   * It prepares the info for the 'Edit Property Modal', then it open it:
   * - the current property is copied in a temporary property definition
   * - the modal is opened
   */
  prepareAndOpenPropertyEditingModal(propertyName) {

    var property;
    if(this.selectedElement.name) {      // preparing property editing in a vertex class
      property = this.selectedElement.properties[propertyName];
    }
    else {      // preparing property editing in an edge class
      property = this.selectedElement[this.edgeName].properties[propertyName];
    }

    // cloning original property into temporary property definition
    this.tmpPropertyDefinition = JSON.parse(JSON.stringify(property));
    this.tmpPropertyDefinition.name = propertyName;
    this.tmpPropertyDefinition.originalName = propertyName;

    // opening modal
    this.editPropertyModal.open();
  }

  /**
   * When the 'Edit Property Modal' is dismissed this method in invoked to clean the temporary property definition
   * and close the modal.
   */
  dismissEditPropertyModal() {

    // setting default values back for the temporary property definition
    this.cleanTmpPropertyDefinition();

    // closing the modal
    this.editPropertyModal.close();
  }

  /**
   * It copies the temporary property definition into the current property being edited,
   * then it closes the modal.
   */
  closeEditPropertyModal() {

    var newPropertyName = this.tmpPropertyDefinition.name;
    var oldPropertyName = this.tmpPropertyDefinition.originalName;

    // deleting 'name' and 'originalName' fields from the temporary property definition (so when copied in the selected element the fields won't be present)
    delete this.tmpPropertyDefinition.originalName;
    delete this.tmpPropertyDefinition.name;

    if(this.selectedElement.name) {     // editing a property in a vertex class

      // deleting old property (delete is needed in case of property renaming)
      delete this.selectedElement.properties[oldPropertyName];

      // copying temporary property definition into the current property being edited
      this.selectedElement.properties[newPropertyName] = JSON.parse(JSON.stringify(this.tmpPropertyDefinition));
    }
    else {      // editing a property in an edge class

      // deleting old property (delete is needed in case of property renaming)
      delete this.selectedElement[this.edgeName].properties[oldPropertyName];

      // copying temporary property definition into the current property being edited
      this.selectedElement[this.edgeName].properties[newPropertyName] = JSON.parse(JSON.stringify(this.tmpPropertyDefinition));
    }

    // setting default values back for the temporary property definition
    this.cleanTmpPropertyDefinition();

    // closing the modal
    this.editPropertyModal.close();

    this.updateSelectedElementInfo();
  }

}

angular.module('detailpanel.component', []).directive(
  `detail-panel`,
  downgradeComponent({component: DetailPanelComponent}));


export {DetailPanelComponent};
