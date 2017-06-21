import {Component, Input, Output, EventEmitter, OnChanges, AfterViewChecked, ViewChild} from '@angular/core';
import {Select2OptionData} from 'ng2-select2';
import {ModalComponent} from 'ng2-bs3-modal/ng2-bs3-modal';

import * as $ from "jquery"
import {downgradeComponent} from '@angular/upgrade/static';

declare var angular:any
import edge = require("selenium-webdriver/edge");


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
  @Output() onSelectedElementRemoved = new EventEmitter();

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
  @ViewChild('dropPropertyModal') dropPropertyModal: ModalComponent;
  @ViewChild('dropClassModal') dropClassModal: ModalComponent;
  @ViewChild('dropEdgeInstanceModal') dropEdgeInstanceModal: ModalComponent;

  private tmpClassName;   // used during class renaming
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
  }

  ngOnChanges(changes) {

    if(changes.selectedElement && this.selectedElement) {
      this.updateSelectedElementInfo();
    }
  }

  getEdgeClassName(link) {

    var edgeClassName = undefined;
    var firstLevelKeys = Object.keys(link);

    for(var key of firstLevelKeys) {
      if(key !== 'source' && key !== 'target') {
        edgeClassName = key;
        break;
      }
    }

    return edgeClassName;
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
        this.edgeName = this.getEdgeClassName(this.selectedElement);

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


  canBeExcluded(propertyName) {

    if(this.selectedElement.externalKey) {

      // looking for the property among the current selected element's external key list.

      for (var currExternalKey of this.selectedElement.externalKey) {
        if (currExternalKey === propertyName) {
          return false;
        }
      }
      return true;
    }
    else {
      return true;
    }

  }


  /**
   * ----------------------------------------------------------
   *
   *                      Modals' methods
   *
   * ----------------------------------------------------------
   */

  /**
   * It prepares and opens a drop property modal.
   */
  prepareAndOpenDropPropertyModal(propertyName) {

    // setting the propertyName in the temporary property definition
    this.tmpPropertyDefinition.name = propertyName;

    // opening the modal
    this.dropPropertyModal.open();
  }

  /**
   * It's called when a drop property modal closing or dismissing occurs.
   * It cleans the temporary property definition and closes/dismisses the modal according to the action passed as param.
   */
  dismissOrCloseDropPropModal(action) {

    if(action === 'close') {

      this.dropPropertyFromSelectedClass();

      // closing the modal
      this.dropPropertyModal.close();
    }
    else if(action === 'dismiss') {
      // dismissing the modal
      this.dropPropertyModal.dismiss();
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
   * If we are dropping a property in an edge class, it will deleted automatically from all the instances of the current selected edge class.
   */
  dropPropertyFromSelectedClass() {

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
    else {

      // dropping the property from all the instances of the current selected edge class
      for(var edge of this.modellingConfig.edges) {
        if (this.getEdgeClassName(edge) === this.edgeName) {

          for (var currentPropName in edge[this.edgeName].properties) {
            if (currentPropName === propertyNameToDrop) {
              if (edge[this.edgeName].properties[currentPropName].mapping) {
                // removing a property present in the source table means excluding it
                edge[this.edgeName].properties[currentPropName].include = false;
              }
              else {
                // removing a property NOT present in the source table means deleting it
                delete edge[this.edgeName].properties[currentPropName];
              }
              break;
            }
          }
        }
      }
    }

    this.updateSelectedElementInfo();   // maybe not needed
  }

  /**
   * It drops the class corresponding to the selected element.
   */
  dropClass() {

    var className;
    var classType;

    // setting the selected element and the current edge name (if we are dropping an edge class) to undefined

    if(this.selectedElement.name) {
      // removing a vertex class
      className = this.selectedElement.name;
      classType = "vertexClass";
    }
    else {
      // removing an edge class
      className = this.getEdgeClassName(this.selectedElement);
      classType = "edgeClass";
      this.edgeName = undefined;
    }

    // cleaning the current selected element
    this.selectedElement = undefined;

    // emitting event for the graph update
    this.onSelectedElementRemoved.emit({className: className, classType: classType});
  }

  /**
   * It drops the current selected edge class instance.
   */
  dropEdgeClassInstance() {

    var className = this.getEdgeClassName(this.selectedElement);
    var sourceName = this.selectedElement.source.name;
    var targetName = this.selectedElement.target.name;
    this.edgeName = undefined;

    // cleaning the current selected element
    this.selectedElement = undefined;

    // emitting event for the graph update
    this.onSelectedElementRemoved.emit({dropEdgeInstance: true, edgeClassName: className, sourceName: sourceName, targetName: targetName});
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

  /**
   * Renames the selected vertex class.
   */
  renameSelectedVertexClass() {

    var oldVertexName = this.selectedElement.name;
    this.selectedElement.name = this.tmpClassName;
    this.onElementRenamed.emit({oldClassName: oldVertexName, newClassName: this.tmpClassName, classType: "vertexClass"});

    this.tmpClassName = undefined;    // maybe not needed, tmpClassName overwritten in prepareAndOpenRenameClassModal !!! check
    this.updateSelectedElementInfo();

    this.renameModal.close();
  }

  /**
   * Renames the selected edge class, automatically updating all the instances of the class.
   */
  renameSelectedEdgeClass() {

    var oldEdgeName = this.getEdgeClassName(this.selectedElement);
    var edgeDef = undefined;

    // update potential all the instances of the edge class
    for(var i=0; i<this.modellingConfig.edges.length; i++) {
      var currEdge = this.modellingConfig.edges[i];
      var currEdgeName = this.getEdgeClassName(currEdge);
      if(oldEdgeName === currEdgeName) {
        edgeDef = currEdge[currEdgeName];
        delete currEdge[currEdgeName];
        currEdge[this.tmpClassName] = edgeDef;
      }
    }

    // emitting event for the graph update
    this.onElementRenamed.emit({oldClassName: oldEdgeName, newClassName: this.tmpClassName, classType: "edgeClass"});

    this.tmpClassName = undefined;    // maybe not needed, tmpClassName overwritten in prepareAndOpenRenameClassModal !!!! check
    this.updateSelectedElementInfo();
    this.renameModal.close();
  }


  /**
   * It prepares and opens a drop class modal.
   */
  prepareAndOpenDropClassModal() {

    // opening the modal
    this.dropClassModal.open();
  }

  prepareAndOpenDropEdgeModal() {
    // opening the modal
    this.dropEdgeInstanceModal.open();
  }

  /**
   * It's called when a drop class modal closing or dismissing occurs.
   * It drops the selected class and closes/dismisses the modal.
   */
  dismissOrCloseDropClassModal(action) {

    if(action === 'close') {

      this.dropClass();

      // closing the modal
      this.dropClassModal.close();
    }
    else if(action === 'dismiss') {
      // dismissing the modal
      this.dropClassModal.dismiss();
    }
  }

  /**
   * It's called when a drop class modal closing or dismissing occurs.
   * It drops the selected edge class instance and closes/dismisses the modal.
   */
  dismissOrCloseDropEdgeInstanceModal(action) {

    if(action === 'close') {

      this.dropEdgeClassInstance();

      // closing the modal
      this.dropEdgeInstanceModal.close();
    }
    else if(action === 'dismiss') {

      // dismissing the modal
      this.dropEdgeInstanceModal.dismiss();
    }
  }

  /**
   * It opens the 'add property' Modal
   */
  prepareAndOpenAddPropertyModal() {

    // opening the modal
    this.addPropertyModal.open();
  }

  /**
   * Add the defined property in the selected class, so if the current selected element is an edge the property will be added:
   * - in the current selected element
   * - in all the other instances of the selected edge class
   */
  addNewPropToSelectedClass() {

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
      else {
        // setting the newProp in an edgeClass

        // setting the property in all the instances of the current selected edge class
        for(var edge of this.modellingConfig.edges) {
          if(this.getEdgeClassName(edge) === this.edgeName) {

            // if properties field is not present, it will be added
            if(!edge[this.edgeName].properties) {
              edge[this.edgeName].properties = {};
            }
            edge[this.edgeName].properties[this.tmpPropertyDefinition.name] = newProp;

          }
        }
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
   * If we are editing a property in an edge class, it will updated automatically in all the instances of the current selected edge class.
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

      // if the current editing property is an external key property, update it also the other JSON declaration
      for(var i=0; i<this.selectedElement.externalKey.length; i++) {
        var currExternalKey = this.selectedElement.externalKey[i];
        if(oldPropertyName === currExternalKey) {
          // update it with the new property name
          this.selectedElement.externalKey[i] = newPropertyName;
          break;
        }
      }
    }
    else {      // editing a property in an edge class

      for(var edge of this.modellingConfig.edges) {
        if(this.getEdgeClassName(edge) === this.edgeName) {

          // deleting old property (delete is needed in case of property renaming)
          delete edge[this.edgeName].properties[oldPropertyName];

          // copying temporary property definition into the current property being edited
          edge[this.edgeName].properties[newPropertyName] = JSON.parse(JSON.stringify(this.tmpPropertyDefinition));
        }
      }
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
