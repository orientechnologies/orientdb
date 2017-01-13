import InitClasses from './classesList';
import InitVertexModal from './vertex/newVertexModal';
import InitEdgeModal from './edge/newEdgeModal';
import InitGenericModal from './generic/newGenericModal';


let schemaModule = angular.module('schema.components', ['database.services']);

InitClasses(schemaModule);
InitVertexModal(schemaModule);
InitEdgeModal(schemaModule);
InitGenericModal(schemaModule);


export default schemaModule;
