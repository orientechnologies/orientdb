import '../views/modal/yesno.html';


if (!String.prototype.startsWith) {
  Object.defineProperty(String.prototype, 'startsWith', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: function (searchString, position) {
      position = position || 0;
      return this.indexOf(searchString, position) === position;
    }
  });
}
if (!String.prototype.contains)
  String.prototype.contains = function (str, startIndex) {
    return -1 !== String.prototype.indexOf.call(this, str, startIndex);
  };
var Utilities = {};


Utilities.confirm = function ($scope, $modal, $q, params) {


  if (params) {
    var modalScope = $scope.$new(true);
    modalScope.title = params.title;
    modalScope.msg = params.body;


    var modalPromise = $modal({
      templateUrl: 'views/modal/yesno.html',
      persist: true,
      show: false,
      scope: modalScope,
      modalClass: ''
    });

    modalPromise.$promise.then(function () {
      modalPromise.show();
      modalScope.confirm = function () {
        params.success();
        modalPromise.hide();
      }
    });

  }
}


export  default Utilities;

