'use strict';

var boostrapTab = angular.module('bootstrap.tabset', []);

boostrapTab.directive('tabset', function () {
  return {
    restrict: 'E',
    replace: true,
    transclude: true,

    controller: function ($scope) {
      $scope.templateUrl = '';
      $scope.clazz = $scope.clazz || 'tabs-style-line';

      var tabs = $scope.tabs = [];
      var controller = this;

      this.selectTab = function (tab) {
        angular.forEach(tabs, function (tab) {
          tab.selected = false;
        });
        tab.selected = true;
        if (tab.context) {
          $scope.$broadcast('context:changed', tab.context);
        }

      };

      this.setTabTemplate = function (templateUrl) {
        $scope.templateUrl = templateUrl;
      }

      this.addTab = function (tab) {
        if (tabs.length == 0) {
          controller.selectTab(tab);
        }
        tabs.push(tab);
      };
    },
    template: '<div >' +
    '' +
    '<ul ng-transclude class="nav nav-tabs"></ul>' +
    '' +
    '' +
    '<div class="tab-content row">' +
    '<ng-include src="templateUrl">' +
    '</ng-include>' +
    '</div>' +
    '</div>'
  };
});
boostrapTab.directive('tab', function () {
  return {
    restrict: 'E',
    replace: true,
    require: '^tabset',
    scope: {
      title: '@',
      templateUrl: '@',
      context: '=?context',
      icon: '=?icon'
    },
    link: function (scope, element, attrs, tabsetController) {
      tabsetController.addTab(scope);

      scope.select = function () {
        tabsetController.selectTab(scope);
      }

      scope.$watch('selected', function () {
        if (scope.selected) {
          tabsetController.setTabTemplate(scope.templateUrl);
        }
      });
    },
    template: '<li ng-class="{\'active\': selected}">' +
    '<a href="" ng-click="select()">' +
    '<i class="fa {{icon}}"></i>' +
    '<span> {{ title | capitalize }}</span>' +
    '</a>' +
    '</li>'
  };
});

export default boostrapTab.name;
