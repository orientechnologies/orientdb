var aside = angular.module('aside.services', []);

notification.factory('Aside', function ($aside) {

    return {
        aside: null,

        show: function (params) {
            if (!params && aside) {
                //aside.show();
            } else {
                aside = $aside({ scope: params.scope, template: params.template, show: params.show, placement: 'left', animation: 'am-slide-left'})
            }
        },
        hide: function () {
            if (aside) {
                aside.$hide();
            }
        },
        toggle: function () {
            if (aside) {
                aside.toggle();
            }
        }

    }
});