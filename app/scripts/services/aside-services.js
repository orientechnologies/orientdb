var aside = angular.module('aside.services', []);

aside.factory('Aside', function () {

    var params = {
        cls: ""
    }
    return {
        params: params,
        show: function (params) {


            this.params.scope = params.scope;
            this.params.tpl = params.template;
            this.params.title = params.title;

            if (params.show)
                this.params.cls = 'show';
            this.params.loading = true;
            if (!this.params.scope.$$phase && !this.params.scope.$root.$$phase) {
                this.params.scope.$apply();
            }
        },
        hide: function () {
            this.params.cls = "";
        },
        toggle: function () {
            this.params.cls = (this.params.cls == "" ? "show" : "");

        },
        isOpen: function () {
            return this.params.cls == "show";
        }

    }
});