var appServices = angular.module('orientdb.directives', ['ngResource']);


appServices.provider('$odialog', function () {

    var $pdialogProvider = {

        $get: function ($q, $modal, $rootScope) {

            var $odialog = {};

            $odialog.confirm = function (params) {
                if (params) {
                    var modalScope = $rootScope.$new(true);
                    modalScope.title = params.title;
                    modalScope.msg = params.body;
                    modalScope.confirm = function () {
                        params.success();
                        modalScope.hide();
                    }

                    var modalPromise = $modal({template: 'views/modal/yesno.html', persist: true, show: false, backdrop: 'static', scope: modalScope, modalClass: ''});
                    $q.when(modalPromise).then(function (modalEl) {
                        modalEl.modal('show');
                    });

                }
            }

            return $odialog;
        }
    }


    return $pdialogProvider;
});