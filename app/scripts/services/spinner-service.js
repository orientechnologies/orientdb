/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 26/08/13
 * Time: 13.55
 * To change this template use File | Settings | File Templates.
 */


var spinner = angular.module('spinner.services', []);

spinner.factory('Spinner',function(){

    var spinner = {
        loading : false
    };
    return spinner;
});