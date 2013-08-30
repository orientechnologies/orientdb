/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 29/08/13
 * Time: 17.30
 * To change this template use File | Settings | File Templates.
 */

(function () {
    CodeMirror.commands.autocomplete = function (cm) {
        CodeMirror.showHint(cm, CodeMirror.hint.sql);
    }
    var Pos = CodeMirror.Pos;

    function forEach(arr, f) {
        for (var i = 0, e = arr.length; i < e; ++i) f(arr[i]);
    }

    function arrayContains(arr, item) {
        if (!Array.prototype.indexOf) {
            var i = arr.length;
            while (i--) {
                if (arr[i] === item) {
                    return true;
                }
            }
            return false;
        }
        return arr.indexOf(item) != -1;
    }

    function scriptHint(editor, keywords,props, getToken, options) {
        // Find the token at the cursor
        var cur = editor.getCursor(), token = getToken(editor, cur), tprop = token;
        token.state = CodeMirror.innerMode(editor.getMode(), token.state).state;
        var context = undefined;
        return {list: getCompletions(token, context, keywords,props, options),
            from: Pos(cur.line, token.start),
            to: Pos(cur.line, token.end)};
    }

    function sqlHint(editor, options) {
        console.log("HINT");
        var props = editor.getOption('metadata').listNameOfClasses();

        props = props.concat(editor.getOption('metadata').listNameOfProperties());
        return scriptHint(editor, sqlKeywords,props,
            function (e, cur) {
                return e.getTokenAt(cur);
            },
            options);
    };
    CodeMirror.registerHelper("hint", "sql", sqlHint);


    var stringProps = ("charAt charCodeAt indexOf lastIndexOf substring substr slice trim trimLeft trimRight " +
        "toUpperCase toLowerCase split concat match replace search").split(" ");
    var arrayProps = ("length concat join splice push pop shift unshift slice reverse sort indexOf " +
        "lastIndexOf every some filter forEach map reduce reduceRight ").split(" ");
    var funcProps = "prototype apply call bind".split(" ");
    var sqlKeywords = ("and select from where in delete create alter delete insert vertex edge " +
        "grant revoke drop rebuild index truncate property traverse explain between contains").split(" ");


    function getCompletions(token, context, keywords,props, options) {
        var found = [], start = token.string.trim();

        function maybeAdd(str) {
            if (str.toLowerCase().indexOf(start.toLowerCase()) == 0 && !arrayContains(found, str)) found.push(str);
        }

        // If not, just look in the window object and any local scope
        // (reading into JS mode internals to get at the local and global variables)

        forEach(keywords, maybeAdd);
        forEach(props, maybeAdd);
        found.sort();
        return found;
    }
})();

