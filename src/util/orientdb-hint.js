/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 29/08/13
 * Time: 17.30
 * To change this template use File | Settings | File Templates.
 */

let OrientDBHint = (function () {
  "use strict";


  CodeMirror.commands.autocomplete = function (cm) {
    CodeMirror.showHint(cm, CodeMirror.hint.sql);
  }
  var Pos = CodeMirror.Pos;

  function forEach(arr, f, render) {
    for (var i = 0, e = arr.length; i < e; ++i) f(arr[i], render);
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

  function scriptHint(editor, keywords, getToken, options) {
    // Find the token at the cursor
    var cur = editor.getCursor(), token = getToken(editor, cur), tprop = token;
    token.state = CodeMirror.innerMode(editor.getMode(), token.state).state;
    var context = undefined;
    return {
      list: getCompletions(editor, token, context, keywords, options),
      from: Pos(cur.line, token.start),
      to: Pos(cur.line, token.end)
    };
  }

  function sqlHint(editor, options) {
    return scriptHint(editor, sqlKeywords,
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
  var sqlKeywords = ("and select from where in delete create alter delete insert vertex edge" +
  "grant revoke drop rebuild index truncate property traverse explain between contains").split(" ");


  function getCompletions(editor, token, context, keywords, options) {
    var found = [], start = token.string.trim();

    function maybeAdd(str, render) {
      var obj = {};
      obj.displayText = str;
      obj.text = str;
      obj.render = render;
      if (str.toLowerCase().indexOf(start.toLowerCase()) == 0 && !arrayContains(found, obj)) found.push(obj);
    }

    // If not, just look in the window object and any local scope
    // (reading into JS mode internals to get at the local and global variables)
    var classes = editor.getOption('metadata').listNameOfClasses();
    var props = editor.getOption('metadata').listNameOfProperties();
    var classesDef = new Array;
    var propsDef = new Array;
    classes.forEach(function (elem, idx, arr) {
      if (classesDef.indexOf(elem) == -1) {
        classesDef.push(elem);
      }
    });
    props.forEach(function (elem, idx, arr) {
      if (propsDef.indexOf(elem) == -1) {
        propsDef.push(elem);
      }
    });
    forEach(keywords, maybeAdd, renderKeyword);
    forEach(classesDef, maybeAdd, renderClass);
    forEach(propsDef, maybeAdd, renderField);
    found.sort(function (a, b) {
      if (a.text.toLowerCase() < b.text.toLowerCase()) return -1;
      if (a.text.toLowerCase() > b.text.toLowerCase()) return 1;
      return 0;
    });

    return found;
  }

  function renderKeyword(etl, data, cur) {
    //etl.appendChild(createLabel("(K)  "));
    etl.appendChild(document.createTextNode(cur.text));

  }

  function createLabel(label) {
    var italic = document.createElement('i');
    italic.innerHTML = label;
    return italic;
  }

  function renderClass(etl, data, cur) {
    etl.appendChild(document.createTextNode(cur.text));
    etl.appendChild(createLabel("  (C)"));
  }

  function renderField(etl, data, cur) {
    etl.appendChild(document.createTextNode(cur.text));
    etl.appendChild(createLabel("  (F)"));
  }
})();


export default OrientDBHint;
