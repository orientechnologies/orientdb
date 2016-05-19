var filterModule = angular.module('filters', [])
filterModule.filter('capitalize', function () {
  return function (input, scope) {
    if (input != null)
      return input.substring(0, 1).toUpperCase() + input.substring(1);
  }
});
filterModule.filter('checkmark', function () {
  return function (input) {
    return input ? '\u2713' : '\u2718';
  };
})
filterModule.filter('sizeFormat', function () {

  return function (size) {

    if (size) {
      if (size > 1000000000) {
        return Math.round(size / 10000000) + " Tb";
      }
      else if (size > 1000000) {
        return Math.round(size / 1000000) + " Mb";
      }
      else if (size > 1000) {

        return Math.round(size / 1000) + " Kb";
      }
      return size + " b";
    }
    return size;
  };
});
filterModule.filter('nograph', function () {
  return function (input) {
    if (input.startsWith("in_")) {
      return input.replace("in_", "");
    }
    if (input.startsWith("out_")) {
      return input.replace("out_", "");
    }
    return input;

  }
});
filterModule.filter('nocomment', function () {
  return function (input) {
    return input.replace(/(\/\*([\s\S]*?)\*\/)|(\/\/(.*)$)/gm, '');
  }
});


filterModule.filter('ctype', function () {

  return function (input, args) {
    var index = input.indexOf(".");
    return input.substring(0, index);
  };
});
filterModule.filter('cname', function () {

  return function (input, args) {
    var index = input.indexOf(".");
    return input.substring(index + 1, input.length);
  };
});


filterModule.filter('formatError', function () {
  return function (input) {
    if (typeof input == 'string') {
      return input;
    } else if (typeof  input == 'object') {
      return input.errors[0].content;
    } else {
      return input;
    }

  }
})
filterModule.filter('classRender', function () {
  return function (input, args) {
    if (args == '@class') {
      return input;
    } else {
      return input;
    }

  }
})
filterModule.filter('formatArray', function () {
  return function (input) {
    if (input instanceof Array) {
      var output = "";
      input.forEach(function (e, idx, arr) {
        output += (idx > 0 ? ", " : " ") + e;
      })
      return output;
    } else {
      return input;
    }
  }
})
filterModule.filter('formatDate', function () {
  return function (input) {
    return moment(input).format('D/M/YY - H:mm:ss');
  }
})

filterModule.filter('formatDateNS', function () {
  return function (input) {
    return moment(input).format('D/M/YY - H:mm');
  }
})
filterModule.filter('operation', function () {

  return function (input, args) {
    switch (input) {
      case 0:
        return "Read"
        break;
      case 1:
        return "Update"
        break;
      case 2:
        return "Delete"
        break;
      case 3:
        return "Create"
        break;
      case 4:
        return "Command"
        break;
      case 5:
        return "Create Class"
        break;
      case 6:
        return "Drop Class"
        break;
    }
    return input;
  };
});

filterModule.filter('qtype', function () {

  return function (input, args) {
    var index = input.indexOf(".");
    var lastIndex = input.lastIndexOf('.');
    return input.substring(index + 1, lastIndex);
  };
});


filterModule.filter('toSizeString', function () {
  return function (input, args) {
    if (input != null) {
      return filesize(input);
    }
    return input;

  }
})
