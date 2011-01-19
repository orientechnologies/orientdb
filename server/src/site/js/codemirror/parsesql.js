var SqlParser = Editor.Parser = (function() {

  function wordRegexp(words) {
    return new RegExp("^(?:" + words.join("|") + ")$", "i");
  }

  var functions = wordRegexp([
    "abs", "acos", "adddate", "aes_encrypt", "aes_decrypt", "ascii",
    "asin", "atan", "atan2", "avg", "benchmark", "bin", "bit_and",
    "bit_count", "bit_length", "bit_or", "cast", "ceil", "ceiling",
    "char_length", "character_length", "coalesce", "concat", "concat_ws",
    "connection_id", "conv", "convert", "cos", "cot", "count", "curdate",
    "current_date", "current_time", "current_timestamp", "current_user",
    "curtime", "database", "date_add", "date_format", "date_sub",
    "dayname", "dayofmonth", "dayofweek", "dayofyear", "decode", "degrees",
    "des_encrypt", "des_decrypt", "elt", "encode", "encrypt", "exp",
    "export_set", "extract", "field", "find_in_set", "floor", "format",
    "found_rows", "from_days", "from_unixtime", "get_lock", "greatest",
    "group_unique_users", "hex", "ifnull", "inet_aton", "inet_ntoa", "instr",
    "interval", "is_free_lock", "isnull", "last_insert_id", "lcase", "least",
    "left", "length", "ln", "load_file", "locate", "log", "log2", "log10",
    "lower", "lpad", "ltrim", "make_set", "master_pos_wait", "max", "md5",
    "mid", "min", "mod", "monthname", "now", "nullif", "oct", "octet_length",
    "ord", "password", "period_add", "period_diff", "pi", "position",
    "pow", "power", "quarter", "quote", "radians", "rand", "release_lock",
    "repeat", "reverse", "right", "round", "rpad", "rtrim", "sec_to_time",
    "session_user", "sha", "sha1", "sign", "sin", "soundex", "space", "sqrt",
    "std", "stddev", "strcmp", "subdate", "substring", "substring_index",
    "sum", "sysdate", "system_user", "tan", "time_format", "time_to_sec",
    "to_days", "trim", "ucase", "unique_users", "unix_timestamp", "upper",
    "user", "version", "week", "weekday", "yearweek"
  ]);

  var keywords = wordRegexp([
    "alter", "grant", "revoke", "primary", "key", "table", "start", "top",
    "transaction", "select", "update", "insert", "delete", "create", "describe",
    "from", "into", "values", "where", "join", "inner", "left", "natural", "and",
    "or", "in", "not", "xor", "like", "using", "on", "order", "group", "by",
    "asc", "desc", "limit", "offset", "union", "all", "as", "distinct", "set",
    "commit", "rollback", "replace", "view", "database", "separator", "if",
    "exists", "null", "truncate", "status", "show", "lock", "unique", "having",
    "drop", "procedure", "begin", "end", "delimiter", "call", "else", "leave", 
    "declare", "temporary", "then"
  ]);

  var types = wordRegexp([
    "bigint", "binary", "bit", "blob", "bool", "char", "character", "date",
    "datetime", "dec", "decimal", "double", "enum", "float", "float4", "float8",
    "int", "int1", "int2", "int3", "int4", "int8", "integer", "long", "longblob",
    "longtext", "mediumblob", "mediumint", "mediumtext", "middleint", "nchar",
    "numeric", "real", "set", "smallint", "text", "time", "timestamp", "tinyblob",
    "tinyint", "tinytext", "varbinary", "varchar", "year"
  ]);

  var operators = wordRegexp([
    ":=", "<", "<=", "==", "<>", ">", ">=", "like", "rlike", "in", "xor", "between"
  ]);

  var operatorChars = /[*+\-<>=&|:\/]/;

  var CFG = {};

  var tokenizeSql = (function() {
    function normal(source, setState) {
      var ch = source.next();
      if (ch == "@" || ch == "$") {
        source.nextWhileMatches(/[\w\d]/);
        return "sql-var";
      }
      else if (ch == "["){
	    setState(inAlias(ch))
	  	return null;
      }
      else if (ch == "\"" || ch == "'" || ch == "`") {
        setState(inLiteral(ch));
        return null;
      }
      else if (ch == "," || ch == ";") {
        return "sql-separator"
      }
      else if (ch == '#') {
        while (!source.endOfLine()) source.next();
        return "sql-comment";
      }
      else if (ch == '-') {
        if (source.peek() == "-") {
          while (!source.endOfLine()) source.next();
          return "sql-comment";
        }
        else if (/\d/.test(source.peek())) {
          source.nextWhileMatches(/\d/);
          if (source.peek() == '.') {
            source.next();
            source.nextWhileMatches(/\d/);
          }
          return "sql-number";
        }
        else
          return "sql-operator";
      }
      else if (operatorChars.test(ch)) {

        if(ch == "/" && source.peek() == "*"){
          setState(inBlock("sql-comment", "*/"));
          return null;
        }
        else{
          source.nextWhileMatches(operatorChars);
          return "sql-operator";
        }
        
      }
      else if (/\d/.test(ch)) {
        source.nextWhileMatches(/\d/);
        if (source.peek() == '.') {
          source.next();
          source.nextWhileMatches(/\d/);
        }
        return "sql-number";
      }
      else if (/[()]/.test(ch)) {
        return "sql-punctuation";
      }
      else {
        source.nextWhileMatches(/[_\w\d]/);
        var word = source.get(), type;
        if (operators.test(word))
          type = "sql-operator";
        else if (keywords.test(word))
          type = "sql-keyword";
        else if (functions.test(word))
          type = "sql-function";
        else if (types.test(word))
          type = "sql-type";
        else
          type = "sql-word";
        return {style: type, content: word};
      }
    }

    function inAlias(quote) {
	  return function(source, setState) {
	    while (!source.endOfLine()) {
		  var ch = source.next();
		  if (ch == ']') {
		    setState(normal);
		    break;
		  }
	    }
	    return "sql-word";
	  }
    }

    function inLiteral(quote) {
      return function(source, setState) {
        var escaped = false;
        while (!source.endOfLine()) {
          var ch = source.next();
          if (ch == quote && !escaped) {
            setState(normal);
            break;
          }
          escaped = CFG.extension == 'T-SQL' ?
                                  !escaped && quote == ch && source.equals(quote) :
                                  !escaped && ch == "\\";
        }        
        return quote == "`" ? "sql-quoted-word" : "sql-literal";
      };
    }

    function inBlock(style, terminator) {
      return function(source, setState) {
        while (!source.endOfLine()) {
          if (source.lookAhead(terminator, true)) {
            setState(normal);
            break;
          }
          source.next();
        }
        return style;
      };
    }

    return function(source, startState) {
      return tokenizer(source, startState || normal);
    };
  })();

  function indentSql(context) {
    return function(nextChars) {
      var firstChar = nextChars && nextChars.charAt(0);
      var closing = context && firstChar == context.type;
      if (!context)
        return 0;
      else if (context.align)
        return context.col - (closing ? context.width : 0);
      else
        return context.indent + (closing ? 0 : indentUnit);
    }
  }

  function parseSql(source) {
    var tokens = tokenizeSql(source);
    var context = null, indent = 0, col = 0;
    function pushContext(type, width, align) {
      context = {prev: context, indent: indent, col: col, type: type, width: width, align: align};
    }
    function popContext() {
      context = context.prev;
    }

    var iter = {
      next: function() {
        var token = tokens.next();
        var type = token.style, content = token.content, width = token.value.length;

        if (content == "\n") {
          token.indentation = indentSql(context);
          indent = col = 0;
          if (context && context.align == null) context.align = false;
        }
        else if (type == "whitespace" && col == 0) {
          indent = width;
        }
        else if (!context && type != "sql-comment") {
          pushContext(";", 0, false);
        }

        if (content != "\n") col += width;

        if (type == "sql-punctuation") {
          if (content == "(")
            pushContext(")", width);
          else if (content == ")")
            popContext();
        }
        else if (type == "sql-separator" && content == ";" && context && !context.prev) {
          popContext();
        }

        return token;
      },

      copy: function() {
        var _context = context, _indent = indent, _col = col, _tokenState = tokens.state;
        return function(source) {
          tokens = tokenizeSql(source, _tokenState);
          context = _context;
          indent = _indent;
          col = _col;
          return iter;
        };
      }
    };
    return iter;
  }

  function configure (parserConfig) {
    for (var p in parserConfig) {
      if (parserConfig.hasOwnProperty(p)) {
        CFG[p] = parserConfig[p];
      }
    }
  }

  return {make: parseSql, electricChars: ")", configure: configure};
})();
