
/**
 *
 */
(function (moment) {

    /**
     * The internal **Java** date formats cache.
     *
     * @property javaDateFormats
     * @type {Object}
     */
    var javaDateFormats = {},

    /**
     * The internal **moment.js** date formats cache.
     *
     * @property momentDateFormats
     * @type {Object}
     */
    momentDateFormats = {},

    /**
     * The format pattern mapping from Java format to momentjs.
     *
     * @property javaFormatMapping
     * @type {Object}
     */
    javaFormatMapping = {
      d: 'D',
      dd: 'DD',
      y: 'YYYY',
      yy: 'YY',
      yyy: 'YYYY',
      yyyy: 'YYYY',
      a: 'a',
      A: 'A',
      M: 'M',
      MM: 'MM',
      MMM: 'MMM',
      MMMM: 'MMMM',
      h: 'h',
      hh: 'hh',
      H: 'H',
      HH: 'HH',
      m: 'm',
      mm: 'mm',
      s: 's',
      ss: 'ss',
      S: 'SSS',
      SS: 'SSS',
      SSS: 'SSS',
      E: 'ddd',
      EE: 'ddd',
      EEE: 'ddd',
      EEEE: 'dddd',
      EEEEE: 'dddd',
      EEEEEE: 'dddd',
      D: 'DDD',
      w: 'W',
      ww: 'WW',
      z: 'ZZ',
      zzzz: 'Z',
      Z: 'ZZ',
      X: 'ZZ',
      XX: 'ZZ',
      XXX: 'Z',
      u: 'E'
    },

    /**
     * The format pattern mapping from Java format to momentjs.
     *
     * @property momentFormatMapping
     * @type {Object}
     */
    momentFormatMapping = {
      D: 'd',
      DD: 'dd',
      YY: 'yy',
      YYY: 'yyyy',
      YYYY: 'yyyy',
      a: 'a',
      A: 'A',
      M: 'M',
      MM: 'MM',
      MMM: 'MMM',
      MMMM: 'MMMM',
      h: 'h',
      hh: 'hh',
      H: 'H',
      HH: 'HH',
      m: 'm',
      mm: 'mm',
      s: 's',
      ss: 'ss',
      S: 'S',
      SS: 'S',
      SSS: 'S',
      ddd: 'E',
      dddd: 'EEEE',
      DDD: 'D',
      W: 'w',
      WW: 'ww',
      ZZ: 'z',
      Z: 'XXX',
      E: 'u'
    };

  function hookMoment (moment) {


    // register as private function (good for testing purposes)
    moment.fn.__translateJavaFormat = translateFormat;

    /**
     * Translates the momentjs format String to a java date format String.
     *
     * @function toJDFString
     * @param {String}  formatString    The format String to be translated.
     * @returns {String}
     */
    moment.fn.toMomentFormatString = function (formatString) {
      if (!javaDateFormats[formatString]) {
        javaDateFormats[formatString] = translateFormat(formatString, javaFormatMapping);
      }
      return javaDateFormats[formatString];
    };

    /**
     * Format the moment with the given java date format String.
     *
     * @function formatWithJDF
     * @param {String}  formatString    The format String to be translated.
     * @returns {String}
     */
    moment.fn.formatWithJDF = function (formatString) {
      return this.format(this.toMomentFormatString(formatString));
    };

    /**
     * Translates the momentjs format string to a java date format string
     *
     * @function toJDFString
     * @param {String}  formatString    The format String to be translated.
     * @returns {String}
     */
    moment.fn.toJDFString = function (formatString) {
      if (!momentDateFormats[formatString]) {
        momentDateFormats[formatString] = translateFormat(formatString, momentFormatMapping);
      }
      return momentDateFormats[formatString];
    };


    if (typeof module !== 'undefined' && module !== null) {
      module.exports = moment;
    } else {
      this.moment = moment;
    }
  }





  if(moment){
    this.moment = moment;
    hookMoment(this.moment);
  }


  /**
   * Translates the java date format String to a momentjs format String.
   *
   * @function translateFormat
   * @param {String}  formatString    The unmodified format string
   * @param {Object}  mapping         The date format mapping object
   * @returns {String}
   */
  var translateFormat = function (formatString, mapping) {
    var len = formatString.length,
      i = 0,
      beginIndex = -1,
      lastChar = null,
      currentChar = "",
      resultString = "";

    for (; i < len; i++) {
      currentChar = formatString.charAt(i);

      if (lastChar === null || lastChar !== currentChar) {
        // change detected
        resultString = _appendMappedString(formatString, mapping, beginIndex, i, resultString);

        beginIndex = i;
      }

      lastChar = currentChar;
    }

    return _appendMappedString(formatString, mapping, beginIndex, i, resultString);
  };

  /**
   * Checks if the substring is a mapped date format pattern and adds it to the result format String.
   *
   * @function _appendMappedString
   * @param {String}  formatString    The unmodified format String.
   * @param {Object}  mapping         The date format mapping Object.
   * @param {Number}  beginIndex      The begin index of the continuous format characters.
   * @param {Number}  currentIndex    The last index of the continuous format characters.
   * @param {String}  resultString    The result format String.
   * @returns {String}
   * @private
   */
  var _appendMappedString = function (formatString, mapping, beginIndex, currentIndex, resultString) {
    var tempString;

    if (beginIndex !== -1) {
      tempString = formatString.substring(beginIndex, currentIndex);
      // check if the temporary string has a known mapping
      if (mapping[tempString]) {
        tempString = mapping[tempString];
      }
      resultString = resultString.concat(tempString);
    }
    return resultString;
  };

}).call(this,moment);
