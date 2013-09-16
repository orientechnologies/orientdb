/**
 * Copyright 2012 OrientDB
 * 
 * DataTables plugin loader with changes to make it works with Twitter Bootstrap
 * 2 (http://datatables.net/blog/Twitter_Bootstrap_2)
 */
define([ 'datatables' ], function() {

  $.fn.dataTableExt.oApi.fnAddTr = function(oSettings, nTr, bRedraw) {
    if (typeof bRedraw == 'undefined') {
      bRedraw = true;
    }

    var nTds = nTr.getElementsByTagName('td');
    if (nTds.length != oSettings.aoColumns.length) {
      alert('Warning: not adding new TR - columns and TD elements must match');
      return;
    }

    var aData = [];
    for ( var i = 0; i < nTds.length; i++) {
      aData.push(nTds[i].innerHTML);
    }

    /* Add the data and then replace DataTable's generated TR with ours */
    var iIndex = this.oApi._fnAddData(oSettings, aData);
    nTr._DT_RowIndex = iIndex;
    oSettings.aoData[iIndex].nTr = nTr;

    oSettings.aiDisplay = oSettings.aiDisplayMaster.slice();

    if (bRedraw) {
      this.oApi._fnReDraw(oSettings);
    }
  };

  // custom asc sort based on data attr
  $.fn.dataTableExt.oSort['data-attr-asc'] = function(a, b) {
    var ae = $(a);
    var be = $(b);
    var av, bv;
    if (ae.data('sort-value') && be.data('sort-value')) {
      av = parseFloat(ae.data('sort-value'));
      bv = parseFloat(be.data('sort-value'));
    }
    return ((av < bv) ? -1 : ((av > bv) ? 1 : 0));
  };

  // custom desc sort based on data attr
  $.fn.dataTableExt.oSort['data-attr-desc'] = function(a, b) {
    var ae = $(a);
    var be = $(b);
    var av, bv;
    if (ae.data('sort-value') && be.data('sort-value')) {
      av = parseFloat(ae.data('sort-value'));
      bv = parseFloat(be.data('sort-value'));
    }
    return ((av < bv) ? 1 : ((av > bv) ? -1 : 0));
  };

  // disable warning alert
  $.fn.dataTableExt.sErrMode = 'throw';

  /* Default class modification */
  $.extend($.fn.dataTableExt.oStdClasses, {
    "sWrapper" : "dataTables_wrapper form-inline"
  });

  /* API method to get paging information */
  $.fn.dataTableExt.oApi.fnPagingInfo = function(oSettings) {
    return {
      "iStart" : oSettings._iDisplayStart,
      "iEnd" : oSettings.fnDisplayEnd(),
      "iLength" : oSettings._iDisplayLength,
      "iTotal" : oSettings.fnRecordsTotal(),
      "iFilteredTotal" : oSettings.fnRecordsDisplay(),
      "iPage" : Math.ceil(oSettings._iDisplayStart / oSettings._iDisplayLength),
      "iTotalPages" : Math.ceil(oSettings.fnRecordsDisplay() / oSettings._iDisplayLength)
    };
  };

  /*
   * COLUMN FILTER method Function: fnGetColumnData Purpose: Return an array of
   * table values from a particular column. Returns: array string: 1d data array
   * Inputs: object:oSettings - dataTable settings object. This is always the
   * last argument past to the function int:iColumn - the id of the column to
   * extract the data from bool:bUnique - optional - if set to false duplicated
   * values are not filtered out bool:bFiltered - optional - if set to false all
   * the table data is used (not only the filtered) bool:bIgnoreEmpty - optional -
   * if set to false empty values are not filtered from the result array Author:
   * Benedikt Forchhammer <b.forchhammer /AT\ mind2.de>
   */
  $.fn.dataTableExt.oApi.fnGetColumnData = function(oSettings, iColumn, bUnique, bFiltered, bIgnoreEmpty) {
    // check that we have a column id
    if (typeof iColumn == "undefined")
      return [];

    // by default we only wany unique data
    if (typeof bUnique == "undefined")
      bUnique = true;

    // by default we do want to only look at filtered data
    if (typeof bFiltered == "undefined")
      bFiltered = true;

    // by default we do not wany to include empty values
    if (typeof bIgnoreEmpty == "undefined")
      bIgnoreEmpty = true;

    // list of rows which we're going to loop through
    var aiRows;

    // use only filtered rows
    if (bFiltered === true)
      aiRows = oSettings.aiDisplay;
    // use all rows
    else
      aiRows = oSettings.aiDisplayMaster; // all row numbers

    // set up data array
    var asResultData = [];

    for ( var i = 0, c = aiRows.length; i < c; i++) {
      iRow = aiRows[i];
      var aData = this.fnGetData(iRow);
      aData = Object.keys(aData).map(function(key) {
        return aData[key];
      });
      var sValue = aData[iColumn];

      // ignore empty values?
      if (bIgnoreEmpty === true && sValue.length === 0)
        continue;

      // ignore unique values?
      else if (bUnique === true && jQuery.inArray(sValue, asResultData) > -1)
        continue;

      // else push the value onto the result data array
      else
        asResultData.push(sValue);
    }

    return asResultData;
  };

  /* Bootstrap style pagination control */
  $.extend($.fn.dataTableExt.oPagination, {
    "bootstrap" : {
      "fnInit" : function(oSettings, nPaging, fnDraw) {
        var oLang = oSettings.oLanguage.oPaginate;
        var fnClickHandler = function(e) {
          e.preventDefault();
          if (oSettings.oApi._fnPageChange(oSettings, e.data.action)) {
            fnDraw(oSettings);
          }
        };

        $(nPaging).addClass('pagination').append(
            '<ul>' + '<li class="prev disabled"><a href="#">&larr; ' + oLang.sPrevious + '</a></li>' +
                '<li class="next disabled"><a href="#">' + oLang.sNext + ' &rarr; </a></li>' + '</ul>');
        var els = $('a', nPaging);
        $(els[0]).bind('click.DT', {
          action : "previous"
        }, fnClickHandler);
        $(els[1]).bind('click.DT', {
          action : "next"
        }, fnClickHandler);
      },
      "fnUpdate" : function(oSettings, fnDraw) {
        var iListLength = 5;
        var oPaging = oSettings.oInstance.fnPagingInfo();
        var an = oSettings.aanFeatures.p;
        var i, j, sClass, iStart, iEnd, iHalf = Math.floor(iListLength / 2);

        if (oPaging.iTotalPages < iListLength) {
          iStart = 1;
          iEnd = oPaging.iTotalPages;
        } else if (oPaging.iPage <= iHalf) {
          iStart = 1;
          iEnd = iListLength;
        } else if (oPaging.iPage >= (oPaging.iTotalPages - iHalf)) {
          iStart = oPaging.iTotalPages - iListLength + 1;
          iEnd = oPaging.iTotalPages;
        } else {
          iStart = oPaging.iPage - iHalf + 1;
          iEnd = iStart + iListLength - 1;
        }

        for (i = 0, iLen = an.length; i < iLen; i++) {
          // Remove the middle elements
          $('li:gt(0)', an[i]).filter(':not(:last)').remove();

          // Add the new list items and their event handlers
          for (j = iStart; j <= iEnd; j++) {
            sClass = (j == oPaging.iPage + 1) ? 'class="active"' : '';
            $('<li ' + sClass + '><a href="#">' + j + '</a></li>').insertBefore($('li:last', an[i])[0]).bind('click', function(e) {
              e.preventDefault();
              oSettings._iDisplayStart = (parseInt($('a', this).text(), 10) - 1) * oPaging.iLength;
              fnDraw(oSettings);
            });
          }

          // Add / remove disabled classes from the static elements
          if (oPaging.iPage === 0) {
            $('li:first', an[i]).addClass('disabled');
          } else {
            $('li:first', an[i]).removeClass('disabled');
          }

          if (oPaging.iPage === oPaging.iTotalPages - 1 || oPaging.iTotalPages === 0) {
            $('li:last', an[i]).addClass('disabled');
          } else {
            $('li:last', an[i]).removeClass('disabled');
          }
        }
      }
    }
  });

  var dOptions = {
    "sDom" : "<'span8'l><'span4'f>rt<'span8'i><'span3'p>",
    "bPaginate" : true,
    "bRetrieve" : true,
    "sPaginationType" : "bootstrap",
    "oLanguage" : {
      "sLengthMenu" : "_MENU_ records per page"
    },
    "iDisplayLength" : 50,
    "aLengthMenu" : [ [ 10, 25, 50, 100, 200, -1 ], [ 10, 25, 50, 100, 200, "All" ] ]
  };

  var fnCreateSelect = function(aData) {
    var ot = [];
    var r = '<select size=1 class="span2"><option value=""></option>', i, iLen = aData.length;
    for (i = 0; i < iLen; i++) {
      var t = $(aData[i]).text() ? $(aData[i]).text() : aData[i];
      if (!_.include(ot, t)) {
        r += "<option value='" + t + "'>" + t + "</option>";
        ot.push(t);
      }
    }
    return r + '</select>';
  };

  var show = function(selector, options) {
    var tables = [];

    var o = options ? _.defaults(options, dOptions) : dOptions;

    var iFnCreateSelect = o.fnCreateSelect || fnCreateSelect;

    for ( var i = 0; i < selector.length; i++) {
      var dt = $(selector[i]).dataTable(o);
      tables.push(dt);

      // var iFnGetColumnData = o.fnGetColumnData || dt.fnGetColumnData;

      if (o.columnFilters) {
        /* Add a select menu for each TH element in the table footer */
        $("tfoot th.filter", selector[i]).each(function(y) {
          var columnData;
          if (o.fnGetColumnData) {
            columnData = o.fnGetColumnData(y);
          } else {
            columnData = dt.fnGetColumnData(y);
          }

          this.innerHTML = iFnCreateSelect(columnData);
          $('select', this).change(function() {
            var k = $(this).val() ? "^" + $(this).val() + "$" : "";
            dt.fnFilter(k, y, true);
          });
        });
      }
    }

    return tables;
  };

  return {
    show : show
  };
});