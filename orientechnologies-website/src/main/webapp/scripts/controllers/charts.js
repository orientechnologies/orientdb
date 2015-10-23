


angular.module('webappApp')
  .controller('ChartsCtrl', function ($scope, $location, Organization, $q) {

    $scope.showYear=true;
    $scope.showMonth=true;
    $scope.showClientOnly=true;



    $scope.currentChart;

    $scope.years = [];
    for(var i = 2012; i <= new Date().getFullYear(); i++){
      $scope.years.push(i);
    }

    $scope.months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    $scope.date = {year:new Date().getFullYear(), month:new Date().getMonth()+1};
    $scope.clientOnly = false;
    $scope.chartTitle = "";


    $scope.chart = function(chartFunction) {
      $scope.showYear=false;
      $scope.showMonth=false;
      $scope.showClientOnly=false;
      $scope.currentChart = chartFunction;
      chartFunction();
    }


    //-----------------------------------------------------------
    //------------------- Issues trend --------------------------
    //-----------------------------------------------------------

    $scope.issues  = function() {

      $scope.showClientOnly=true;

      $scope.chartTitle = 'Total Open/closed issues';

      var chartData = {
        data: {
          x: "x",
          columns: [["x"], ["still open"], ["new"], ["closed"]]
        },
        axis: {
          x: {
            type: 'timeseries',
            tick: {
              format: '%Y-%m-%d'
            }
          }
        }
      };

      var chart = c3.generate(chartData);


      Organization.all("reports/openIssuesPerInterval/monthly/"+$scope.clientOnly).getList().then(function (data) {
        var chartData  = {columns:[["x"], ["still open"], ["new"], ["closed"]]};
        var total = 0;
        data.forEach(function (item, idx, ar) {
          var monthly = parseInt(item.value, 10);
          total += monthly;
          chartData.columns[0][idx + 1] = item['label'] + "-01";
          chartData.columns[2][idx + 1] = monthly;

          console.log("open: "+total+" new: "+monthly);
        });

        Organization.all("reports/closedIssuesPerInterval/monthly/"+$scope.clientOnly).getList().then(function (data) {

          data.forEach(function (item, idx2, ar) {
            var value = parseInt(item.value, 10);
            console.log("closed: "+value+" ("+chartData.columns[1][idx2 + 1]+" - "+value+")");
            if(idx2>1){
              chartData.columns[1][idx2 + 1] = chartData.columns[1][idx2] + chartData.columns[2][idx2 + 1] - value;
            }else{
              chartData.columns[1][idx2 + 1] = chartData.columns[2][idx2 + 1] - value;
            }
            console.log("still open: "+chartData.columns[1][idx2 + 1]);
            chartData.columns[3][idx2 + 1] = value;

          });


          chart.load(chartData);
        });

      });

    }


    //-----------------------------------------------------------
    //-------------- open/closed per dev per month --------------
    //-----------------------------------------------------------

    $scope.openClosedPerDevPerMonth  = function() {

      $scope.showYear=true;
      $scope.showMonth=true;
      $scope.showClientOnly=true;

      var chartData = {
        data: {
          x: "x",
          columns: [ ['x'], ["open in month"], ["closed in month"], ["total still open"]],
          type: "bar"
        },
        bar: {
          width: {
            ratio: 0.5
          }
        },
        axis: {
          x: {
            type: 'category'
          }
        }
      };

      chartData.data.onclick = function(d, element){
        $location.path("/issues").search({q: "is:open assignee:\""+chartData.data.columns[0][d.x+1]+"\""});
        $scope.$apply();
      }

      var committers = [];

      var today = new Date();
      var month = $scope.date.month ? $scope.date.month - 1 : today.getMonth();
      var year = $scope.date.year ? $scope.date.year:today.getFullYear();

      $scope.chartTitle = 'Open/closed issues per developer per month ('+(month+1)+"/"+year+")";


      var members = [];

      Organization.all("members").getList().then(function (data) {


        data.forEach(function (member, idx, ar) {
          var memberName = member.name;
          if(memberName== 'prjhub'){
            return;
          }

          if(members.indexOf(memberName) == -1){
            members.push(memberName);
          }
        });


        members.forEach(function (item, idx, ar) {

          chartData.data.columns[0][idx + 1] = item;
          chartData.data.columns[1][idx + 1] = 0;
        });


        var chart = c3.generate(chartData);

        Organization.all("reports/openIssuesPerDeveloper/"+$scope.clientOnly).getList().then(function (data) {
          var chartData  = {columns:[["total still open"]]};
          members.forEach(function (item, idx, ar) {
            chartData.columns[0][idx+1] = 0;
          });
          data.forEach(function (item, idx, ar) {
            var index = members.indexOf(item.label);
            if(index != -1) {
              chartData.columns[0][index+1] = parseInt(item.value, 10);
            }
          });

          chart.load(chartData);
        });


        Organization.all("reports/openIssuesPerDeveloper/"+year+"/"+month+"/"+$scope.clientOnly).getList().then(function (data) {
          var chartData  = {columns:[["open in month"]]};
          members.forEach(function (item, idx, ar) {
            chartData.columns[0][idx+1] = 0;
          });
          data.forEach(function (item, idx, ar) {
            var index = members.indexOf(item.label);
            if(index != -1) {
              chartData.columns[0][index+1] = parseInt(item.value, 10);
            }
          });

          chart.load(chartData);
        });

        Organization.all("reports/closedIssuesPerDeveloper/"+year+"/"+month+"/"+$scope.clientOnly).getList().then(function (data) {
          var chartData  = {columns:[["closed in month"]]};
          members.forEach(function (item, idx, ar) {
            chartData.columns[0][idx+1] = 0;
          });
          data.forEach(function (item, idx, ar) {
            var index = members.indexOf(item.label);
            if(index != -1) {
              chartData.columns[0][index+1] = parseInt(item.value, 10);
            }
          });

          chart.load(chartData);
        });

      });


    }



    //-----------------------------------------------------------
    //--------------   issues per developer ---------------------
    //-----------------------------------------------------------

    $scope.issuesPerDev  = function() {

      $scope.showClientOnly=true;

      $scope.chartTitle = 'Total Issues per Developer';

      var chartData = {
        data: {
          columns: [],
          type:"pie"
        }
      };

      chartData.data.onclick = function(d, element){
        $location.path("/issues").search({q: "is:open assignee:\""+ d.name +"\""});
        $scope.$apply();
      }

      Organization.all("reports/issuesPerDeveloper/"+$scope.clientOnly).getList().then(function (data) {
        data.forEach(function (item, idx, ar) {
          if(item.label!="none") {
            chartData.data.columns.push([item.label, item.value]);
          }
        });


        c3.generate(chartData);
      });
    }


    //-----------------------------------------------------------
    //--------------   closed issues per developer --------------
    //-----------------------------------------------------------

    $scope.closedIssuesPerDev  = function() {

      $scope.showClientOnly=true;

      $scope.chartTitle = 'Total Closed Issues per Developer';

      var chartData = {
        data: {
          columns: [],
          type:"pie"
        }
      };

      chartData.data.onclick = function(d, element){
        $location.path("/issues").search({q: "is:open assignee:\""+ d.name +"\""});
        $scope.$apply();
      }



      Organization.all("reports/closedIssuesPerDeveloper/"+$scope.clientOnly).getList().then(function (data) {
        data.forEach(function (item, idx, ar) {
          if(item.label!="none") {
            chartData.data.columns.push([item.label, item.value]);
          }
        });

        c3.generate(chartData);
      });
    }

    //-----------------------------------------------------------
    //-------------- open/closed per client ---------------------
    //-----------------------------------------------------------
    $scope.openClosedPerClient  = function() {
      var chartData = {
        data: {
          x: "x",
          columns: [ ['x'], ["total"], ["open"], ["closed"]],
          type: "bar"
        },
        bar: {
          width: {
            ratio: 0.7
          }
        },
        axis: {
          x: {
            type: 'category'
          },
          rotated: true
        },
        size: {
          height: 1000
        }
      };
      chartData.data.onclick = function(d, element){
        $location.path("/issues").search({q: "is:open client:\""+chartData.data.columns[0][d.x+1]+"\""});
        $scope.$apply();
      }


      var committers = [];

      $scope.chartTitle = 'Open/closed issues per Client';

      var members = [];

      Organization.all("clients").getList().then(function (data) {
        data.forEach(function (member, idx, ar) {
          var memberName = member.name;
          if(memberName== 'prjhub'){
            return;
          }

          if(members.indexOf(memberName) == -1){
            members.push(memberName);
          }
        });

        chartData.size.height = members.length * 40;

        members.forEach(function (item, idx, ar) {

          chartData.data.columns[0][idx + 1] = item;
          chartData.data.columns[1][idx + 1] = 0;
        });

        var chart = c3.generate(chartData);

        Organization.all("reports/issuesPerClient").getList().then(function (data) {
          var chartData  = {columns:[["total"]]};
          members.forEach(function (item, idx, ar) {
            chartData.columns[0][idx+1] = 0;
          });
          data.forEach(function (item, idx, ar) {
            var index = members.indexOf(item.label);
            if(index != -1) {
              chartData.columns[0][index+1] = parseInt(item.value, 10);
            }
          });

          chart.load(chartData);

          Organization.all("reports/openIssuesPerClient").getList().then(function (data) {
            var chartData  = {columns:[["open"]]};
            members.forEach(function (item, idx, ar) {
              chartData.columns[0][idx+1] = 0;
            });
            data.forEach(function (item, idx, ar) {
              var index = members.indexOf(item.label);
              if(index != -1) {
                chartData.columns[0][index+1] = parseInt(item.value, 10);
              }
            });

            chart.load(chartData);

            Organization.all("reports/closedIssuesPerClient").getList().then(function (data) {
              var chartData  = {columns:[["closed"]]};
              members.forEach(function (item, idx, ar) {
                chartData.columns[0][idx+1] = 0;
              });
              data.forEach(function (item, idx, ar) {
                var index = members.indexOf(item.label);
                if(index != -1) {
                  chartData.columns[0][index+1] = parseInt(item.value, 10);
                }
              });

              chart.load(chartData);
            });
          });
        });
      });


    }

    $scope.chart($scope.issues);
  });
