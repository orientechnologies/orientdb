import { downgradeInjectable } from "@angular/upgrade/static";
import { Http } from "@angular/http";
import "rxjs/add/operator/toPromise";

import { Injectable } from "@angular/core";
declare var angular: any;
import { Headers } from "@angular/http";
import { API } from "../../../constants";

@Injectable()
class MetricService {
  constructor(private http: Http) {}

  getMetrics(): any {
    let url = API + "metrics";
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  list() {
    let url = API + "metrics/list";
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }
  getConfig() {
    let url = API + "metrics/config";
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }
  saveConfig(config) {
    let url = API + "metrics/config";
    return this.http
      .post(url, config, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  getOptions() {
    let headers = new Headers({
      Authorization: localStorage.getItem("SimpleAuth"),
      "X-Requested-With": "XMLHttpRequest"
    });
    return {
      headers: headers
    };
  }

  getInfo(agent) {
    let url = API + (agent ? "/node/info" : "server");
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  listDatabases() {
    let url = API + "listDatabases";
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }
  threadDumps() {
    let url = API + `/node/threadDump`;
    return this.http
      .get(url, this.getOptions())
      .toPromise()
      .then(data => {
        return data.json();
      });
  }

  calculateGauges(stats) {
    // CPU

    let cpuValue = parseFloat(stats["gauges"]["server.runtime.cpu"].value);
    let cpuPercent = (100 * cpuValue).toFixed(2);

    // DISK CACHE
    let maxDiskCache = stats["gauges"]["server.runtime.diskCache.total"].value;

    let totalDiskCache = stats["gauges"]["server.runtime.diskCache.used"].value;

    let diskCachePercent = Math.floor((totalDiskCache * 100) / maxDiskCache);

    // RAM

    let maxMemory = stats["gauges"]["server.runtime.memory.heap.max"].value;

    let usedMemoy = stats["gauges"]["server.runtime.memory.heap.used"].value;

    let ramUsage = stats["gauges"]["server.runtime.memory.heap.usage"].value;
    let ramPercent = (100 * ramUsage).toFixed(2);

    // DISK

    let totalDisk = stats["gauges"]["server.disk.space.totalSpace"].value;
    let usableDisk = stats["gauges"]["server.disk.space.usableSpace"].value;
    let diskPercent = Math.floor(100 - (usableDisk * 100) / totalDisk);

    return {
      cpuValue,
      cpuPercent,
      maxDiskCache,
      totalDiskCache,
      diskCachePercent,
      maxMemory,
      usedMemoy,
      ramPercent,
      totalDisk,
      usableDisk,
      diskPercent
    };
  }
}

angular
  .module("metric.services", [])
  .factory(`MetricService`, downgradeInjectable(MetricService));

export { MetricService };
