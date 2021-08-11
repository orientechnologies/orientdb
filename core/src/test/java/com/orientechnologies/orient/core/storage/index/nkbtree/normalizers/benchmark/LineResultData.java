package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.benchmark;

import java.util.List;

public class LineResultData {
  private String seriesName;
  private List<Double> xData;
  private List<Double> yData;

  public LineResultData(final String seriesName) {
    this.seriesName = seriesName;
  }

  public void addXData(final List<Double> xData) {
    this.xData = xData;
  }

  public void addYData(final List<Double> yData) {
    this.yData = yData;
  }

  public String getSeriesName() {
    return this.seriesName;
  }

  public List<Double> getxData() {
    return xData;
  }

  public List<Double> getyData() {
    return yData;
  }
}
