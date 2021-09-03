package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.knowm.xchart.*;
import org.knowm.xchart.internal.chartpart.Chart;
import org.knowm.xchart.style.Styler;

public class Plotter {
  public CategoryChart getCategoryChart(
      final String chartName,
      final String xAxisTitle,
      final String yAxisTitle,
      final Styler.LegendPosition position) {
    final CategoryChart chart =
        new CategoryChartBuilder()
            .width(500)
            .height(500)
            .title(chartName)
            .xAxisTitle(xAxisTitle)
            .theme(Styler.ChartTheme.Matlab)
            .yAxisTitle(yAxisTitle)
            .build();
    chart.getStyler().setLegendPosition(position);
    chart.getStyler().setAvailableSpaceFill(.96);
    return chart;
  }

  public XYChart getXYChart(
      final String chartName,
      final String xAxisTitle,
      final String yAxisTitle,
      final Styler.LegendPosition position) {
    final XYChart chart =
        new XYChartBuilder()
            .width(500)
            .height(500) // .title(chartName)
            .theme(Styler.ChartTheme.Matlab)
            .xAxisTitle(xAxisTitle)
            .yAxisTitle(yAxisTitle)
            .build();
    chart.getStyler().setChartTitleVisible(true);
    chart.getStyler().setLegendPosition(position);
    chart.getStyler().setYAxisLogarithmic(false);
    chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Line);
    chart.getStyler().setXAxisLabelRotation(45);
    return chart;
  }

  public Histogram addSeriesToHistogram(final CategoryChart chart) {
    Histogram histogram = new Histogram(getGaussianData(10000), 10, -10, 10);
    chart.addSeries(
        "histogram",
        histogram.getxAxisData(),
        histogram.getyAxisData(),
        getFakeErrorData(histogram.getxAxisData().size()));
    return histogram;
  }

  public Histogram addSeriesToHistogram(final CategoryChart chart, final List<Double> data) {
    Histogram histogram = new Histogram(data, 10, -10, 10);
    chart.addSeries("histogram", histogram.getxAxisData(), histogram.getyAxisData());
    return histogram;
  }

  public Histogram addSeriesToHistogram(
      final CategoryChart chart, final List<Double> data, final List<Double> errorData) {
    Histogram histogram = new Histogram(data, 10, -10, 10);
    chart.addSeries("histogram", histogram.getxAxisData(), histogram.getyAxisData(), errorData);
    return histogram;
  }

  public XYChart addSeriesToLineChart(
      final XYChart chart, final String seriesName, final List xData, final List yData) {
    chart.addSeries(seriesName, xData, yData);
    return chart;
  }

  public void addSeriesToLineChart(final XYChart chart, final List<LineResultData> seriesData) {
    for (final LineResultData lrd : seriesData) {
      this.addSeriesToLineChart(chart, lrd.getSeriesName(), lrd.getxData(), lrd.getyData());
    }
  }

  private List<Double> getGaussianData(int count) {
    List<Double> data = new ArrayList<Double>(count);
    Random r = new Random();
    for (int i = 0; i < count; i++) {
      data.add(r.nextGaussian() * 5);
    }
    return data;
  }

  private List<Double> getFakeErrorData(int count) {
    final List<Double> data = new ArrayList<Double>(count);
    Random r = new Random();
    for (int i = 0; i < count; i++) {
      data.add(r.nextDouble() * 200);
    }
    return data;
  }

  public void exportChartAsPDF(final Chart chart, final String fileName) throws IOException {
    VectorGraphicsEncoder.saveVectorGraphic(
        chart, fileName, VectorGraphicsEncoder.VectorGraphicsFormat.PDF);
  }

  public void exportChartAsPNG(final Chart chart, final String fileName) throws IOException {
    BitmapEncoder.saveBitmap(chart, fileName, BitmapEncoder.BitmapFormat.PNG);
  }
}
