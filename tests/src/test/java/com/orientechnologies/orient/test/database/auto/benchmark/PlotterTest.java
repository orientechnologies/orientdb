package com.orientechnologies.orient.test.database.auto.benchmark;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.style.Styler;

public class PlotterTest {
  private Plotter plotter;

  @Before
  public void setup() {
    plotter = new Plotter();
  }

  @Test
  public void histogram() throws Exception {
    final CategoryChart chart =
        plotter.getCategoryChart(
            "Test chart name", "Test x axis", "Test y axis", Styler.LegendPosition.InsideNW);
    plotter.addSeriesToHistogram(chart);
    plotter.exportChartAsPDF(chart, "target/histogram");
    Assert.assertTrue(new File("target/histogram.pdf").exists());
  }

  @Test
  public void lineChart() throws Exception {
    final XYChart chart =
        plotter.getXYChart(
            "Test chart name", "Test x axis", "Test y axis", Styler.LegendPosition.InsideNW);
    final List<Integer> xData = new ArrayList<>();
    final List<Double> yData = new ArrayList<>();
    for (int i = -3; i <= 3; i++) {
      xData.add(i);
      yData.add(Math.pow(10, i));
    }
    plotter.addSeriesToLineChart(chart, "10^x", xData, yData);
    plotter.exportChartAsPDF(chart, "target/lineChart");
    Assert.assertTrue(new File("target/lineChart.pdf").exists());
  }
}
