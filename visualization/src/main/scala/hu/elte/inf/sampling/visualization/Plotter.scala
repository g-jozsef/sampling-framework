package hu.elte.inf.sampling.visualization

import java.nio.file.{Files, Paths}

import hu.elte.inf.sampling.core.{Logger, Shop}
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.VectorGraphicsEncoder.VectorGraphicsFormat
import org.knowm.xchart._
import org.knowm.xchart.style.BoxStyler.BoxplotCalCulationMethod
import org.knowm.xchart.style.Styler.{ChartTheme, LegendLayout, LegendPosition}

object Plotter extends Logger {
  val cutoff = 40

  def box(describe: String, yLabel: String, resWidth: Int, resHeight: Int, saveOutputPath: String, backgroundRunning: Boolean, logOnly: Boolean, dataSource: (Iterator[Double], String)*) = {
    logDebug(s"Plotting $describe... " + saveOutputPath)

    // Create Chart
    val chart = new BoxChartBuilder().title(if (describe.length > cutoff - 1) describe.substring(0, cutoff) else describe).width(resWidth).height(resHeight).theme(ChartTheme.Matlab).yAxisTitle(yLabel).build();
    chart.getStyler.setBoxplotCalCulationMethod(BoxplotCalCulationMethod.N_LESS_1_PLUS_1)
    chart.getStyler.setToolTipsEnabled(true)
    chart.getStyler.setXAxisLabelRotation(30)

    // Series// Series
    dataSource.foreach(
      data => {
        val a: Array[Double] = data._1.toArray
        logInfo(s"${saveOutputPath.replace(".", "_")}: ${data._2}: ${a.mkString(", ")}")
        chart.addSeries(data._2, a)
      }
    )

    if (saveOutputPath != "") {
      Files.createDirectories(Paths.get(saveOutputPath.replace(".", "_")).getParent)
      BitmapEncoder.saveBitmap(chart, saveOutputPath.replace(".", "_") + "_box.png", BitmapFormat.PNG);
    }

    if (!backgroundRunning) {
      new SwingWrapper[BoxChart](chart).displayChart
    }

  }

  def scatter(descibe: String, resWidth: Int, resHeight: Int, saveOutputPath: String, backgroundRunning: Boolean, nElements: Int, dataSource: Array[Shop.Type]) = {
    logDebug("Plotting generated stream... " + saveOutputPath)

    val data = new Array[Array[Double]](2)
    for (i: Int <- (0 to 1)) {
      data(i) = new Array[Double](dataSource.length)
    }
    for (i: Int <- dataSource.indices) {
      data(0)(i) = i
      data(1)(i) = dataSource(i).toDouble
    }
    val chart = new XYChartBuilder().width(resWidth).height(resHeight).theme(ChartTheme.Matlab).build();
    import org.knowm.xchart.XYSeries.XYSeriesRenderStyle
    import org.knowm.xchart.style.Styler.LegendPosition

    chart.getStyler.setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Scatter)
    chart.getStyler.setChartTitleVisible(false)
    chart.getStyler.setLegendPosition(LegendPosition.InsideSW)
    chart.getStyler.setMarkerSize(1)
    chart.getStyler.setLegendVisible(false);
    chart.addSeries("Stream", data(0), data(1))

    if (saveOutputPath != "") {
      Files.createDirectories(Paths.get(saveOutputPath.replace(".", "_")).getParent)
      BitmapEncoder.saveBitmap(chart, saveOutputPath.replace(".", "_") + "_scatter.png", BitmapFormat.PNG);
    }

    if (!backgroundRunning) {
      new SwingWrapper[XYChart](chart).displayChart
    }
  }

  def line(descibe: String, resWidth: Int, resHeight: Int, yRange: Option[(Double, Double)], yLabel: String, saveOutputPath: String, backgroundRunning: Boolean, source: (Iterator[Double], String)*) = {
    logDebug("Plotting errors... " + saveOutputPath)

    import org.knowm.xchart.XYChart

    // Create Chart
    val chart = new XYChartBuilder().width(resWidth).height(resHeight).theme(ChartTheme.Matlab).title(descibe).yAxisTitle(yLabel).xAxisTitle("").build

    chart.getStyler.setMarkerSize(5)
    yRange match {
      case Some(value) =>
        chart.getStyler.setYAxisMin(value._1)
        chart.getStyler.setYAxisMax(value._2)
      case None =>
        ()
    }

    chart.getStyler.setLegendPosition(LegendPosition.OutsideS)
    chart.getStyler.setLegendLayout(LegendLayout.Horizontal)

    source.foreach(
      data => {
        val a: Array[Double] = data._1.toArray
        chart.addSeries(data._2, a)
      }
    )

    if (saveOutputPath != "") {
      Files.createDirectories(Paths.get(saveOutputPath.replace(".", "_")).getParent)
      // BitmapEncoder.saveBitmap(chart, saveOutputPath.replace(".", "_") + ".png", BitmapFormat.PNG);
      VectorGraphicsEncoder.saveVectorGraphic(chart, saveOutputPath + "_line.svg", VectorGraphicsFormat.SVG);
    }

    if (!backgroundRunning) {
      new SwingWrapper[XYChart](chart).displayChart
    }
  }
}