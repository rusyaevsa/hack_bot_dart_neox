package rht.hack.components

import rht.common.domain.candles.{Candle, Figis}

object Metrics {
  def avg(candles: Seq[Candle]): BigDecimal = {
    candles.map(_.details.close).sum / candles.length
  }

  var minValue =
    Map(Figis.ADA -> Double.MaxValue,
      Figis.BNB -> Double.MaxValue,
      Figis.BTC -> Double.MaxValue,
      Figis.DOT -> Double.MaxValue,
      Figis.DOGE -> Double.MaxValue,
      Figis.ETH -> Double.MaxValue)

  var maxValue =
    Map(Figis.ADA -> Double.MinValue,
      Figis.BNB -> Double.MinValue,
      Figis.BTC -> Double.MinValue,
      Figis.DOT -> Double.MinValue,
      Figis.DOGE -> Double.MinValue,
      Figis.ETH -> Double.MinValue)

  var avgValue =
    Map(Figis.ADA -> Double.MinValue,
      Figis.BNB -> Double.MinValue,
      Figis.BTC -> Double.MinValue,
      Figis.DOT -> Double.MinValue,
      Figis.DOGE -> Double.MinValue,
      Figis.ETH -> Double.MinValue)


  def findMin(candle: Candle): Double = {
    if (minValue(candle.figi) > candle.details.close)
      minValue = minValue.updated(candle.figi, candle.details.close.doubleValue())
    minValue(candle.figi)
  }

  def findMax(candle: Candle): Double = {
    if (maxValue(candle.figi) < candle.details.close)
      maxValue = maxValue.updated(candle.figi, candle.details.close.doubleValue())
    maxValue(candle.figi)
  }
}
