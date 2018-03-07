import java.io.{BufferedWriter, File, FileWriter}
import java.text.DecimalFormat

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Tool to perform back-testing on a time series using different investment strategies
  *
  * Created by mleone on 3/7/2018.
  */
object Backtesting {
  val SMALL_DIGIT_FORMAT = new DecimalFormat("0.##")

  def main(args: Array[String]): Unit = {
    val basePath = "src/main/resources/data/"
    val inputPath = basePath + "input/"
    val outputPath = basePath + "output/"

    //Load time series
    val timeSeries = TimeSeries.fromFile(inputPath + "TSLA.csv")

    //Define budget and trading fees
    val budget = 10000.0
    val fee = 6.95

    //Try investment strategy with fixed thresholds to buy/sell
    val minBase = 290 to 380
    val increments = 1 to 100
    val fixedStrategies = minBase.flatMap(min => increments.map(inc => new FixedStrategy(min, min + inc)))
    BacktestingRun.runAllAndSave(budget, fee, timeSeries, fixedStrategies, outputPath + "TSLA_Fixed.csv")

    //Try investment strategy using different SMA days with lower/upper bands defined by some SDEV
    val smaDays = Seq(10, 15, 20, 25, 30, 50, 65, 100)
    val sdevValues = (0.05 to 3.0).by(0.05)
    val smaSdevStrategies = smaDays.flatMap(d => sdevValues.map(s => new SmaSdevStrategy(d, s)))
    BacktestingRun.runAllAndSave(budget, fee, timeSeries, smaSdevStrategies, outputPath + "TSLA_SmaSdev.csv")

    //Try investment strategy using different SMA days with lower/upper bands defined by some percentage
    val percentValues = (0.1 to 10.0).by(0.1)
    val smaPercentStrategies = smaDays.flatMap(d => percentValues.map(p => new SmaPercentStrategy(d, p)))
    BacktestingRun.runAllAndSave(budget, fee, timeSeries, smaPercentStrategies, outputPath + "TSLA_Percent.csv")

  }

}

case class TimeSeries(quotes: Array[Quote]) {
  def computeMovingAverage(days: Int): SimpleMovingAverage = {
    if(quotes.length < days)
      throw new RuntimeException(s"Not enough data for days: $days")

    //Compute total for first days
    var i = 0
    var total: Double = 0
    while(i < days) {
      total += quotes(i).adjClose
      i+=1
    }

    //Compute avg and sdev for first days
    var avg = total / days
    var totalDiff: Double = 0
    i = 0
    while(i < days) {
      totalDiff += Math.pow(avg - quotes(i).adjClose, 2)
      i+=1
    }
    var variance = totalDiff / days
    var sdev = Math.sqrt(variance)
    val firstItem = SMAItem(avg, variance, sdev)

    //Compute avg and sdev for all days
    val items = ArrayBuffer.empty[SMAItem]
    i = 0
    while(i < quotes.length) {
      if(i < days) {
        items.append(firstItem)
      } else {
        total -= quotes(i - days).adjClose
        total += quotes(i).adjClose
        totalDiff -= Math.pow(avg - quotes(i - days).adjClose, 2)
        avg = total / days
        totalDiff += Math.pow(avg - quotes(i).adjClose, 2)
        variance = totalDiff / days
        sdev = Math.sqrt(variance)
        items.append(SMAItem(avg, variance, sdev))
      }
      i+=1
    }

    SimpleMovingAverage(items.toArray)
  }
}

object TimeSeries {
  def fromFile(path: String): TimeSeries = {
    val source = Source.fromFile(path)
    val lines = source.getLines.toArray
    source.close

    val quotes = lines.drop(1).map(Quote.parseLine)
      .sortBy(_.dateNum)
    TimeSeries(quotes)
  }

  def fromValues(values: Array[Double]): TimeSeries = {
    TimeSeries(values.zipWithIndex.map{case(v, i) => Quote(i.toString, i, v, v, v, v, v, 1)})
  }
}

case class Quote(date: String, dateNum: Long, open: Double, high: Double, low: Double, close: Double, adjClose: Double, volume: Double)
object Quote {
  def parseLine(line: String): Quote = {
    val a = line.split(",")
    Quote(a(0), a(0).replace("-", "").toLong, a(1).toDouble, a(2).toDouble, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble)
  }
}

case class SimpleMovingAverage(values: Array[SMAItem])

case class SMAItem(avg: Double, variance: Double, sdev: Double)


class BacktestingRun(val budget: Double, val commissionFee: Double, val timeSeries: TimeSeries) {
  private var stopLoss: Option[Double] = None
  private var currentBudget = budget
  private val shares = ArrayBuffer.empty[SharesLot]
  private val trades = ArrayBuffer.empty[Trade]
  private var gains = 0.0
  private var losses = 0.0
  private var gainsCount = 0
  private var lossesCount = 0
  private var lostLastTime = false
  private var conLossesCount = 0
  private var conLossesTotal = 0.0
  private var maxConLossesCount = 0
  private var maxConLossesTotal = 0.0
  private var totalFees = 0.0
  private var stopLossCount = 0

  def run(strategy: InvestmentStrategy): BacktestingOutput = {
    //Init internal status
    this.stopLoss = None
    this.currentBudget = budget
    this.shares.clear()
    this.trades.clear()
    this.gains = 0.0
    this.losses = 0.0
    this.gainsCount = 0
    this.lossesCount = 0
    this.lostLastTime = false
    this.conLossesCount = 0
    this.conLossesTotal = 0.0
    this.maxConLossesCount = 0
    this.maxConLossesTotal = 0.0
    this.totalFees = 0.0
    this.stopLossCount = 0

    //Init strategy
    strategy.init(timeSeries)

    //Loop through time series
    timeSeries.quotes.zipWithIndex.foreach{case (q, i) =>
      val price = q.adjClose
      val date = q.date
      val totalShares = shares.map(_.shares).sum

      //Stop loss or ask strategy for action
      var action = InvestmentAction.Nothing
      if(stopLoss.isDefined && price < stopLoss.get && shares.nonEmpty) {
        action = InvestmentAction.Sell
        stopLossCount += 1
      } else {
        action = strategy.takeAction(q, i, totalShares, currentBudget)
      }

      //Buy as much as we can
      if (action == InvestmentAction.Buy) {
        buyAll(price, date)

        //Sell all
      } else if(action == InvestmentAction.Sell && shares.nonEmpty) {
        sellAll(price, date)
      }
    }

    //Always sell everything at the end
    sellAll(timeSeries.quotes.last.adjClose, "END")

    //Compute performance
    val profit = currentBudget - budget
    val returnP = profit / budget * 100
    val avgGainLoss = (gains / Math.max(gainsCount, 1)) / (losses / Math.max(lossesCount, 1))
    val gainToPain = gains / Math.max(losses, 1)
    BacktestingOutput(currentBudget, profit, returnP, maxConLossesTotal, avgGainLoss, gainsCount,
      lossesCount, totalFees, stopLossCount, gainToPain, losses, trades.toArray)
  }

  private def buyAll(price: Double, date: String) = {
    val sharesToBuy = (currentBudget / price).toInt
    if(sharesToBuy > 0) {
      val lot = SharesLot(sharesToBuy, price)
      shares += lot
      trades += Trade("BuyLong", sharesToBuy, price, date)
      currentBudget -= sharesToBuy * price
      currentBudget -= commissionFee
      totalFees += commissionFee
    }
  }

  private def sellAll(price: Double, date: String) = {
    val totalShares = shares.map(_.shares).sum
    if(totalShares > 0) {

      //Sell all shares
      val mvSell = totalShares * price
      currentBudget += mvSell
      currentBudget -= commissionFee
      totalFees += commissionFee
      trades += Trade("SellLong", totalShares, price, date)

      //Compute profit
      val profit = shares.map(lot => (price - lot.price) * lot.shares).sum - (shares.size + 1) * commissionFee
      shares.clear()

      //Keep track of profit and losses
      if(profit > 0) {
        gains += profit
        gainsCount += 1
        lostLastTime = false
      } else {
        val lossAbs = Math.abs(profit)
        losses += lossAbs
        lossesCount += 1
        if(lostLastTime) {
          conLossesCount += 1
          conLossesTotal += lossAbs
        } else {
          conLossesCount = 1
          conLossesTotal = lossAbs
          lostLastTime = true
        }
        maxConLossesCount = Math.max(maxConLossesCount, conLossesCount)
        maxConLossesTotal = Math.max(maxConLossesTotal, conLossesTotal)
      }
    }
  }

  case class SharesLot(shares: Int, price: Double)
}
object BacktestingRun {

  def runAllAndSave(budget: Double, fee: Double, inputPath: String, strategies: Seq[InvestmentStrategy], outPath: String): Unit = {
    println(s"Loading time series: ${new File(inputPath).getName}")
    val timeSeries = TimeSeries.fromFile(inputPath)
    runAllAndSave(budget, fee, timeSeries, strategies, outPath)
  }

  def runAllAndSave(budget: Double, fee: Double, timeSeries: TimeSeries, strategies: Seq[InvestmentStrategy], outPath: String): Unit = {
    val backtesting = new BacktestingRun(budget, fee, timeSeries)
    runAllAndSave(backtesting, strategies, outPath)
  }

  def runAllAndSave(backtesting: BacktestingRun, strategies: Seq[InvestmentStrategy], outPath: String): Unit = {
    println(s"Running strategies to produce: ${new File(outPath).getName}")
    val writer = new BufferedWriter(new FileWriter(outPath))
    writer.write("Strategy,Profit$,Yield%,TotalLoss,MaxContLoss,TotalTrades,WinningTrades,LosingTrades,Fees,GainToPain")
    var maxYield = -1.0
    var bestStrategy: InvestmentStrategy = null
    var bestOutput: BacktestingOutput = null
    strategies.foreach{strategy =>
      //Run
      val output = backtesting.run(strategy)
      if(output.returnP > maxYield) {
        maxYield = output.returnP
        bestStrategy = strategy
        bestOutput = output
      }

      //Output
      writer.write("\n")
      writer.write(strategy.name)
      writer.write(",")
      writer.write(Backtesting.SMALL_DIGIT_FORMAT.format(output.netProfit))
      writer.write(",")
      writer.write(Backtesting.SMALL_DIGIT_FORMAT.format(output.returnP))
      writer.write(",")
      writer.write(Backtesting.SMALL_DIGIT_FORMAT.format(output.totalLosses))
      writer.write(",")
      writer.write(Backtesting.SMALL_DIGIT_FORMAT.format(output.maxContinuousLoss))
      writer.write(",")
      writer.write(output.trades.size.toString)
      writer.write(",")
      writer.write(output.gainsCount.toString)
      writer.write(",")
      writer.write(output.lossesCount.toString)
      writer.write(",")
      writer.write(Backtesting.SMALL_DIGIT_FORMAT.format(output.totalFees))
      writer.write(",")
      writer.write(Backtesting.SMALL_DIGIT_FORMAT.format(output.gainToPain))
    }
    writer.close()

    if(bestStrategy != null) {
      println(s"Best strategy: ${bestStrategy.name} with yield: ${Backtesting.SMALL_DIGIT_FORMAT.format(bestOutput.returnP)}%, trades:")
      bestOutput.trades.foreach{t =>
        val mv = Backtesting.SMALL_DIGIT_FORMAT.format(t.shares * t.price)
        println(s"\t${t.date}: ${t.action} ${t.shares} at $$${t.price} = $$$mv")
      }
    }

  }

}

case class Trade(action: String, shares: Int, price: Double, date: String)

case class BacktestingOutput(
                              finalBudget: Double,
                              netProfit: Double,
                              returnP: Double,
                              maxContinuousLoss: Double,
                              avgGainLoss: Double,
                              gainsCount: Int,
                              lossesCount: Int,
                              totalFees: Double,
                              stopLossCount: Int,
                              gainToPain: Double,
                              totalLosses: Double,
                              trades: Array[Trade]
                            )

trait InvestmentStrategy {
  val name: String
  def init(timeSeries: TimeSeries): Unit = {}
  def takeAction(quote: Quote, quoteIndex: Int, currentShares: Int, currentBudget: Double): InvestmentAction.Value
}

object InvestmentAction extends Enumeration {
  val Buy, Sell, Nothing = Value
}

abstract class BaseSmaStrategy(val days: Int) extends InvestmentStrategy {
  protected var sma: SimpleMovingAverage = _
  override def init(timeSeries: TimeSeries): Unit = {
    this.sma = timeSeries.computeMovingAverage(days)
  }
}

class SmaSdevStrategy(override val days: Int, val bandSize: Double) extends BaseSmaStrategy(days) {
  override val name: String = s"SMA-SDEV($days-${Backtesting.SMALL_DIGIT_FORMAT.format(bandSize)})"
  override def takeAction(quote: Quote, quoteIndex: Int, currentShares: Int, currentBudget: Double): InvestmentAction.Value = {
    val price = quote.adjClose
    val smaItem = this.sma.values(quoteIndex)
    val avg = smaItem.avg
    val upperBand = avg + bandSize * smaItem.sdev
    val lowerBand = avg - bandSize * smaItem.sdev

    if(price > upperBand) {
      InvestmentAction.Buy
    } else if(price < upperBand) {
      InvestmentAction.Sell
    } else {
      InvestmentAction.Nothing
    }
  }
}

class SmaPercentStrategy(override val days: Int, val bandPercentage: Double) extends BaseSmaStrategy(days) {
  override val name: String = s"SMA-Perc($days-${Backtesting.SMALL_DIGIT_FORMAT.format(bandPercentage)}%)"
  override def takeAction(quote: Quote, quoteIndex: Int, currentShares: Int, currentBudget: Double): InvestmentAction.Value = {
    val price = quote.adjClose
    val smaItem = this.sma.values(quoteIndex)
    val avg = smaItem.avg
    val bandMovement = bandPercentage/100 * avg
    val upperBand = avg + bandMovement
    val lowerBand = avg - bandMovement

    if(price > upperBand) {
      InvestmentAction.Buy
    } else if(price < upperBand) {
      InvestmentAction.Sell
    } else {
      InvestmentAction.Nothing
    }
  }
}

class FixedStrategy(val min: Double, val max: Double) extends InvestmentStrategy {
  override val name: String = s"MinMax($min-$max)"
  override def takeAction(quote: Quote, quoteIndex: Int, currentShares: Int, currentBudget: Double): InvestmentAction.Value = {
    val price = quote.adjClose
    if(price <= min) {
      InvestmentAction.Buy
    } else if(price > max) {
      InvestmentAction.Sell
    } else {
      InvestmentAction.Nothing
    }
  }
}