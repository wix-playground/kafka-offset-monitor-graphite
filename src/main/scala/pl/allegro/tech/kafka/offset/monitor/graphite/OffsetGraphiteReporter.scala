package pl.allegro.tech.kafka.offset.monitor.graphite

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.codahale.metrics.graphite.{GraphiteReporter, GraphiteSender, GraphiteUDP}
import com.codahale.metrics.{Gauge, MetricFilter, MetricRegistry}
import com.google.common.cache._
import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.readytalk.metrics.StatsDReporter

class OffsetGraphiteReporter (pluginsArgs: String) extends com.quantifind.kafka.offsetapp.OffsetInfoReporter {

  GraphiteReporterArguments.parseArguments(pluginsArgs)

  val metrics : MetricRegistry = new MetricRegistry()


  val graphite : GraphiteSender = {
    val address = new InetSocketAddress(GraphiteReporterArguments.graphiteHost, GraphiteReporterArguments.graphitePort)
    new GraphiteUDP(address)
  }

  StatsDReporter.forRegistry(metrics)
    .prefixedWith(GraphiteReporterArguments.graphitePrefix)
    .convertRatesTo(TimeUnit.MINUTES)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .filter(MetricFilter.ALL)
    .build(GraphiteReporterArguments.graphiteHost, GraphiteReporterArguments.graphitePort)
    .start(GraphiteReporterArguments.graphiteReportPeriod, TimeUnit.SECONDS)

  val removalListener : RemovalListener[String, GaugesValues] = new RemovalListener[String, GaugesValues] {
    override def onRemoval(removalNotification: RemovalNotification[String, GaugesValues]) = {
      metrics.remove(removalNotification.getKey() + "offset")
      metrics.remove(removalNotification.getKey() + "logSize")
      metrics.remove(removalNotification.getKey() + "lag")
    }
  }

  val gauges : LoadingCache[String, GaugesValues] = CacheBuilder.newBuilder()
    .expireAfterAccess(GraphiteReporterArguments.metricsCacheExpireSeconds, TimeUnit.SECONDS)
    .removalListener(removalListener)
    .build(
      new CacheLoader[String, GaugesValues]() {
        def load(key: String): GaugesValues = {
          val values: GaugesValues = new GaugesValues()

          val offsetGauge: Gauge[Long] = new Gauge[Long] {
            override def getValue: Long = {
              values.offset
            }
          }

          val lagGauge: Gauge[Long] = new Gauge[Long] {
            override def getValue: Long = {
              values.lag
            }
          }

          val logSizeGauge: Gauge[Long] = new Gauge[Long] {
            override def getValue: Long = {
              values.logSize
            }
          }

          metrics.register(key + "offset", offsetGauge)
          metrics.register(key + "logSize", logSizeGauge)
          metrics.register(key + "lag", lagGauge)

          values
        }
      }
   )

  override def report(info: scala.IndexedSeq[OffsetInfo]) =  {
    info.foreach(i => {
      val values: GaugesValues = gauges.get(getMetricName(i))
      values.logSize = i.logSize
      values.offset = i.offset
      values.lag = i.lag
    })
  }

  def getMetricName(offsetInfo: OffsetInfo): String = {
    "topic_" + offsetInfo.topic.replace(".", "_") + ".group_" + offsetInfo.group.replace(".", "_") + ".partition_" + offsetInfo.partition + ".gauge_"
  }
}
