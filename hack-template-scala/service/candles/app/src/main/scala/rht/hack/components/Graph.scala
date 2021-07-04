package rht.hack.components

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.{ClosedShape, CompletionStrategy, OverflowStrategy}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import com.redis.RedisClient
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import rht.common.domain.candles.Candle
import rht.hack.components.Metrics.{avg, findMax, findMin}
import rht.hack.components.PubSub.{Pub, Sub}

import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES}


case class PublishMessage(channel: String, message: String)

case class SubscribeMessage(channels: List[String])

case class UnsubscribeMessage(channels: List[String])

case object GoDown

case class TriggerKey(targetCurrency: BigDecimal, dir: Boolean, figi: String) {
  override def toString: String = s"$targetCurrency:${if (dir) 1 else 0}:$figi"
}

object Graph {

  implicit val actorSystem: ActorSystem = ActorSystem("actorSystem")
  val ps: ActorRef = actorSystem.actorOf(Props(new Pub))
  val ss: ActorRef = actorSystem.actorOf(Props(new Sub(TriggerGraph.run())))
  val redisClient: RedisClient = new RedisClient("localhost", 6379, secret = Some("sOmE_sEcUrE_pAsS"), database = 10)

  ss ! SubscribeMessage(List("stream_triggers"))

  def candle2json(candle: Candle): String = {
    val map: Map[String, BigDecimal] = Map(candle.figi.value -> candle.details.close)
    compactRender(map2jvalue(map))
  }

  def parseKey(key: Option[String]): TriggerKey = {
    val lst = key.get.split(":")
    TriggerKey(BigDecimal(lst(0)), if (lst(1) == "0") true else false, lst(2))
  }

  def run(): ActorRef = {
    val inKafka = Source.actorRef(
      completionMatcher = {
        case Done =>
          CompletionStrategy.immediately
      },
      failureMatcher = PartialFunction.empty,
      bufferSize = 2000,
      overflowStrategy = OverflowStrategy.dropHead)

    val graph = RunnableGraph.fromGraph(GraphDSL.create(inKafka) {
      implicit builder =>
        inKafka =>
          import GraphDSL.Implicits._

          val out = Sink.ignore
          val bCast = builder.add(Broadcast[Candle](5))

          val publishMessage = Flow[Candle]
            .map(candle => ps ! PublishMessage("stream_currency", candle2json(candle)))

          val metricMinMax =
            Flow[Candle]
              .groupBy(10, _.figi)
              .map(candle => (candle.figi, findMin(candle), findMax(candle)))
              .mergeSubstreams

          val metricSliding =
            Flow[Candle]
              .groupBy(10, _.figi)
              .sliding(30, 1)
              .filterNot(_.isEmpty)
              .map(arr => (arr.head.figi, avg(arr)))
              .mergeSubstreams

          val metricTenMinutes =
            Flow[Candle]
              .groupBy(10, _.figi)
              .groupedWithin(10000, new FiniteDuration(Duration("10 minutes").toMinutes, MINUTES))
              .filterNot(_.isEmpty)
              .map(arr => (arr.head.figi, avg(arr)))
              .mergeSubstreams

          val triggerProcessing =
            Flow[Candle]
              .groupBy(10, _.figi)
              .map(candle => redisClient.keys().map(x => x.map(parseKey))
                  .getOrElse(List.empty)
                  .filter(key => key.figi == candle.figi.value &&
                    (key.dir && key.targetCurrency <= candle.details.close ||
                      !key.dir && key.targetCurrency >= candle.details.close)))

              .map {
                triggersKey =>
                  triggersKey
                    .flatMap(key => redisClient.lrange(key.toString, 0, -1)
                      .getOrElse(List.empty)
                      .map {
                        jsonOpt =>
                          jsonOpt.map(json => ps ! PublishMessage("events_stream", json))
                      }
                    )
                  triggersKey.map(key => redisClient.del(key.toString))
              }.mergeSubstreams

          inKafka ~> bCast
          bCast ~> publishMessage ~> out
          bCast ~> metricSliding ~> Sink.foreach((y: Any) => println(s"Average at last 30 updates: $y"))
          bCast ~> metricMinMax ~> Sink.foreach((y: Any) => println(s"Minimum and maximum at the all time: $y"))
          bCast ~> metricTenMinutes ~> Sink.foreach((y: Any) => println(s"Average at 10 minutes: $y"))
          bCast ~> triggerProcessing ~> Sink.ignore
          ClosedShape
    })

    graph.run()
  }
}
