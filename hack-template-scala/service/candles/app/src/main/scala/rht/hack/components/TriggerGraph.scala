package rht.hack.components

import akka.Done
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.{ClosedShape, CompletionStrategy, OverflowStrategy}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import com.redis.RedisClient
import net.liftweb.json.parse


object TriggerGraph {
  implicit val actorSystem: ActorSystem = ActorSystem("actorSystem")
  val redisClient: RedisClient = new RedisClient("localhost", 6379, secret = Some("sOmE_sEcUrE_pAsS"), database = 10)

  def run(): ActorRef = {
    val inRedisTriggers = Source.actorRef(
      completionMatcher = {
        case Done =>
          CompletionStrategy.immediately
      },
      failureMatcher = PartialFunction.empty,
      bufferSize = 2000,
      overflowStrategy = OverflowStrategy.dropHead)

    val graph = RunnableGraph.fromGraph(GraphDSL.create(inRedisTriggers) {
      implicit builder =>
        inRedisTriggers =>
          import GraphDSL.Implicits._

          val out = Sink.foreach(println)

          val publishMessage = Flow[String]
            .map {
              value =>
                val mapValue = parse(value).values.asInstanceOf[Map[String, Any]]
                (
                  List(mapValue.getOrElse("target", ""),
                    mapValue.getOrElse("direction", ""),
                    mapValue.getOrElse("figi", "")) mkString ":",
                  value
                )
            }.map {
            case (key, value) =>
              redisClient.lpush(key, value)
          }

          inRedisTriggers ~> publishMessage ~> out
          ClosedShape
    })
    graph.run()
  }
}
