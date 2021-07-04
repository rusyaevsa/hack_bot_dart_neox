package rht.hack.components.PubSub

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.redis.{E, M, PubSubMessage, RedisClient, S, U}
import net.liftweb.json.parse
import rht.hack.components.{GoDown, SubscribeMessage, UnsubscribeMessage}


class Sub(triggerListener: ActorRef) extends Actor {

  def json2trigger(json: String): Map[String, Any] = {
    parse(json).values.asInstanceOf[Map[String, Any]]
  }

  println("starting subscription service ..")
  val system: ActorSystem = ActorSystem("sub")
  val redisClient: RedisClient = new RedisClient("localhost", 6379, secret = Some("sOmE_sEcUrE_pAsS"))
  val s: ActorRef = system.actorOf(Props(new Subscriber(redisClient)))
  s ! Register(callback)

  def receive: Receive = {
    case SubscribeMessage(chs) => sub(chs)
    case UnsubscribeMessage(chs) => unsub(chs)
    case GoDown =>
      redisClient.quit
      system.terminate()

    case x => println("Got in Sub " + x)
  }

  def sub(channels: List[String]): Unit = {
    s ! Subscribe(channels.toArray)
  }

  def unsub(channels: List[String]): Unit = {
    s ! Unsubscribe(channels.toArray)
  }

  def callback(pubsub: PubSubMessage): Unit = pubsub match {
    case E(exception) => println(exception)
    case S(channel, no) => println("subscribed to " + channel + " and count = " + no)
    case U(channel, no) => println("unsubscribed from " + channel + " and count = " + no)
    case M(channel, msg) =>
      msg match {
        case "exit" =>
          println("unsubscribe all ..")
          redisClient.unsubscribe
        case x =>
          triggerListener ! x
          println("received message on channel " + channel + " as : " + x)
      }
  }
}
