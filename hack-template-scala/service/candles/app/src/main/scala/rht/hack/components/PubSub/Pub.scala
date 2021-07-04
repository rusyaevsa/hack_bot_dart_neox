package rht.hack.components.PubSub

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.redis.RedisClient
import rht.hack.components.{GoDown, PublishMessage}

class Pub extends Actor {
  println("starting publishing service ..")
  val system: ActorSystem = ActorSystem("pub")
  val redisClient = new RedisClient("localhost", 6379, secret = Some("sOmE_sEcUrE_pAsS"))
  val p: ActorRef = system.actorOf(Props(new Publisher(redisClient)))

  def receive: Receive = {
    case PublishMessage(ch, msg) => publish(ch, msg)
    case GoDown =>
      redisClient.quit
      system.terminate()
    case x => println("Got in Pub " + x)
  }

  def publish(channel: String, message: String): Unit = {
    p ! Publish(channel, message)
  }
}
