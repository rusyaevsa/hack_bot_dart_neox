package rht.hack

import rht.hack.components.Graph
/**
  * Program entry point
  *
  * You can create another one temporarily to test your code,
  * but the final entry point must be this one!
  */
object Main extends HackathonApp {
  /**
    * Your "main" function
    *
    * @param args program arguments
    * @return Actor, which will be used to push events to Akka Stream
    */
  override def start(args: List[String]): SourceActor = {
    Graph.run()
  }

}
