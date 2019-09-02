package com.mandelag.mba

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, Sink}
import akka.util.ByteString

object MarketBasketAnalysis {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem.create("MarketBasketAnalysis")
    implicit val materializer = ActorMaterializer.create(system)
    implicit val executor = system.dispatcher

    val transactionCsv = Paths.get("basket.csv")

    val transactionData = FileIO.fromPath(transactionCsv)
      .via(Framing.delimiter(ByteString("\n"), Integer.MAX_VALUE,true))
      .map(_.utf8String)
      .map(_.split(","))

    val mbaCsvFormat =
      Flow[Map[(Set[String], Set[String]), Int]]
      .map(associations => {
        associations.keys.map( key => {
          val key1 = key._1.toList.sorted.mkString("|")
          val key2 = key._2.toList.sorted.mkString("|")
          val count = associations(key)
          ByteString(List(key1, key2, count).mkString(","))
        }).toList
      })
      .mapConcat(i => i)
      .intersperse(ByteString("\n"))

    val (killSwitch, future) = transactionData
      .via(MbaPipeline.core)
      .via(mbaCsvFormat)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(FileIO.toPath(Paths.get("analysis-result.csv")))(Keep.both)
      .run()

    future.onComplete(x => {
      println("Done!")
      killSwitch.shutdown()
      system.terminate()
    })

  }

}
