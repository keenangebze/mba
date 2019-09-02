package com.mandelag.mba

import akka.NotUsed
import akka.stream.scaladsl._

import scala.collection.mutable.ArrayBuffer

object MbaPipeline {
  val DEFAULT_GROUP_SIZE = 100000

  // Core of the Market Basket Analysis
  def core: Flow[Array[String], Map[(Set[String], Set[String]), Int], NotUsed] = Flow[Array[String]]
    .mapConcat(items => {
      Utilities.generateCombination(items).toList
    })
    .async
    .mapConcat(itemCombination => {
      val subpattern = Utilities.generateCombination(itemCombination.toArray)
      subpattern
        .map(sp => (sp, itemCombination -- sp))
        .toList
    })
    .async
    .grouped(DEFAULT_GROUP_SIZE)
    .map( association => {
      val res = association.foldLeft(scala.collection.mutable.Map[(Set[String], Set[String]), Int]())((m, tuple) => {
        val prev = m.getOrElse(tuple, 0)
        m(tuple) = prev + 1
        m
      })
      res.toMap
    })
}

object Utilities {

  // Bentley, Jon (2000), Programming Pearls, ACM Press/Addison–Wesley, pp. 116, 121
  // from https://en.wikipedia.org/wiki/Insertion_sort#Algorithm
  def insertionSort(items: ArrayBuffer[String]): Unit = {
    var i = 1
    while (i < items.length - 1) {
      var j = i
      while (j > 0 && (items(j-1) > items(j)) ) {
        val tmp = items(j-1) // simple swap
        items(j-1) = items(j)
        items(j) = tmp

        j = j - 1
      }
      i = i + 1
    }
  }

  def sortAndRemoveDuplicate(items: Array[String]): Array[String] = {
    val buf: ArrayBuffer[String] = ArrayBuffer(items: _*)
    Utilities.insertionSort(buf)
    buf.distinct.toArray
  }

  def generateCombination(distinctItems: Array[String]): Iterator[Set[String]] = {
    val set: Set[String] = distinctItems.toSet
    val subsets = set.subsets()
    subsets
  }
}
