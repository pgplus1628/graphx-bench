/**
 * Created by z on 12/10/15.
 */
package org.zork.graphx


class Timer {

  var t0 : Long = 0L
  var t1 : Long = 0L

  def reset() : Unit = {
    t0 = 0L
    t1 = 0L
  }

  def start() : Unit = {
    t0 = System.currentTimeMillis()
  }

  def stop() : Unit = {
    t1 = System.currentTimeMillis()
  }

  def elapsed() : Long = {
    return t1 - t0
  }

}
