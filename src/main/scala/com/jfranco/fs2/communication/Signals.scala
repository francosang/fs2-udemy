package com.jfranco.fs2.communication

import fs2.*
import cats.effect.*
import fs2.concurrent.SignallingRef
import scala.concurrent.duration._

import scala.util.Random

/** A way for streams to communicate.
  */
object Signals extends IOApp.Simple {

  override def run: IO[Unit] = {

    def signaller(signal: SignallingRef[IO, Boolean]): Stream[IO, Nothing] =
      Stream
        .repeatEval(IO(Random.between(1, 1000)))
        .evalTap(it => IO.println(s"Generating $it"))
        .metered(100.millis)
        .evalMap(i => if (i % 5 == 0) signal.set(true) else IO.unit)
        .drain

    def worker(signallingRef: SignallingRef[IO, Boolean]): Stream[IO, Nothing] =
      Stream
        .repeatEval(IO.println("Working..."))
        .metered(50.millis)
        .interruptWhen(signallingRef)
        .drain

    Stream
      .eval(SignallingRef[IO, Boolean](false))
      .flatMap { ref =>
        worker(ref).concurrently(signaller(ref))
      }
      .compile
      .drain

    type Temperature = Double
    def tempSensor(
        alarm: SignallingRef[IO, Option[Temperature]],
        threshold: Temperature
    ): Stream[IO, Nothing] = Stream
      .repeatEval(IO(Random.between(-40, 40)))
      .evalTap(t => IO.println(s"Temp $t"))
      .evalMap(t =>
        if (t > threshold) alarm.set(Some(t)) *> IO.println(s"Setting $t")
        else IO.unit
      )
      .metered(100.millis)
      .drain

    def cooler(
        alarm: SignallingRef[IO, Option[Temperature]]
    ): Stream[IO, Nothing] =
      alarm.discrete
        .collect { case Some(t) => t }
        .evalTap(t => IO.println(s"Warn! Temp: $t"))
        .drain

    Stream
      .eval(SignallingRef[IO, Option[Temperature]](None))
      .flatMap { ref =>
        tempSensor(ref, 21).merge(cooler(ref))
      }
      .interruptAfter(2.seconds)
      .compile
      .drain
  }
}
