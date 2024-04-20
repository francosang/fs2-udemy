package com.jfranco.fs2.communication

import fs2.*
import fs2.concurrent.*
import cats.effect.*

import scala.concurrent.duration.*
import scala.util.Random

object Topics extends IOApp.Simple {
  override def run: IO[Unit] = {
    Stream
      .eval(Topic[IO, Int])
      .flatMap { topic =>
        val publisher = Stream
          .iterate(1)(_ + 1)
          .covary[IO]
          .through(topic.publish)
          .drain

        val consumer1 = topic
          .subscribe(10)
          .evalMap(i => IO.println(s"Read $i from c1"))
          .drain

        val consumer2 = topic
          .subscribe(10)
          .evalMap(i => IO.println(s"Read $i from c2"))
          .metered(200.millis)
          .drain

        Stream(publisher, consumer1, consumer2).parJoinUnbounded
      }
      .interruptAfter(3.seconds)
      .compile
      .drain

    case class CarPosition(id: Long, lat: Double, lng: Double)

    def createCar(
        id: Long,
        topic: Topic[IO, CarPosition]
    ): Stream[IO, Nothing] =
      Stream
        .repeatEval(
          IO(
            CarPosition(
              id,
              Random.between(-90.0, 90.0),
              Random.between(-90.0, 90.0)
            )
          )
        )
        .evalTap(c => IO.println(s"Updating position of car: ${c.id}"))
        .metered(1.second)
        .through(topic.publish)
        .drain

    def createMapUpdater(topic: Topic[IO, CarPosition]): Stream[IO, Nothing] =
      topic.subscribe(10).evalMap(pos => IO.println(s"Draw car $pos")).drain

    def createDriverNotifier(
        topic: Topic[IO, CarPosition],
        shouldNotify: CarPosition => Boolean,
        notify: CarPosition => IO[Unit]
    ): Stream[IO, Nothing] = {
      topic
        .subscribe(10)
        .filter(shouldNotify)
        .evalTap(notify)
        .drain
    }

    Stream
      .eval(Topic[IO, CarPosition])
      .flatMap { topic =>
        val cars = Stream.range(1, 10).map(createCar(_, topic))
        val mapUpdater = createMapUpdater(topic)
        val notifier = createDriverNotifier(
          topic,
          car => car.id % 5 == 0,
          car => IO.println(s"Sending notification to car: ${car.id}")
        )

        (cars ++ Stream(mapUpdater, notifier)).parJoinUnbounded
      }
      .interruptAfter(3.seconds)
      .compile
      .drain
  }
}
