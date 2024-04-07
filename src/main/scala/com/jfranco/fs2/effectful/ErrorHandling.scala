package com.jfranco.fs2.effectful

import cats.effect.{IO, IOApp}
import fs2._

object ErrorHandling extends IOApp.Simple {

  private def streamIOError: Stream[IO, Unit] =
    Stream.eval(IO.raiseError(new Exception("boom!")))

  private def streamError: Stream[IO, Unit] =
    Stream.raiseError(new Exception("boom!"))

  private def streamWithErrors: Stream[IO, Unit] =
    Stream
      .repeatEval(IO.println("Emitting..."))
      .take(3) ++
      Stream
        .raiseError[IO](new Exception("boom!"))

  private def streamStartingWithErrors: Stream[IO, Unit] =
    Stream
      .raiseError[IO](new Exception("boom!")) ++
      Stream
        .repeatEval(IO.println("Emitting..."))
        .take(3)

  private def streamRandomErrors: Stream[IO, Double] =
    Stream.repeatEval(IO(math.random())).evalMap { randomValue =>
      if (randomValue < 0.9)
        IO.println(s"Processing $randomValue...").as(randomValue)
      else
        IO.println("Error occurred") *> IO.raiseError(
          new Exception(s"Error $randomValue....")
        )
    }

  implicit class RichStream[A](s: Stream[IO, A]) {
    def flatAttempt: Stream[IO, A] = {
      s.attempt.collect { case Right(value) => value }
    }
  }

  override def run: IO[Unit] = {
    // streamIOError.compile.drain.void
    // streamWithErrors.compile.drain.void
    // streamError.compile.drain.void
    // streamStartingWithErrors.compile.drain.void

    // Crashes, lets the error rise
    streamRandomErrors.compile.drain.void

    // Handle error without crash, but stops processing
    streamRandomErrors
      .take(10)
      .handleErrorWith(err =>
        Stream.eval(IO.println(s"Handle error: ${err.getMessage}"))
      )
      .compile
      .drain
      .void

    // Use attempt to handle error, elements are wrapped in either, not crashes but stops processing
    streamRandomErrors
      .take(10)
      .attempt
      .compile
      .toList
      .flatMap(IO.println)
      .void

    // Use flatAttempt to discard the errors and only keep the values (read previous comment)
    streamRandomErrors
      .take(10)
      .flatAttempt
      .compile
      .toList
      .flatMap(IO.println)
      .void

    // TODO: take 10 successful elements in total, not taking into consideration the errors.
    streamRandomErrors
      .attempt
      .flatMap {
        case Right(value) => Stream.emit(value)
        case Left(_)      => Stream.empty
      }
      .take(10)
      .compile
      .toList
      .flatMap(IO.println)
      .void
  }
}
