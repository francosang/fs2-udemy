package com.jfranco.fs2.pipelines

import fs2._
import cats.effect._
import fs2.Stream.ToPull

/** Pulls are the plumbing underlying streams.
  * All streams and stream transformations are built using pulls.
  *
  *  - Stream transformations are built using pulls.
  *  - All streams can be converted to pulls, and some pulls can be converted to streams.
  *  - Pulls can output elements and return a result.
  *  - Pulls are monads — they can be composed using flatMap.
  */
object Pulls extends IOApp.Simple {
  override def run: IO[Unit] = {
    val s = Stream(1, 2) ++ Stream(3) ++ Stream(4, 5)

    val outputPull: Pull[Pure, Int, Unit] = Pull.output1(1)
    IO.println(outputPull.stream.toList)

    val outputChunk = Pull.output(Chunk(1, 2, 3))
    IO.println(outputChunk.stream.toList)

    val donePull: Pull[Pure, Nothing, Unit] = Pull.done

    val purePull: Pull[Pure, Nothing, Int] = Pull.pure(5)

    val combined =
      for {
        _ <- Pull.output1(1)
        _ <- Pull.output(Chunk(2, 3, 4))
      } yield ()
    IO.println(combined.stream.toList)
    IO.println(combined.stream.chunks.toList)

    val emptyPull: Pull[Pure, Nothing, Unit] = Pull.done
    val emptyStream: Stream[Pure, Nothing] = emptyPull.stream
    val emptyPullAgain: Pull[Pure, Nothing, Unit] = emptyStream.pull.echo

    // This is an intermediate representation, it allows to retrieve the pull
    val emptyToPull: ToPull[Pure, Nothing] = emptyStream.pull

    // Unlike streams, pulls have a result.
    // The pulls you’ve seen so far all have a result type of Unit.
    // To demonstrate, let’s examine the type of the helloPull.

    // It has an output type of String, as we expect, and a result of Unit:
    val helloPull: Pull[Pure, String, Unit] = Pull.output1("hello")

    // We can build a pull with a more useful result using Pull.pure.
    // This has an output type of INothing, meaning it doesn't output any values,
    // and a result of String.
    // You can think of the result as a temporary value that can be used to construct other pulls.
    val helloResult: Pull[Nothing, Nothing, String] = Pull.pure("hello")

    val helloStream = helloPull.stream
    val res: Pull[Pure, String, Option[Stream[Pure, String]]] =
      helloStream.pull.take(2)

    // Take a moment to examine that type signature of res:
    // - It gives us a pull with an output type of String:
    //   it outputs the number of elements we ask it to take.
    // - More interestingly, it results in an Option[Stream[Nothing, String]].
    //   This represents the rest of the stream,
    //   which we can use to manipulate the remaining output.

    val toPull: ToPull[Pure, Int] = s.pull
    val echoPull: Pull[Pure, Int, Unit] = s.pull.echo
    val takePull: Pull[Pure, Int, Option[Stream[Pure, Int]]] = s.pull.take(3)
    val dropPull: Pull[Pure, Int, Option[Stream[Pure, Int]]] = s.pull.drop(3)

    // Exercise -> implement using pulls
    def skipLimit[A](skip: Int, limit: Int)(s: Stream[IO, A]): Stream[IO, A] = {
      s.pull
        .drop(skip)
        .flatMap {
          case Some(value) =>
            value.pull.take(limit) >> Pull.done
          case None => Pull.done
        }
        .stream
    }
    skipLimit(10, 10)(Stream.range(1, 100)).compile.toList.flatMap(IO.println)
    skipLimit(1, 15)(Stream.range(1, 5)).compile.toList.flatMap(IO.println)

    val unconsedRange
        : Pull[Pure, Nothing, Option[(Chunk[Int], Stream[Pure, Int])]] =
      s.pull.uncons
    def firstChunk[A]: Pipe[Pure, A, A] = s => {
      s.pull.uncons.flatMap {
        case Some((chunk, restOfStream)) => Pull.output(chunk)
        case None                        => Pull.done
      }.stream
    }
    IO.println(s.through(firstChunk).toList)

    def drop[A](n: Int): Pipe[Pure, A, A] = s => {
      def go(s: Stream[Pure, A], n: Int): Pull[Pure, A, Unit] = {
        s.pull.uncons.flatMap {
          case Some((chunk, restOfStream)) =>
            if (chunk.size < n) go(restOfStream, n - chunk.size)
            else Pull.output(chunk.drop(n)) >> restOfStream.pull.echo
          case None => Pull.done
        }
      }
      go(s, n).stream
    }
    IO.println(s.through(drop(-1)).toList)

    // Exercise
    def filter[A](p: A => Boolean): Pipe[Pure, A, A] = s => {
      def go(s: Stream[Pure, A]): Pull[Pure, A, Unit] = {
        s.pull.uncons.flatMap {
          case Some((chunk, restOfStream)) =>
            Pull.output(chunk.filter(p)) >> go(restOfStream)
          case None => Pull.done
        }
      }
      go(s).stream
    }
    IO.println(s.through(filter(_ % 2 == 1)).toList)

    def runningSum: Pipe[Pure, Int, Int] = s => {
      s.scanChunks(0) { (sumAcc, chunk: Chunk[Int]) =>
        val newSum =
          chunk.foldLeft[Int](0)(_ + _) + sumAcc
        (newSum, Chunk.singleton(newSum))
      }
    }
    IO.println(s.through(runningSum).toList)

    // Exercise
    def runningMax: Pipe[Pure, Int, Int] = s => {
      s.scanChunks(Int.MinValue) { (maxAcc, chunk: Chunk[Int]) =>
        {
          val max = chunk.foldLeft(maxAcc)(_.max(_)).max(maxAcc)
          (max, Chunk.singleton(max))
        }
      }
    }
    IO.println(s.through(runningMax).toList)
    val t = Stream(-1, -2, -3) ++ Stream(10) ++ Stream(4, 7, 1)
    IO.println(t.through(runningMax).toList)
  }
}
