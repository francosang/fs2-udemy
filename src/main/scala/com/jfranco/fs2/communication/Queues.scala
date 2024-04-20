package com.jfranco.fs2.communication

import fs2._
import cats.effect._

import scala.concurrent.duration._
import cats.effect.std._
import scala.util.Random

import java.time.LocalDateTime

object Queues extends IOApp.Simple {
  override def run: IO[Unit] = {
    Stream
      .eval(Queue.unbounded[IO, Int])
      .flatMap { queue =>
        Stream.eval(Ref.of[IO, Int](0)).flatMap { ref =>
          val producer =
            Stream
              .iterate(0)(_ + 1)
              .covary[IO]
              .evalMap(e => IO.println(s"Offering $e") *> queue.offer(e))
              .drain
          val consumer =
            Stream
              .fromQueueUnterminated(queue)
              .evalMap(e =>
                IO.println(s"Received value: $e - Updating state.") *> ref
                  .update(_ + e)
              )
              .metered(300.millis)
              .drain
          producer.merge(consumer).interruptAfter(3.seconds) ++ Stream
            .eval(IO.println("Final state:") *> ref.get.flatMap(IO.println))
        }
      }
      .compile
      .drain

    Stream
      .eval(Queue.unbounded[IO, Option[Int]])
      .flatMap { queue =>
        val p = (
          Stream.range(0, 10).map(Some.apply) ++
            Stream(None) ++
            Stream(Some(11))
        ).evalMap(queue.offer)
        val c =
          Stream.fromQueueNoneTerminated(queue).evalMap(i => IO.println(i))
        c.merge(p)
      }
      .interruptAfter(5.seconds)
      .compile
      .drain

    trait Controller {
      def postAccount(
          customerId: Long,
          accountType: String,
          creationDate: LocalDateTime
      ): IO[Unit]
    }

    class Server(controller: Controller) {
      def start(): IO[Nothing] = {
        val prog =
          for {
            randomWait <- IO(math.abs(Random.nextInt()) % 500)
            _ <- IO.sleep(randomWait.millis)
            id = Random.between(1L, 1000L)
            _ <- controller.postAccount(
              customerId = id,
              accountType = if (Random.nextBoolean()) "ira" else "brokerage",
              creationDate = LocalDateTime.now()
            )
            _ <- IO.println(s"Producing: $id")
          } yield ()
        prog.foreverM
      }
    }

    object PrintController extends Controller {
      override def postAccount(
          customerId: Long,
          accountType: String,
          creationDate: LocalDateTime
      ): IO[Unit] = {
        IO.println(
          s"Initiating account creation. Customer: $customerId Account type: $accountType Created: $creationDate"
        )
      }
    }

    case class CreateAccountData(
        customerId: Long,
        accountType: String,
        creationDate: LocalDateTime
    )
    class QueueController(queue: Queue[IO, CreateAccountData])
        extends Controller {
      override def postAccount(
          customerId: Long,
          accountType: String,
          creationDate: LocalDateTime
      ): IO[Unit] = {
        queue.offer(CreateAccountData(customerId, accountType, creationDate))
      }
    }

    // Exercise
    // Create a stream that emits the queue
    // Create a stream for the server (started)
    // Create a consumer stream which reads from the queue and prints the message
    // Run everything concurrently
    Stream
      .eval(Queue.unbounded[IO, CreateAccountData])
      .flatMap { queue =>
        val controller = QueueController(queue)
        val server = Server(controller)

        val consumer =
          Stream
            .fromQueueUnterminated(queue)
            .evalMap(e => IO.println(s"Consumed: $e"))
            .drain

        Stream(
          Stream.eval(server.start()),
          consumer
        ).parJoinUnbounded
      }
      .compile
      .drain
  }
}
