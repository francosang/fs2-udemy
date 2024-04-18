package com.jfranco.fs2.pipelines

import cats.effect.{IO, IOApp}
import fs2._
import cats.effect._

/** https://kebab-ca.se/chapters/fs2/pulls.html
  */
object PullsAnotherClass extends IOApp.Simple {

  type Dough = Int
  type Jiaozi = Int
  type Bowl = List[Jiaozi]
  type Leftovers = List[Jiaozi]
  type Box = Ref[IO, Leftovers]

  def roll(rollsToMake: Int): Stream[Pure, Dough] =
    Stream.iterate(0)(_ + 1).take(rollsToMake)

  val cook: Pipe[Pure, Dough, Jiaozi] = _.flatMap { dough =>
    Stream(
      dough * 3,
      dough * 3 + 1,
      dough * 3 + 2
    )
  }

  def serve(jiaoziToServe: Int): Pipe[Pure, Jiaozi, Jiaozi] =
    // Take outputs the specified number of elements but discards the rest.
    // This discards the leftovers.
    // We need to change it.
    _.take(jiaoziToServe)

  /** take a number of jiaozi from the stream, just as take,
    * but should send the remaining jiaozi down the store pipe.
    */
  def serveThen(
      numberOfRolls: Int,
      store: Pipe[IO, Jiaozi, Nothing]
  ): Pipe[IO, Jiaozi, Jiaozi] =
    _.pull
      .take(numberOfRolls)
      .flatMap {
        case Some(rest) => rest.through(store).pull.echo
        case None       => Pull.done
      }
      .stream

  def store(box: Box): Pipe[IO, Jiaozi, Nothing] =
    _.evalMap(jiaozi => box.update(jiaozi :: _)).drain

  /** Simulate the process of making jiaozi.
    *
    * It does not work as expected. The leftovers are not being stored in the box.
    *
    * We can not both output the jiaozi and store the leftovers in the same stream.
    * We need to change the signatures of the methods/pipes.
    */
  def sim(numberOfRolls: Int, jiaoziToServe: Int): IO[(Bowl, Leftovers)] = {
    val emptyBox: IO[Box] = Ref.of(Nil)
    for {
      box <- emptyBox
      bowl = roll(numberOfRolls)
        .through(cook)
        .through(serve(jiaoziToServe))
        .compile
        .toList
      leftovers <- box.get
    } yield (bowl, leftovers)
  }

  /** This version stores the leftovers in the box.
    * The serve pipe now that redirects the leftovers to the box.
    */
  def simWithLeftovers(
      numberOfRolls: Int,
      jiaoziToServe: Int
  ): IO[(Bowl, Leftovers)] = {
    val emptyBox: IO[Box] = Ref.of(Nil)
    for {
      box <- emptyBox
      bowl <- roll(numberOfRolls)
        .through(cook)
        .through(serveThen(jiaoziToServe, store(box)))
        .compile
        .toList
      leftovers <- box.get
    } yield (bowl, leftovers)
  }

  override def run: IO[Unit] = simWithLeftovers(2, 4).flatMap(IO.println)
}
