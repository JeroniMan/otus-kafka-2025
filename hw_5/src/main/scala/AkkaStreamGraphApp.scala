// src/main/scala/AkkaStreamGraphApp.scala

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, ZipWith}
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._

/**
 * Демонстрация графа обработки данных с Akka Streams
 *
 * Граф выполняет следующие операции:
 * 1. Входной поток чисел 1-5
 * 2. Broadcast на 3 параллельных потока
 * 3. Умножение на 10, 2 и 3 соответственно
 * 4. Zip трех потоков в кортеж
 * 5. Суммирование элементов кортежа
 */
object AkkaStreamGraphApp extends App {

  // Создание actor system и materializer
  implicit val system: ActorSystem = ActorSystem("AkkaStreamGraph")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  println("=" * 50)
  println("AKKA STREAMS GRAPH DSL DEMO")
  println("=" * 50)

  // Определение графа обработки данных
  val graph = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    // === ИСТОЧНИК ДАННЫХ ===
    val source = builder.add(Source(1 to 5))

    // === ОПЕРАЦИИ ПРЕОБРАЗОВАНИЯ ===
    val multiplyBy10 = builder.add(Flow[Int].map { x =>
      val result = x * 10
      println(s"Stream 1: $x * 10 = $result")
      result
    })

    val multiplyBy2 = builder.add(Flow[Int].map { x =>
      val result = x * 2
      println(s"Stream 2: $x * 2 = $result")
      result
    })

    val multiplyBy3 = builder.add(Flow[Int].map { x =>
      val result = x * 3
      println(s"Stream 3: $x * 3 = $result")
      result
    })

    // Суммирование трех значений
    val sumFlow = builder.add(Flow[(Int, Int, Int)].map { case (a, b, c) =>
      val sum = a + b + c
      println(s"Sum: ($a, $b, $c) = $sum")
      sum
    })

    // === BROADCAST И ZIP ===
    val broadcast = builder.add(Broadcast[Int](3))

    // Функция для объединения трех потоков в кортеж
    val zipFunction = (a: Int, b: Int, c: Int) => (a, b, c)

    val zip = builder.add(ZipWith[Int, Int, Int, (Int, Int, Int)](zipFunction))

    // === SINK ===
    val sink = builder.add(Sink.foreach[Int] { result =>
      println(s"Final result: $result")
    })

    // === СОЕДИНЕНИЕ КОМПОНЕНТОВ ГРАФА ===
    source ~> broadcast

    broadcast.out(0) ~> multiplyBy10 ~> zip.in0
    broadcast.out(1) ~> multiplyBy2  ~> zip.in1
    broadcast.out(2) ~> multiplyBy3  ~> zip.in2

    zip.out ~> sumFlow ~> sink

    ClosedShape
  }

  // Запуск графа
  println("\nStarting graph processing...")
  println("-" * 50)

  val runnableGraph = RunnableGraph.fromGraph(graph)
  val future = runnableGraph.run()

  // Ожидание завершения
  Thread.sleep(2000)

  println("-" * 50)
  println("\nExpected results: 15, 30, 45, 60, 75")
  println("Graph processing completed!")
  println("=" * 50)

  // Завершение actor system
  system.terminate()
  Await.result(system.whenTerminated, 10.seconds)
}