// src/main/scala/AkkaKafkaStreamApp.scala

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, ZipWith}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._

/**
 * Интеграция Akka Streams с Apache Kafka
 *
 * Архитектура:
 * 1. Producer отправляет числа 1-5 в Kafka топик
 * 2. Consumer читает из топика
 * 3. Применяется граф обработки (broadcast -> multiply -> zip -> sum)
 * 4. Результаты отправляются в output топик
 */
object AkkaKafkaStreamApp extends App {

  implicit val system: ActorSystem = ActorSystem("AkkaKafkaStream")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  // Конфигурация Kafka
  val bootstrapServers = "localhost:9092"
  val inputTopic = "input-numbers"
  val outputTopic = "output-results"
  val groupId = "akka-streams-group"

  println("=" * 60)
  println("AKKA STREAMS + KAFKA GRAPH DSL")
  println("=" * 60)
  println(s"Bootstrap servers: $bootstrapServers")
  println(s"Input topic: $inputTopic")
  println(s"Output topic: $outputTopic")
  println(s"Consumer group: $groupId")
  println("-" * 60)

  // Настройки Producer - используем StringSerializer
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  // Настройки Consumer - используем StringDeserializer
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId(groupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

  // Подписка на топик
  val subscription = Subscriptions.topics(inputTopic)

  // Шаг 1: Отправка данных в Kafka
  println("\n[PRODUCER] Sending numbers to Kafka...")
  val producerGraph = Source(1 to 5)
    .map { number =>
      println(s"  → Sending: $number")
      // Отправляем как String
      new ProducerRecord[String, String](inputTopic, number.toString, number.toString)
    }
    .runWith(Producer.plainSink(producerSettings))

  // Ждем пока producer отправит все сообщения
  Thread.sleep(2000)

  // Шаг 2: Граф обработки данных из Kafka
  println("\n[CONSUMER] Starting graph processing...")
  println("-" * 60)

  val processingGraph = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    // === ИСТОЧНИК - Kafka Consumer ===
    val kafkaSource = builder.add(
      Consumer.plainSource(consumerSettings, subscription)
        .map(record => record.value().toInt)  // Преобразуем String в Int для обработки
        .take(5) // Берем только 5 сообщений для демо
    )

    // === ОБРАБОТКА ===
    val multiplyBy10 = builder.add(Flow[Int].map { x =>
      val result = x * 10
      println(s"  Stream 1: $x × 10 = $result")
      result
    })

    val multiplyBy2 = builder.add(Flow[Int].map { x =>
      val result = x * 2
      println(s"  Stream 2: $x × 2 = $result")
      result
    })

    val multiplyBy3 = builder.add(Flow[Int].map { x =>
      val result = x * 3
      println(s"  Stream 3: $x × 3 = $result")
      result
    })

    val sumFlow = builder.add(Flow[(Int, Int, Int)].map { case (a, b, c) =>
      val sum = a + b + c
      println(s"  Zip & Sum: ($a, $b, $c) = $sum")
      sum
    })

    // === BROADCAST И ZIP ===
    val broadcast = builder.add(Broadcast[Int](3))
    val zip = builder.add(ZipWith[Int, Int, Int, (Int, Int, Int)]((a, b, c) => (a, b, c)))

    // === РЕЗУЛЬТАТ - отправляем обратно в Kafka ===
    val kafkaSink = builder.add(
      Flow[Int]
        .map { result =>
          println(s"  ★ RESULT: $result → Sending to output topic")
          // Отправляем результат как String
          new ProducerRecord[String, String](outputTopic, result.toString, result.toString)
        }
        .to(Producer.plainSink(producerSettings))
    )

    // === СОЕДИНЕНИЕ КОМПОНЕНТОВ ===
    kafkaSource ~> broadcast

    broadcast.out(0) ~> multiplyBy10 ~> zip.in0
    broadcast.out(1) ~> multiplyBy2  ~> zip.in1
    broadcast.out(2) ~> multiplyBy3  ~> zip.in2

    zip.out ~> sumFlow ~> kafkaSink

    ClosedShape
  }

  val future = RunnableGraph.fromGraph(processingGraph).run()

  // Ожидание обработки
  Thread.sleep(5000)

  println("-" * 60)
  println("\n[SUMMARY]")
  println("Input:    1,  2,  3,  4,  5")
  println("Expected: 15, 30, 45, 60, 75")
  println("\n✓ Graph processing completed!")
  println("\nCheck results in:")
  println("  • Kafka UI: http://localhost:9000")
  println("  • Topic: output-results")
  println("=" * 60)

  // Корректное завершение
  system.terminate()
  Await.result(system.whenTerminated, 10.seconds)
}