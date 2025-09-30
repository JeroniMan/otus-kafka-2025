// src/main/scala/OutputTopicReader.scala

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer
import akka.kafka.Subscriptions
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.IntegerDeserializer
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

/**
 * Утилита для чтения результатов из output топика
 */
object OutputTopicReader extends App {

  implicit val system: ActorSystem = ActorSystem("OutputReader")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  val bootstrapServers = "localhost:9092"
  val outputTopic = "output-results"

  println("=" * 60)
  println("READING RESULTS FROM OUTPUT TOPIC")
  println("=" * 60)
  println(s"Topic: $outputTopic")
  println("Press Ctrl+C to stop")
  println("-" * 60)

  val consumerSettings = ConsumerSettings(system, new IntegerDeserializer, new IntegerDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("output-reader-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  Consumer
    .plainSource(consumerSettings, Subscriptions.topics(outputTopic))
    .map { record =>
      val key = Option(record.key()).map(_.toString).getOrElse("null")
      val value = record.value()
      val partition = record.partition()
      val offset = record.offset()

      println(f"[RESULT] Value: $value%3d | Key: $key%4s | Partition: $partition | Offset: $offset")
      value
    }
    .runForeach { value =>
      // Можно добавить дополнительную обработку
      if (Set(15, 30, 45, 60, 75).contains(value)) {
        println(s"  ✓ Expected result: $value")
      }
    }
    .onComplete { _ =>
      system.terminate()
    }
}