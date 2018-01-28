package cn.leapcloud.watchout

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import java.io.File
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue
import org.junit.Test as test

/**
 * Created by stream.
 */
class KafkaStreamTest {

  @test
  fun `we should receive multi records from some topic`() {
    val streamProps = Properties()
    streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-nginxLog-dsl15")
    streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // The commit interval for flushing records to state stores and downstream must be lower than.
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000)

    WatchNginxLog(WatchNginxLogConfig(streamProps, "nginxLog",
      listOf(
        TopicStoreAndWindowTime("nginxLog4xx", 500L),
        TopicStoreAndWindowTime("nginxLog5xx", 500L)
      ))).start()

    //producer
    val content = File("./src/test/resources/data").readLines()
    val producerProps = Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", StringSerializer::class.java.name)
    producerProps.put("value.serializer", StringSerializer::class.java.name)

    val producer = KafkaProducer<String, String>(producerProps)
    content.forEach {
      producer.send(ProducerRecord("nginxLog", it.hashCode().toString(), it)).get(1, TimeUnit.SECONDS)
    }

    //consumer
    val consumerProps = Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("group.id", "consumer-tutorial")
    consumerProps.put("auto.offset.reset", "earliest")
    consumerProps.put("key.deserializer", StringDeserializer::class.java.name)
    consumerProps.put("value.deserializer", LongDeserializer::class.java.name)
    val consumer = KafkaConsumer<String, Long>(consumerProps)
    var asserted = false
    consumer.subscribe(listOf("nginxLog4xxCount", "nginxLog2xxCount"))
//  val earliest = consumer.beginningOffsets(setOf(TopicPartition("nginxLog4xxCount", 0)))
//  consumer.assign(earliest.keys)
//  consumer.seekToBeginning(earliest.keys)

    Thread({
      val serverNames = listOf("api.maxleap.cn", "wonapi.maxleap.cn")
      try {
        while (true) {
          val records = consumer.poll(java.lang.Long.MAX_VALUE)
          for (record in records) {
            println(record)
            assertTrue { serverNames.contains(record.key()) }
            assertTrue { record.value() > 0 }
            asserted = true
          }
        }
      } catch (e: WakeupException) {

      } finally {
        consumer.close()
      }
    }).start()
    CountDownLatch(1).await(15, TimeUnit.SECONDS)
    assertTrue { asserted }
  }

}




