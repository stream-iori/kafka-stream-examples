package cn.leapcloud.watchout

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.Predicate
import org.apache.kafka.streams.kstream.TimeWindows
import java.util.*


/**
 * Created by stream.
 */

interface StreamController {
  /**
   * start KafkaStream
   */
  fun start()

  /**
   * stop KafkaStream
   */
  fun stop()
}

data class TopicStoreAndWindowTime(val topic: String, val time: Long)
data class WatchNginxLogConfig(val props: Properties, val source: String, val topicAndWindows: List<TopicStoreAndWindowTime>)

class WatchNginxLog(private val config: WatchNginxLogConfig) : StreamController {
  private val builder = KStreamBuilder()
  private val jsonSerde = Serdes.serdeFrom(JsonSerializer(), JsonDeserializer())
  private lateinit var kafkaStream: KafkaStreams

  override fun start() {
    val streams = builder.stream<String, JsonNode>(Serdes.String(), jsonSerde, config.source)
      .branch(
        Predicate { _, value -> value.get("status").asInt() in 400..499 },
        Predicate { _, value -> value.get("status").asInt() in 500..599 })

    val aggregateByServerName = fun(stream: KStream<String, JsonNode>, timeWindows: TimeWindows, localStoreName: String) =
      stream.groupBy({ _, value -> value.get("server_name").asText() }, Serdes.String(), jsonSerde)
        .aggregate({ 0L }, { _, _, aggregate -> aggregate + 1 }, timeWindows, Serdes.Long(), localStoreName)
        .toStream()
        .map { key, value -> KeyValue(key.key(), value) }

    assert(streams.size == config.topicAndWindows.size)
    for (index in streams.indices) {
      val time = config.topicAndWindows[index].time
      val topic = config.topicAndWindows[index].topic
      aggregateByServerName.invoke(streams[index], TimeWindows.of(time).advanceBy(time), "${topic}LocalStore").to(Serdes.String(), Serdes.Long(), "${topic}Count")
    }

    kafkaStream = KafkaStreams(builder, config.props)
    kafkaStream.start()
  }

  override fun stop() {
    kafkaStream.close()
  }
}

class WatchBizLog() : StreamController {

  private val builder = KStreamBuilder()
  private val jsonSerde = Serdes.serdeFrom(JsonSerializer(), JsonDeserializer())
  private lateinit var kafkaStream: KafkaStreams

  override fun stop() {
    kafkaStream.close()
  }

  override fun start() {
    val streams = builder.stream<String, JsonNode>(Serdes.String(), jsonSerde, "topic")
  }

}

fun main(args: Array<String>) {

}