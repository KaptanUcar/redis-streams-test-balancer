package net.kaptanucar.redisstreamstestbalancer.service

import net.kaptanucar.redisstreamstestbalancer.config.RedisStreamProperties
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.PendingMessages
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class RedisStreamService(
        private val redisTemplate: RedisTemplate<String, String>,
        private val redisStreamProperties: RedisStreamProperties
) {

    companion object {
        const val MAX_PENDING_MESSAGE_COUNT = 10000L
    }

    private val streamOps get() = redisTemplate.opsForStream<Any, Any>()

    fun getAllConsumerNames(): List<String> = streamOps
            .consumers(redisStreamProperties.key, redisStreamProperties.group)
            .map { it.consumerName() }
            .toList()

    fun getActivePodNames(): List<String> = redisTemplate
            .keys("${redisStreamProperties.podInfoKeyPrefix}:*")
            .map { it.substringAfterLast(':') }

    fun getAllPendingMessages(): PendingMessages = streamOps
            .pending(redisStreamProperties.key, redisStreamProperties.group, Range.unbounded<Any>(), MAX_PENDING_MESSAGE_COUNT)

    fun deleteConsumers(consumerNames: List<String>) = consumerNames
            .map { Consumer.from(redisStreamProperties.group, it) }
            .forEach {
                redisTemplate.opsForStream<Any, Any>()
                        .deleteConsumer(redisStreamProperties.key, it)
            }

    fun reclaimMessage(
            recordId: RecordId,
            newConsumerName: String
    ): List<MapRecord<String, Any, Any>> = streamOps
            .claim(
                    redisStreamProperties.key,
                    redisStreamProperties.group,
                    newConsumerName,
                    Duration.ZERO,
                    recordId
            )

    fun addMessage(map: Map<Any, Any>) = streamOps
            .add(MapRecord.create(redisStreamProperties.key, map))

    fun acknowledgeMessage(record: MapRecord<String, Any, Any>) = streamOps
            .acknowledge(redisStreamProperties.group, record)
}