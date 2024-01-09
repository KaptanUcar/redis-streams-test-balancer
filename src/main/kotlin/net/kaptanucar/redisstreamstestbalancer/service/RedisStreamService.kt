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

    fun getAllConsumerNames(): List<String> = redisTemplate
            .opsForStream<Any, Any>()
            .consumers(redisStreamProperties.key, redisStreamProperties.group)
            .map { it.consumerName() }
            .toList()

    fun getActivePodNames(): List<String> = redisTemplate
            .keys("${redisStreamProperties.podInfoKeyPrefix}:*")
            .map { it.substringAfterLast(':') }

    fun getAllPendingMessages(): PendingMessages = redisTemplate
            .opsForStream<Any, Any>()
            .pending(redisStreamProperties.key, redisStreamProperties.group, Range.unbounded<Any>(), 10000)

    fun deleteConsumers(consumerNames: List<String>) = consumerNames
            .map { Consumer.from(redisStreamProperties.group, it) }
            .forEach {
                redisTemplate.opsForStream<Any, Any>()
                        .deleteConsumer(redisStreamProperties.key, it)
            }

    fun reclaimMessage(
            recordId: RecordId,
            newConsumerName: String
    ): List<MapRecord<String, Any, Any>> = redisTemplate
            .opsForStream<Any, Any>()
            .claim(
                    redisStreamProperties.key,
                    redisStreamProperties.group,
                    newConsumerName,
                    Duration.ZERO,
                    recordId
            )

    fun addMessage(map: Map<Any, Any>) = redisTemplate
            .opsForStream<Any, Any>()
            .add(MapRecord.create(redisStreamProperties.key, map))

    fun acknowledgeMessage(record: MapRecord<String, Any, Any>) = redisTemplate
            .opsForStream<Any, Any>()
            .acknowledge(redisStreamProperties.group, record)
}