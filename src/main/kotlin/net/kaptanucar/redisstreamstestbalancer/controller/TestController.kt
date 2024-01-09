package net.kaptanucar.redisstreamstestbalancer.controller

import net.kaptanucar.redisstreamstestbalancer.config.RedisStreamProperties
import net.kaptanucar.redisstreamstestbalancer.model.DummyEvent
import org.apache.commons.lang3.RandomStringUtils
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/test")
class TestController(
        val redisTemplate: RedisTemplate<String, String>,
        val redisStreamProperties: RedisStreamProperties
) {

    @GetMapping
    fun publish(): RecordId? {
        val message = DummyEvent(message = RandomStringUtils.randomAlphanumeric(10))

        return redisTemplate.opsForStream<Any, Any>()
                .add(ObjectRecord.create(redisStreamProperties.key, message))
    }
}