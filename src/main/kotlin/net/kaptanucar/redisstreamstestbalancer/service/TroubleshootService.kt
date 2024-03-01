package net.kaptanucar.redisstreamstestbalancer.service

import io.lettuce.core.RedisBusyException
import net.kaptanucar.redisstreamstestbalancer.config.RedisStreamProperties
import org.slf4j.LoggerFactory
import org.springframework.data.redis.RedisSystemException
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException

@Service
class TroubleshootService(
        private val redisTemplate: RedisTemplate<String, String>,
        private val redisStreamProperties: RedisStreamProperties
) {

    companion object {
        private val ALREADY_EXISTS = ResponseStatusException(HttpStatus.NO_CONTENT, "Already exists")
        private val UNKNOWN_ERROR = ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Unknown error")
    }

    private val logger = LoggerFactory.getLogger(TroubleshootService::class.java)
    private val streamOps get() = redisTemplate.opsForStream<Any, Any>()

    fun initializeStream(): String =
            try {
                streamOps.createGroup(redisStreamProperties.key, redisStreamProperties.group)
            } catch (ex: RedisSystemException) {
                logger.info("An error occurred while creating consumer group! ", ex)

                when (ex.rootCause is RedisBusyException) {
                    true -> throw ALREADY_EXISTS
                    else -> throw UNKNOWN_ERROR
                }
            }
}