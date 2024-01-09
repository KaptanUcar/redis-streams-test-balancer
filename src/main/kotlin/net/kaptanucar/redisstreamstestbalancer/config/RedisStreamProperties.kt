package net.kaptanucar.redisstreamstestbalancer.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("redis-stream")
data class RedisStreamProperties(
        var key: String = "",
        var group: String = "",
        var podInfoKeyPrefix: String = "",
)