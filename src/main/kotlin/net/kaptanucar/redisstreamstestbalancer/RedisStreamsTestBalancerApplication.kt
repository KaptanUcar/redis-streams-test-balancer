package net.kaptanucar.redisstreamstestbalancer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@EnableConfigurationProperties
@SpringBootApplication
class RedisStreamsTestBalancerApplication

fun main(args: Array<String>) {
    runApplication<RedisStreamsTestBalancerApplication>(*args)
}
