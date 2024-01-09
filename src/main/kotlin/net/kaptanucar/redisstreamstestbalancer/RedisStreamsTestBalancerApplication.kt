package net.kaptanucar.redisstreamstestbalancer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RedisStreamsTestBalancerApplication

fun main(args: Array<String>) {
	runApplication<RedisStreamsTestBalancerApplication>(*args)
}
