package net.kaptanucar.redisstreamstestbalancer.testextension

import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.PendingMessage
import org.springframework.data.redis.connection.stream.RecordId
import java.time.Duration
import kotlin.reflect.KClass

fun KClass<PendingMessage>.createForTest(index: Long, consumerName: String) = PendingMessage(
        RecordId.of("1706040577668-$index"),
        Consumer.from("group-name", consumerName),
        Duration.ZERO,
        1
)