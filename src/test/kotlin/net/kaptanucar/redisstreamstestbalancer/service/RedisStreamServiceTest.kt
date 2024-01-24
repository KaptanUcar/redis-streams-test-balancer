package net.kaptanucar.redisstreamstestbalancer.service

import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.slot
import net.kaptanucar.redisstreamstestbalancer.config.RedisStreamProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.PendingMessages
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumer
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumers
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.StreamOperations
import java.time.Duration
import java.util.stream.Stream

@ExtendWith(MockKExtension::class)
class RedisStreamServiceTest {

    @InjectMockKs
    private lateinit var underTest: RedisStreamService

    @MockK
    private lateinit var redisTemplate: RedisTemplate<String, String>

    @MockK
    private lateinit var redisStreamProperties: RedisStreamProperties

    @MockK
    private lateinit var streamOperations: StreamOperations<String, Any, Any>

    @Nested
    internal inner class GetAllConsumerNames {

        @MockK
        private lateinit var consumers: XInfoConsumers

        @MockK
        private lateinit var consumer1: XInfoConsumer

        @MockK
        private lateinit var consumer2: XInfoConsumer

        @BeforeEach
        fun setUp() {
            every { consumers.stream() } returns Stream.of(consumer1, consumer2)
            every { consumers.map<String>(any()) } answers { callOriginal() }

            every { consumer1.consumerName() } returns "consumer-1"
            every { consumer2.consumerName() } returns "consumer-2"
        }

        @Test
        fun `it should get all consumer names`() {
            // given
            every { redisStreamProperties.key } returns "stream-key"
            every { redisStreamProperties.group } returns "stream-group"

            every { redisTemplate.opsForStream<Any, Any>() } returns streamOperations

            every { streamOperations.consumers("stream-key", "stream-group") } returns consumers

            // when
            val actual = underTest.getAllConsumerNames()

            // then
            assertThat(actual).containsExactlyInAnyOrder("consumer-1", "consumer-2")
        }
    }

    @Nested
    internal inner class GetActivePodNames {

        @Test
        fun `it should get active pod names`() {
            // given
            every { redisStreamProperties.podInfoKeyPrefix } returns "key-prefix"

            every {
                redisTemplate.keys("key-prefix:*")
            } returns setOf("key-prefix:active-1", "key-prefix:active-2")

            // when
            val actual = underTest.getActivePodNames()

            // then
            assertThat(actual).containsExactlyInAnyOrder("active-1", "active-2")
        }
    }

    @Nested
    internal inner class GetAllPendingMessages {

        @MockK
        private lateinit var pendingMessages: PendingMessages

        @Test
        fun `it should get all pending messages`() {
            // given
            every { redisStreamProperties.key } returns "stream-key"
            every { redisStreamProperties.group } returns "stream-group"

            every { redisTemplate.opsForStream<Any, Any>() } returns streamOperations

            every {
                streamOperations.pending(
                        "stream-key",
                        "stream-group",
                        Range.unbounded<Any>(),
                        10000)
            } returns pendingMessages

            // when
            val actual = underTest.getAllPendingMessages()

            // then
            assertThat(actual).isEqualTo(pendingMessages)
        }
    }

    @Nested
    internal inner class DeleteConsumers {

        private val capturedConsumerList = mutableListOf<Consumer>()

        @Test
        fun `it should delete consumers`() {
            // given
            every { redisStreamProperties.key } returns "stream-key"
            every { redisStreamProperties.group } returns "stream-group"

            every { redisTemplate.opsForStream<Any, Any>() } returns streamOperations

            every {
                streamOperations.deleteConsumer(eq("stream-key"), capture(capturedConsumerList))
            } returns true

            // when
            underTest.deleteConsumers(listOf("consumer-1", "consumer-2"))

            // then
            assertThat(capturedConsumerList)
                    .map<String> { it.name }
                    .containsExactlyInAnyOrder("consumer-1", "consumer-2")
        }
    }

    @Nested
    internal inner class ReclaimMessage {

        @MockK
        private lateinit var mapRecord: MapRecord<String, Any, Any>

        @Test
        fun `it should claim message`() {
            // given
            val recordId = RecordId.of("pending-0")

            every { redisStreamProperties.key } returns "stream-key"
            every { redisStreamProperties.group } returns "stream-group"

            every { redisTemplate.opsForStream<Any, Any>() } returns streamOperations

            every {
                streamOperations.claim(
                        "stream-key",
                        "stream-group",
                        "new-consumer",
                        Duration.ZERO,
                        recordId
                )
            } returns listOf(mapRecord)

            // when
            val actual = underTest.reclaimMessage(recordId, "new-consumer")

            // then
            assertThat(actual).containsExactlyInAnyOrder(mapRecord)
        }
    }

    @Nested
    internal inner class AddMessage {

        private val mapRecordSlot = slot<MapRecord<String, Any, Any>>()

        @Test
        fun `it should add message`() {
            // given
            every { redisStreamProperties.key } returns "stream-key"

            every { redisTemplate.opsForStream<Any, Any>() } returns streamOperations

            every { streamOperations.add(capture(mapRecordSlot)) } returns RecordId.of("pending-0")

            // when
            val actual = underTest.addMessage(mapOf("foo" to "bar"))

            // then
            assertThat(mapRecordSlot.captured.stream).isEqualTo("stream-key")
            assertThat(mapRecordSlot.captured.value).containsExactlyInAnyOrderEntriesOf(mapOf("foo" to "bar"))

            assertThat(actual!!.value).isEqualTo("pending-0")
        }
    }

    @Nested
    internal inner class AcknowledgeMessage {

        @MockK
        private lateinit var mapRecord: MapRecord<String, Any, Any>

        @Test
        fun `it should acknowledge message`() {
            // given
            every { redisStreamProperties.group } returns "stream-group"

            every { redisTemplate.opsForStream<Any, Any>() } returns streamOperations

            every { streamOperations.acknowledge("stream-group", mapRecord) } returns 1L

            // when
            val actual = underTest.acknowledgeMessage(mapRecord)

            // then
            assertThat(actual).isEqualTo(1)
        }
    }
}