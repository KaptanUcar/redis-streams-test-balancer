package net.kaptanucar.redisstreamstestbalancer.service

import io.lettuce.core.RedisBusyException
import io.lettuce.core.RedisReadOnlyException
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import net.kaptanucar.redisstreamstestbalancer.config.RedisStreamProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.data.redis.RedisSystemException
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.StreamOperations
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException

@ExtendWith(MockKExtension::class)
class TroubleshootServiceTest {

    @InjectMockKs
    private lateinit var underTest: TroubleshootService

    @MockK
    private lateinit var redisTemplate: RedisTemplate<String, String>

    @MockK
    private lateinit var redisStreamProperties: RedisStreamProperties

    @MockK
    private lateinit var streamOperations: StreamOperations<String, Any, Any>

    @Nested
    internal inner class InitializeStream {

        @Test
        fun `it should initialize stream`() {
            // given
            every { redisStreamProperties.key } returns "stream-key"
            every { redisStreamProperties.group } returns "stream-group"

            every { redisTemplate.opsForStream<Any, Any>() } returns streamOperations
            every { streamOperations.createGroup("stream-key", "stream-group") } returns "OK"

            // when
            val actual = underTest.initializeStream()

            // then
            assertThat(actual).isEqualTo("OK")
        }

        @Test
        fun `it should throw exception when consumer group already exists`() {
            // given
            every { redisStreamProperties.key } returns "stream-key"
            every { redisStreamProperties.group } returns "stream-group"

            every { redisTemplate.opsForStream<Any, Any>() } returns streamOperations

            every {
                streamOperations.createGroup("stream-key", "stream-group")
            } throws RedisSystemException("", RedisBusyException("-BUSYGROUP"))

            // when
            val executable: () -> Unit = { underTest.initializeStream() }

            // then
            val exception = assertThrows<ResponseStatusException>(executable)

            assertThat(exception.statusCode).isEqualTo(HttpStatus.NO_CONTENT)
            assertThat(exception.reason).isEqualTo("Already exists")
        }

        @Test
        fun `it should throw exception when unknown error occurred`() {
            // given
            every { redisStreamProperties.key } returns "stream-key"
            every { redisStreamProperties.group } returns "stream-group"

            every { redisTemplate.opsForStream<Any, Any>() } returns streamOperations

            every {
                streamOperations.createGroup("stream-key", "stream-group")
            } throws RedisSystemException("", RedisReadOnlyException(""))

            // when
            val executable: () -> Unit = { underTest.initializeStream() }

            // then
            val exception = assertThrows<ResponseStatusException>(executable)

            assertThat(exception.statusCode).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            assertThat(exception.reason).isEqualTo("Unknown error")
        }
    }
}