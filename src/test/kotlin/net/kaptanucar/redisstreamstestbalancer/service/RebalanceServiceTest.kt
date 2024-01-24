package net.kaptanucar.redisstreamstestbalancer.service

import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verify
import net.kaptanucar.redisstreamstestbalancer.model.response.RebalanceReclaimResultsResponse
import net.kaptanucar.redisstreamstestbalancer.model.response.RebalanceResultResponse
import net.kaptanucar.redisstreamstestbalancer.testextension.createForTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.PendingMessage
import org.springframework.data.redis.connection.stream.PendingMessages

@ExtendWith(MockKExtension::class)
class RebalanceServiceTest {

    @InjectMockKs
    private lateinit var underTest: RebalanceService

    @MockK(relaxed = true)
    private lateinit var redisStreamService: RedisStreamService

    @Test
    fun `it should do nothing when there is no active pods available`() {
        // given
        every { redisStreamService.getActivePodNames() } returns emptyList()
        every {
            redisStreamService.getAllConsumerNames()
        } returns listOf("inactive-1_0", "inactive-1_1", "inactive-2_0")

        // when
        val actual = underTest.balance()

        // then
        verify(exactly = 0) { redisStreamService.getAllPendingMessages() }

        val expected = RebalanceResultResponse(
                activeConsumers = emptyList(),
                inactiveConsumers = listOf("inactive-1_0", "inactive-1_1", "inactive-2_0")
        )
        assertThat(actual).isEqualTo(expected)
    }

    @Test
    fun `it should reclaim inactive consumer messages`() {
        // given
        every {
            redisStreamService.getActivePodNames()
        } returns listOf("active-1")

        every {
            redisStreamService.getAllConsumerNames()
        } returns listOf("active-1_0", "active-1_1", "inactive-2_0", "inactive-3_0")

        val pendingMessage1 = PendingMessage::class.createForTest(0, "active-1_0")
        val pendingMessage2 = PendingMessage::class.createForTest(1, "inactive-2_0")

        val pendingMessages = PendingMessages(
                "group-name",
                listOf(pendingMessage1, pendingMessage2)
        )
        every { redisStreamService.getAllPendingMessages() } returns pendingMessages

        val pendingMessage2Record = mockk<MapRecord<String, Any, Any>>()
        every { pendingMessage2Record.value } returns mapOf("foo" to "bar")
        every { redisStreamService.reclaimMessage(pendingMessage2.id, any()) } returns listOf(pendingMessage2Record)

        // when
        val actual = underTest.balance()

        // then
        verify { redisStreamService.addMessage(mapOf("foo" to "bar")) }
        verify { redisStreamService.acknowledgeMessage(pendingMessage2Record) }

        verify { redisStreamService.deleteConsumers(listOf("inactive-2_0", "inactive-3_0")) }

        val expected = RebalanceResultResponse(
                activeConsumers = listOf("active-1_0", "active-1_1"),
                inactiveConsumers = listOf("inactive-2_0", "inactive-3_0"),
                reclaimStats = RebalanceReclaimResultsResponse(
                        fromInactiveConsumers = 1,
                        fromActiveConsumers = 0
                )
        )
        assertThat(actual).isEqualTo(expected)
    }

    @Test
    fun `it should reclaim old pending consumer messages`() {
        // given
        every {
            redisStreamService.getActivePodNames()
        } returns listOf("active-1")

        every {
            redisStreamService.getAllConsumerNames()
        } returns listOf("active-1_0", "active-1_1")

        val pendingMessage1 = PendingMessage::class.createForTest(0, "active-1_0")
        val pendingMessage2 = PendingMessage::class.createForTest(1, "active-1_1")
        val pendingMessage3 = PendingMessage::class.createForTest(2, "active-1_1")

        val pendingMessages = PendingMessages(
                "group-name",
                listOf(pendingMessage1, pendingMessage2, pendingMessage3)
        )
        every { redisStreamService.getAllPendingMessages() } returns pendingMessages

        val pendingMessage2Record = mockk<MapRecord<String, Any, Any>>()
        every { pendingMessage2Record.value } returns mapOf("foo" to "bar")
        every { redisStreamService.reclaimMessage(pendingMessage2.id, any()) } returns listOf(pendingMessage2Record)

        // when
        val actual = underTest.balance()

        // then
        verify { redisStreamService.addMessage(mapOf("foo" to "bar")) }
        verify { redisStreamService.acknowledgeMessage(pendingMessage2Record) }

        verify { redisStreamService.deleteConsumers(emptyList()) }

        val expected = RebalanceResultResponse(
                activeConsumers = listOf("active-1_0", "active-1_1"),
                inactiveConsumers = emptyList(),
                reclaimStats = RebalanceReclaimResultsResponse(
                        fromInactiveConsumers = 0,
                        fromActiveConsumers = 1
                )
        )
        assertThat(actual).isEqualTo(expected)
    }
}