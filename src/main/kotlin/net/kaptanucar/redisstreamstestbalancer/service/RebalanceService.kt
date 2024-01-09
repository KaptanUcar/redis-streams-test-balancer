package net.kaptanucar.redisstreamstestbalancer.service

import net.kaptanucar.redisstreamstestbalancer.model.response.RebalanceReclaimResultsResponse
import net.kaptanucar.redisstreamstestbalancer.model.response.RebalanceResultResponse
import org.springframework.data.redis.connection.stream.PendingMessage
import org.springframework.data.redis.connection.stream.PendingMessages
import org.springframework.stereotype.Service

@Service
class RebalanceService(
        private val redisStreamService: RedisStreamService
) {

    fun balance(): RebalanceResultResponse {
        val activePods = redisStreamService.getActivePodNames()

        val registeredConsumers = redisStreamService.getAllConsumerNames()
        val activeConsumers = registeredConsumers.filterNot { isConsumerInactive(it, activePods) }
        val inactiveConsumers = registeredConsumers.filter { isConsumerInactive(it, activePods) }

        if (activeConsumers.isEmpty()) {
            return RebalanceResultResponse(
                    activeConsumers = activeConsumers,
                    inactiveConsumers = inactiveConsumers
            )
        }

        val pendingMessages = redisStreamService.getAllPendingMessages()

        val reclaimsFromInactiveConsumer = reclaimInactiveConsumerMessages(pendingMessages, inactiveConsumers, activeConsumers)
        redisStreamService.deleteConsumers(inactiveConsumers)

        val reclaimsFromActiveConsumer = reclaimOldPendingMessages(pendingMessages, activeConsumers)

        return RebalanceResultResponse(
                activeConsumers = activeConsumers,
                inactiveConsumers = inactiveConsumers,
                reclaimStats = RebalanceReclaimResultsResponse(
                        fromInactiveConsumers = reclaimsFromInactiveConsumer,
                        fromActiveConsumers = reclaimsFromActiveConsumer
                )
        )
    }

    private fun isConsumerInactive(consumerName: String, activePods: List<String>): Boolean {
        val podName = consumerName.substringBeforeLast('_')
        return !activePods.contains(podName)
    }

    private fun reclaimOldPendingMessages(
            pendingMessages: PendingMessages,
            activeConsumers: List<String>
    ): Int {
        val messages = pendingMessages
                .filter { activeConsumers.contains(it.consumerName) }
                .groupBy { it.consumerName }
                .filter { it.value.size > 1 }

        return messages.map { message ->
            val oldMessages = message.value
                    .sortedByDescending { it.idAsString }
                    .drop(1)

            reclaimPendingMessages(oldMessages, activeConsumers)
        }.sum()
    }

    private fun reclaimInactiveConsumerMessages(
            pendingMessages: PendingMessages,
            inactiveConsumers: List<String>,
            activeConsumers: List<String>
    ): Int {
        val messages = pendingMessages
                .filter { inactiveConsumers.contains(it.consumerName) }
                .toList()

        return reclaimPendingMessages(messages, activeConsumers)
    }

    private fun reclaimPendingMessages(
            pendingMessages: List<PendingMessage>,
            activeConsumers: List<String>
    ): Int {
        return pendingMessages.sumOf { reclaimPendingMessage(it, activeConsumers) }
    }

    private fun reclaimPendingMessage(pendingMessage: PendingMessage, activeConsumers: List<String>): Int {
        val newConsumerName = activeConsumers.random()
        val claimedRecords = redisStreamService.reclaimMessage(pendingMessage.id, newConsumerName)

        // Clone the record, requeue it and acknowledge the old one
        claimedRecords.forEach {
            redisStreamService.addMessage(it.value)
            redisStreamService.acknowledgeMessage(it)
        }

        return claimedRecords.size
    }
}