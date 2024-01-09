package net.kaptanucar.redisstreamstestbalancer.model.response

data class RebalanceResultResponse(
        val activeConsumers: List<String> = emptyList(),
        val inactiveConsumers: List<String> = emptyList(),
        val reclaimStats: RebalanceReclaimResultsResponse = RebalanceReclaimResultsResponse()
)

data class RebalanceReclaimResultsResponse(
        val fromInactiveConsumers: Int = 0,
        val fromActiveConsumers: Int = 0
)