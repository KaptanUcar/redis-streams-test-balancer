package net.kaptanucar.redisstreamstestbalancer.controller

import net.kaptanucar.redisstreamstestbalancer.model.response.RebalanceResultResponse
import net.kaptanucar.redisstreamstestbalancer.service.RebalanceService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/rebalance")
class RebalanceController(private val rebalanceService: RebalanceService) {

    @GetMapping
    fun trigger(): RebalanceResultResponse = rebalanceService.balance()
}