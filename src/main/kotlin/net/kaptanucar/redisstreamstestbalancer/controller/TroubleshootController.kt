package net.kaptanucar.redisstreamstestbalancer.controller

import net.kaptanucar.redisstreamstestbalancer.service.TroubleshootService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/troubleshoot")
class TroubleshootController(private val troubleshootService: TroubleshootService) {

    @PostMapping("/initialize-stream")
    @ResponseStatus(HttpStatus.CREATED)
    fun initializeStream() = troubleshootService.initializeStream()
}