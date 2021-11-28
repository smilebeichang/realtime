package cn.edu.sysu.gmall_logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author : song bei chang
 * @create 2021/11/26 15:48
 */
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String, String> kafka;

    // 提交路径
    @GetMapping("/applog")
    public String doLog(@RequestParam("param") String logJson) {

        //1. 日志落盘
        saveToDisk(logJson);

        //2. 写入kafka
        saveToKafka(logJson);
        return "ok";
    }

    private void saveToKafka(String logJson) {
        kafka.send("ods_log",logJson);
    }

    private void saveToDisk(String logJson) {
        log.info(logJson);
    }

}


