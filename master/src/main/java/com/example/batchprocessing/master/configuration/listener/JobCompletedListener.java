package com.example.batchprocessing.master.configuration.listener;

import com.example.batchprocessing.master.configuration.structure.YearReport;
import org.springframework.boot.autoconfigure.batch.JobExecutionEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration(proxyBeanMethods = false)
public class JobCompletedListener {

    // todo: clear this out after the job is done. some sort of listener?
    public static final Map<Integer, YearReport> reportMap = new ConcurrentHashMap<Integer, YearReport>();

    @EventListener
    public void batchJobCompleted(JobExecutionEvent event) {
        var running = Map.of(//
                "running", event.getJobExecution().getStatus().isRunning(),//
                "finished", event.getJobExecution().getExitStatus().getExitCode() //
        );//
        System.out.println(MessageFormat.format("jobExecutionEvent: [{0}]", running));
        this.reportMap.clear();
    }

}
