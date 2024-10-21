package scheduler.service;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;

public interface BaseJob extends Job {

    public void execute(JobExecutionContext context) throws JobExecutionException;
}
