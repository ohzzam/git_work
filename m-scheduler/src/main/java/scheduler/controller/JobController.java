package scheduler.controller;

import scheduler.model.JobAndTrigger;
import scheduler.model.JobInfo;
import scheduler.model.JobStatusVo;
import scheduler.service.BaseJob;
import scheduler.service.IJobAndTriggerService;
import scheduler.tool.DateUnit;
import scheduler.tool.SpringUtil;
import com.github.pagehelper.PageInfo;
import com.ndata.module.DateUtils;
import com.ndata.module.StringUtils;

import lombok.extern.slf4j.Slf4j;

import org.mybatis.spring.SqlSessionTemplate;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.quartz.DateBuilder.futureDate;

@Slf4j
@RestController
@RequestMapping(value = "job")
public class JobController {

	@Autowired
	private SqlSessionTemplate sqlSessionTemplate;	// transaction 사용 안할 경우 사용
	
    @Autowired
    private IJobAndTriggerService iJobAndTriggerService;

    @Autowired
    @Qualifier("Scheduler")
    private Scheduler scheduler;
    @Autowired
    private DateUnit dateUnit;


    @PostMapping(value = "/addjob")
    public void addjob(JobInfo jobInfo) throws Exception {
        if ("".equals(jobInfo.getJobClassName()) || "".equals(jobInfo.getCronExpression())) {
            return;
        }
        //jobGroupName 이 empty 인 경우에 UUID로 할당한다.
        if (StringUtils.isEmpty(jobInfo.getJobGroupName())) {
        	jobInfo.setJobGroupName(UUID.randomUUID().toString());
        }
        if (jobInfo.getTimeType() == null) {
            addCronJob(jobInfo);
            return;
        }
        addSimpleJob(jobInfo);
    }

    //CronTrigger
    public void addCronJob(JobInfo jobInfo) throws Exception {

        scheduler.start();
        log.info(">> JobInfo : {}", jobInfo.getJobClassName());

        JobDetail jobDetail = JobBuilder.newJob(getClass(jobInfo.getJobClassName()).getClass()).
                withIdentity(jobInfo.getJobClassName(), jobInfo.getJobGroupName())
                .build();

        CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(jobInfo.getCronExpression());
        CronTrigger trigger = TriggerBuilder.newTrigger().
                withIdentity(jobInfo.getJobClassName(), jobInfo.getJobGroupName())
                .withSchedule(scheduleBuilder)
                .build();

        try {
            scheduler.scheduleJob(jobDetail, trigger);
            
        } catch (SchedulerException e) {
            log.error("failed to create job" + e);
            throw new Exception("failed to create job");
        }
    }

    //Simple Trigger
    public void addSimpleJob(JobInfo jobInfo) throws Exception {

        scheduler.start();

        JobDetail jobDetail = JobBuilder.newJob(getClass(jobInfo.getJobClassName()).getClass())
                .withIdentity(jobInfo.getJobClassName(), jobInfo.getJobGroupName())
                .build();

        DateBuilder.IntervalUnit verDate = dateUnit.verification(jobInfo.getTimeType());
        SimpleTrigger simpleTrigger = (SimpleTrigger) TriggerBuilder.newTrigger()
                .withIdentity(jobInfo.getJobClassName(), jobInfo.getJobGroupName())
                .startAt(futureDate(Integer.parseInt(jobInfo.getCronExpression()), verDate))
                .forJob(jobInfo.getJobClassName(), jobInfo.getJobGroupName())
                .build();

        try {
            scheduler.scheduleJob(jobDetail, simpleTrigger);
            
        } catch (SchedulerException e) {
        	log.error("failed to create job" + e);
            throw new Exception("failed to create job");
        }
    }

    @PostMapping(value = "/pausejob")
    public void pausejob(@RequestParam(value = "jobClassName") String jobClassName, @RequestParam(value = "jobGroupName") String jobGroupName) throws Exception {
        jobPause(jobClassName, jobGroupName);
    }

    public void jobPause(String jobClassName, String jobGroupName) throws Exception {
        scheduler.pauseJob(JobKey.jobKey(jobClassName, jobGroupName));
    }

    @PostMapping(value = "/resumejob")
    public void resumejob(@RequestParam(value = "jobClassName") String jobClassName, @RequestParam(value = "jobGroupName") String jobGroupName) throws Exception {
        jobresume(jobClassName, jobGroupName);
    }

    public void jobresume(String jobClassName, String jobGroupName) throws Exception {
        scheduler.resumeJob(JobKey.jobKey(jobClassName, jobGroupName));
    }

    @PostMapping(value = "/reschedulejob")
    public void rescheduleJob(@RequestParam(value = "jobClassName") String jobClassName,
                              @RequestParam(value = "jobGroupName") String jobGroupName,
                              @RequestParam(value = "cronExpression") String cronExpression,
                              @RequestParam(value = "lastSuccessTime") String lastSuccessTime) throws Exception {
        jobreschedule(jobClassName, jobGroupName, cronExpression, lastSuccessTime);
    }

    public void jobreschedule(String jobClassName, String jobGroupName, String cronExpression, String lastSuccessTime) throws Exception {
        try {
            TriggerKey triggerKey = TriggerKey.triggerKey(jobClassName, jobGroupName);
            CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);

            CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);

            trigger = trigger.getTriggerBuilder().withIdentity(triggerKey).withSchedule(scheduleBuilder).build();

            scheduler.rescheduleJob(triggerKey, trigger);
            
            //스케쥴 최종 성공시간이 NULL이 아닌 경우에는 업데이트한다.
            if (StringUtils.isNotEmpty(lastSuccessTime)) {
            	JobStatusVo jobStatus = new JobStatusVo(trigger.getJobKey().getName(), 
        				trigger.getJobKey().getGroup(), null, null, null);
            	jobStatus.setLastSuccessTime(DateUtils.strToDate(lastSuccessTime, "yyyy-MM-dd HH:mm:ss"));
            	sqlSessionTemplate.update("jobtrigger.updateLastSuccessTime", jobStatus);
            }
            
        } catch (SchedulerException e) {
        	log.error("failed to change job schedule" + e);
            throw new Exception("failed to change job schedule");
        }
    }

    @PostMapping(value = "/deletejob")
    public void deletejob(@RequestParam(value = "jobClassName") String jobClassName, @RequestParam(value = "jobGroupName") String jobGroupName) throws Exception {
        jobdelete(jobClassName, jobGroupName);
    }

    public void jobdelete(String jobClassName, String jobGroupName) throws Exception {
        scheduler.pauseTrigger(TriggerKey.triggerKey(jobClassName, jobGroupName));
        scheduler.unscheduleJob(TriggerKey.triggerKey(jobClassName, jobGroupName));
        scheduler.deleteJob(JobKey.jobKey(jobClassName, jobGroupName));
        sqlSessionTemplate.delete("jobtrigger.deleteJobStatus", new JobStatusVo(jobClassName, 
        		jobGroupName, null, null, null));
    }

    @GetMapping(value = "/queryjob")
    public Map<String, Object> queryjob(@RequestParam(value = "pageNum") Integer pageNum, @RequestParam(value = "pageSize") Integer pageSize) {
        PageInfo<JobAndTrigger> jobAndTrigger = iJobAndTriggerService.getJobAndTriggerDetails(pageNum, pageSize);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("JobAndTrigger", jobAndTrigger);
        map.put("number", jobAndTrigger.getTotal());
        return map;
    }

    public BaseJob getClass(String classname) throws Exception {
        //Class<?> class1 = Class.forName(classname);
        //BaseJob baseJob = (BaseJob) class1.newInstance();
        BaseJob baseJob = (BaseJob) SpringUtil.getBean(classname);
        return baseJob;
    }

}
