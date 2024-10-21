package scheduler.service;

import scheduler.model.JobAndTrigger;
import com.github.pagehelper.PageInfo;

public interface IJobAndTriggerService {
    PageInfo<JobAndTrigger> getJobAndTriggerDetails(Integer pageNum, Integer pageSize);
}
