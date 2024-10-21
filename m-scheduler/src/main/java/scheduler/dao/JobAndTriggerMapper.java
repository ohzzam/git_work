package scheduler.dao;

import scheduler.model.JobAndTrigger;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class JobAndTriggerMapper {
	
	@Autowired
	private SqlSessionTemplate sqlSessionTemplate;	// transaction 사용 안할 경우 사용

    public List<JobAndTrigger> getJobAndTriggerDetails() {
    	List<JobAndTrigger> jobTrigger = sqlSessionTemplate.selectList("jobtrigger.getJobAndTriggerDetails");
    	return jobTrigger;
    };
}
