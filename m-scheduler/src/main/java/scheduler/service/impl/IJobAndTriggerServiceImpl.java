package scheduler.service.impl;

import scheduler.dao.JobAndTriggerMapper;
import scheduler.model.JobAndTrigger;
import scheduler.service.IJobAndTriggerService;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class IJobAndTriggerServiceImpl implements IJobAndTriggerService {

    //@Autowired
    //private JobAndTriggerMapper jobAndTriggerMapper;
    
    @Autowired
	private SqlSessionTemplate sqlSessionTemplate;	// transaction 사용 안할 경우 사용

    @Override
    public PageInfo<JobAndTrigger> getJobAndTriggerDetails(Integer pageNum, Integer pageSize) {
        PageHelper.startPage(pageNum, pageSize);
        List<JobAndTrigger> list = sqlSessionTemplate.selectList("jobtrigger.getJobAndTriggerDetails");//jobAndTriggerMapper.getJobAndTriggerDetails();
        PageInfo<JobAndTrigger> page = new PageInfo<>(list);
        return page;
    }
}
