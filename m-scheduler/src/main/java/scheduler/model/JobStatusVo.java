package scheduler.model;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class JobStatusVo {
	
	private String jobTriggerName;

    private String jobGroupName;

    private Date lastFireTime;
    
    private Date lastSuccessTime;
    
    private String resultMsg;
}
