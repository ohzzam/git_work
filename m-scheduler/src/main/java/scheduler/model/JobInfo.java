package scheduler.model;

public class JobInfo {

    private String jobClassName;

    private String jobGroupName;

    private String cronExpression;

    private String jobType;

    private Integer timeType;


   /* public String getJobClassName() {
        return "com.example.quartz.jobs."+jobClassName.trim();
    }*/
    public String getJobClassName() {
        return jobClassName.trim();
    }

    public void setJobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
    }

    public String getJobGroupName() {
        return jobGroupName;
    }

    public void setJobGroupName(String jobGroupName) {
        this.jobGroupName = jobGroupName;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public Integer getTimeType() {
        return timeType;
    }

    public void setTimeType(Integer timeType) {
        this.timeType = timeType;
    }
}
