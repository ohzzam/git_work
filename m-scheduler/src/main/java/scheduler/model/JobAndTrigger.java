package scheduler.model;

import java.util.Date;
import java.math.BigInteger;

public class JobAndTrigger {

    private String JOB_NAME;
    private String JOB_GROUP;
    private String JOB_CLASS_NAME;
    private String TRIGGER_NAME;
    private String TRIGGER_GROUP;
    private BigInteger REPEAT_INTERVAL;
    private BigInteger TIMES_TRIGGERED;
    private String CRON_EXPRESSION;
    private String TIME_ZONE_ID;
    private Date LAST_FIRE_TIME;
    private Date LAST_SUCCESS_TIME;

    public String getJOB_NAME() {
        return JOB_NAME;
    }
    public void setJOB_NAME(String jOB_NAME) {
        JOB_NAME = jOB_NAME;
    }
    public String getJOB_GROUP() {
        return JOB_GROUP;
    }
    public void setJOB_GROUP(String jOB_GROUP) {
        JOB_GROUP = jOB_GROUP;
    }
    public String getJOB_CLASS_NAME() {
        return JOB_CLASS_NAME;
    }
    public void setJOB_CLASS_NAME(String jOB_CLASS_NAME) {
        JOB_CLASS_NAME = jOB_CLASS_NAME;
    }
    public String getTRIGGER_NAME() {
        return TRIGGER_NAME;
    }
    public void setTRIGGER_NAME(String tRIGGER_NAME) {
        TRIGGER_NAME = tRIGGER_NAME;
    }
    public String getTRIGGER_GROUP() {
        return TRIGGER_GROUP;
    }
    public void setTRIGGER_GROUP(String tRIGGER_GROUP) {
        TRIGGER_GROUP = tRIGGER_GROUP;
    }
    public BigInteger getREPEAT_INTERVAL() {
        return REPEAT_INTERVAL;
    }
    public void setREPEAT_INTERVAL(BigInteger rEPEAT_INTERVAL) {
        REPEAT_INTERVAL = rEPEAT_INTERVAL;
    }
    public BigInteger getTIMES_TRIGGERED() {
        return TIMES_TRIGGERED;
    }
    public void setTIMES_TRIGGERED(BigInteger tIMES_TRIGGERED) {
        TIMES_TRIGGERED = tIMES_TRIGGERED;
    }
    public String getCRON_EXPRESSION() {
        return CRON_EXPRESSION;
    }
    public void setCRON_EXPRESSION(String cRON_EXPRESSION) {
        CRON_EXPRESSION = cRON_EXPRESSION;
    }
    public String getTIME_ZONE_ID() {
        return TIME_ZONE_ID;
    }
    public void setTIME_ZONE_ID(String tIME_ZONE_ID) {
        TIME_ZONE_ID = tIME_ZONE_ID;
    }
    public Date getLAST_FIRE_TIME() {
        return LAST_FIRE_TIME;
    }
    public void setLAST_FIRE_TIME(Date lAST_FIRE_TIME) {
        LAST_FIRE_TIME = lAST_FIRE_TIME;
    }
    public Date getLAST_SUCCESS_TIME() {
        return LAST_SUCCESS_TIME;
    }
    public void setLAST_SUCCESS_TIME(Date lAST_SUCCESS_TIME) {
        LAST_SUCCESS_TIME = lAST_SUCCESS_TIME;
    }
}
