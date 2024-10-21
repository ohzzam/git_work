package scheduler.jobs;

import scheduler.model.JobStatusVo;
import scheduler.service.JobService;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.mybatis.spring.SqlSessionTemplate;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ndata.module.StringUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

@Slf4j
@Component("insaUpdateJobBmsv")
public class InsaUpdateJobBmsv extends JobService {

	@Autowired
	private SqlSessionTemplate sqlSessionTemplate;	// transaction 사용 안할 경우 사용
	
	public InsaUpdateJobBmsv() {
		//get run envs
		//getRunEnvs();
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		//Date prevJobFireTime = context.getPreviousFireTime();
		Date jobFireTime = context.getFireTime();
		log.info("> insaUpdateJobBmsv Execution Time: " + DateFormatUtils.format(jobFireTime, "yyyy-MM-dd HH:mm:ss"));

		Trigger trigger = context.getTrigger();
		JobStatusVo jobStatus = new JobStatusVo(trigger.getJobKey().getName(), 
				trigger.getJobKey().getGroup(), null, null, null);

		try {
			connectDB();
			
			dbConn.setAutocommit(false);
			
			// 마지막 작업 실행 결과를 조회한다.
			jobStatus = sqlSessionTemplate.selectOne("jobtrigger.getJobStatus", jobStatus);
			if (jobStatus == null) {
				jobStatus = new JobStatusVo(trigger.getJobKey().getName(), 
								trigger.getJobKey().getGroup(), jobFireTime, jobFireTime, null);
			} else if (jobStatus.getLastSuccessTime() == null) {
				jobStatus.setLastSuccessTime(jobFireTime);
			}

			//작업상태를 실행중으로 업데이트한다.
			jobStatus.setResultMsg(JOB_STATUS_EXECUTING);
			sqlSessionTemplate.update("jobtrigger.updateJobStatus", jobStatus);
			
			//1 Day 전의 날짜로 조회한다.
			String sPrevRetrieveTime = StringUtils.getPreviousTimeString(jobStatus.getLastSuccessTime().getTime(), "yyyyMMdd", 1, 0);
			log.info(">> insaUpdateJobBmsv - Data Retrieve Time: {}", sPrevRetrieveTime);
					
			processUserInfo(sPrevRetrieveTime);
			processDeptInfo(sPrevRetrieveTime);
			//processUserInfo(DateFormatUtils.format(new Date(), "yyyyMMddHHmmss"));
			//processDeptInfo(DateFormatUtils.format(new Date(), "yyyyMMddHHmmss"));
			
			//commit 한다.
			dbConn.commit();
			
			jobStatus.setLastFireTime(jobFireTime);
			jobStatus.setLastSuccessTime(new Date());//작업완료시간을 마지막성공시간으로 업데이트한다.
			jobStatus.setResultMsg(JOB_STATUS_COMPLETED);
			sqlSessionTemplate.update("jobtrigger.updateJobStatus", jobStatus);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			// 작업 실행결과를 업데이트한다.
			jobStatus.setLastFireTime(jobFireTime);
			jobStatus.setResultMsg(JOB_STATUS_FAILED);
			sqlSessionTemplate.update("jobtrigger.updateJobStatus", jobStatus);
			
			//rollback 한다.
			try {
				dbConn.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				log.error(e1.getMessage());
			}
			
			throw new JobExecutionException(e.getMessage());
		} finally {
			try {
				dbConn.disconnect();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
	}

	private void processUserInfo(String dateTime) throws Exception {
		// prepare sql statement
		String sql = "SELECT * FROM (\n"
				+ " SELECT bmsv.*,\n"
				+ "    CASE\n"
				+ "	 WHEN (SELECT COUNT(*) from " + MST_USER_TABLE + " where ALTKEY = encaes(bmsv.ALTKEYDECODE)) = 0\n"
				+ "	  THEN 'ADD'\n"
				+ "	 WHEN (SELECT COUNT(*) from " + MST_USER_TABLE + " where ALTKEY = encaes(bmsv.ALTKEYDECODE) AND\n"
				+ "	     (LOGINID != bmsv.LOGINID OR USERNAME != bmsv.USERNAME OR DEPTCD != bmsv.DEPTCD OR INSAJIKWICD != bmsv.INSAJIKWICD OR INSAJIKWINM != bmsv.INSAJIKWINM \n"
				+ "	      OR GRADE != bmsv.GRADE OR GRADENAME != bmsv.GRADENAME OR USERORDER != bmsv.USERORDER OR ISDELETED != bmsv.ISDELETED OR BIRTHDT != bmsv.BIRTHDT OR GENDER != bmsv.GENDER OR INITDT != bmsv.INITDT\n"
				+ "	      OR SEMAIL != bmsv.SEMAIL OR SOFFICEPHONE != bmsv.SOFFICEPHONE OR SEMAIL != bmsv.SEMAIL OR SMOBILEPHONE != bmsv.SMOBILEPHONE)) = 1\n"
				+ "	  THEN 'MOD'\n"
				+ "	 ELSE '' END as GUBUN\n"
				+ " FROM (SELECT \n"
				+ "	encaes(b.ALTKEYDECODE)  AS ALTKEY ,\n"
				+ "	b.ALTKEYDECODE AS ALTKEYDECODE,\n"
				+ "	b.LOGINID,\n"
				+ "	b.USERNAME,\n"
				+ "	b.DEPTCD,\n"
				+ "	mst.DEPTNM AS DEPTNM,\n"
				+ "	REPLACE(b.SPOSITION,'9999999',NULL) AS INSAJIKWICD,\n"
				+ "	b.POSITIONDETAIL AS INSAJIKWINM,\n"
				+ "	b.GRADE, \n"
				+ "	b.GRADENAME,\n"
				+ "	NVL(b.USERORDER,99999) AS USERORDER,\n"
				+ "  CASE\n"
				+ "	 WHEN b.USER_STAT LIKE 'AA%' OR b.USER_STAT LIKE 'BA%'\n"
				+ "	  THEN '0'\n"
				+ "	 WHEN b.USER_STAT LIKE 'AB%'\n"
				+ "	  THEN '3'\n"
				+ "	 WHEN b.USER_STAT LIKE 'DA%'\n"
				+ "	  THEN '2'  \n"
				+ "	 ELSE '' END as ISDELETED,\n"
				+ "	substr(b.UPDATEDT,0,8) AS UPDT,\n"
				+ "	b.BIRTHDT,\n"
				+ "	b.GENDER,\n"
				+ "	b.INITDT,\n"
				+ "	b.SEMAIL,\n"
				+ " b.SOFFICEPHONE,\n"
				+ " b.SMOBILEPHONE,\n"
				+ " '" + SOURCE_ORG + "' AS SOURCEORG,\n"
				+ " CASE\n"
				+ "	 WHEN (SELECT COUNT(*) from " + MST_USER_TABLE + " where ALTKEY = encaes(b.ALTKEYDECODE) AND DEPTCD != b.DEPTCD) = 1\n"
				+ "	  THEN (SELECT DEPTCD from " + MST_USER_TABLE + " where ALTKEY = encaes(b.ALTKEYDECODE))\n"
				+ "	 ELSE '' END as BEFOREDEPTCD,\n"
				+ " CASE\n"
				+ "  WHEN (SELECT COUNT(*) from " + MST_USER_TABLE + " where ALTKEY = encaes(b.ALTKEYDECODE)) = 0\n"
				+ "   THEN TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') \n"
				+ "	 ELSE '' END as REGDT,\n"
				+ " TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') as UPDATEDT,\n"
				+ "	'' as UPDATEID\n"
				+ " FROM " + INSA_USER_TABLE + " b, " + MST_ORG_TABLE + " mst\n"
				+ " WHERE b.ALTKEYDECODE IS NOT NULL AND b.DEPTCD IS NOT NULL AND (substr(b.UPDATEDT,0,8) >= ? OR substr(b.REGDT,0,8) >= ?)\n"
				+ "       AND b.DEPTCD = mst.DEPTCD(+)) bmsv)\n"
				+ "WHERE GUBUN IS NOT NULL";
		//log.info("processUserInfo sql={},updatedt>={}", sql, dateTime);
		PreparedStatement pstmt = dbConn.prepareStatement(sql);

		// bind param
		pstmt.setString(1, dateTime);
		pstmt.setString(2, dateTime);

		// execute query
		ResultSet rs = pstmt.executeQuery();
		
		while (rs.next()) {
			String type = rs.getString("GUBUN"); 
			switch(type) {
				case "ADD":
					insUserInfo(dateTime, rs);
					break;
				case "MOD":
					updateUserInfo(dateTime, rs);
					break;
				case "DEL":
					delUserInfo(dateTime, rs);
					break;
			}
		}
	}

	private void processDeptInfo(String dateTime) throws Exception {
		// prepare sql statement
		String sql = "SELECT * FROM (\n"
				+ " SELECT \n"
				+ "   DEPTCD,\n"
				+ "   DEPTNM,\n"
				+ "   replace(b.PARENTDEPTCD,'ROOT','1000000') AS PARENTDEPTCD,\n"
				+ "   DEPTORDER,\n"
				+ "   TOPDEPTCD,\n"
				+ "   DEPTNM as DISPLAYNAME,\n"
				+ "   TOPDEPTCD as REPREDEPTCD,\n"
				+ "   substr(UPDATEDT,0,8) AS UPDT,\n"
				+ "   replace(ISDELETED,'1','0') AS ISDELETED,\n"
				+ "   DEPTDEPTH,\n"
				+ "   '" + SOURCE_ORG + "' AS SOURCEORG,\n"
				+ "   CASE\n"
				+ " 	    WHEN (SELECT COUNT(*) from " + MST_ORG_TABLE + " where DEPTCD = b.DEPTCD) = 0\n"
				+ " 	   	  THEN TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') \n"
				+ " 	   	ELSE '' END as REGDT,\n"
				+ "   TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') as UPDATEDT,\n"
				+ "   '' as UPDATEID,\n"
				+ "   CASE\n"
				+ " 	    WHEN (SELECT COUNT(*) from " + MST_ORG_TABLE + " where DEPTCD = b.DEPTCD) = 0\n"
				+ " 	   	  THEN 'ADD'\n"
				+ " 		WHEN (SELECT COUNT(*) from " + MST_ORG_TABLE + " where DEPTCD = b.DEPTCD \n"
				+ " 		   AND (DEPTNM != b.DEPTNM OR PARENTDEPTCD != replace(b.PARENTDEPTCD,'ROOT','1000000') OR TOPDEPTCD != b.TOPDEPTCD\n"
				+ "            OR DISPLAYNAME != b.DEPTNM OR REPREDEPTCD != b.TOPDEPTCD OR UPDT != substr(b.UPDATEDT,0,8) OR DEPTDEPTH != b.DEPTDEPTH OR ISDELETED != replace(b.ISDELETED,'1','0'))) = 1\n"
				+ " 	  	  THEN 'MOD'\n"
				+ " 		ELSE '' END as GUBUN\n"
				+ " FROM " + INSA_ORG_TABLE + " b\n"
				+ " WHERE ISDELETED = '1') bmsv \n"
				+ "WHERE bmsv.GUBUN IS NOT NULL AND bmsv.DEPTNM IS NOT NULL";

		PreparedStatement pstmt = dbConn.prepareStatement(sql);

		// execute query
		ResultSet rs = pstmt.executeQuery();
		
		while (rs.next()) {
			String type = rs.getString("GUBUN"); 
			switch(type) {
				case "ADD":
					insDeptInfo(dateTime, rs);
					break;
				case "MOD":
					updateDeptInfo(dateTime, rs);
					break;
				case "DEL":
					break;
			}
		}
	}
	
	private void insUserInfo(String dateTime, ResultSet rs) throws Exception {
		String[] colNames = {
				"ALTKEY",
				"ALTKEYDECODE",
				"LOGINID",
				"USERNAME",
				"DEPTCD",
				"DEPTNM",
				"INSAJIKWICD",
				"INSAJIKWINM",
				"GRADE",
				"GRADENAME",
				"USERORDER",
				"ISDELETED",
				"BIRTHDT",
				"GENDER",
				"INITDT",
				"SEMAIL",
				"SOFFICEPHONE",
				"SMOBILEPHONE",
				"SOURCEORG",
				"BEFOREDEPTCD",
				"UPDT",
				"REGDT",
				"UPDATEDT"};
		
		String sql = makeInsertSql(MST_USER_TABLE, colNames);
		//log.info(">>ADD_SQL=[{}]", sql);
		
		PreparedStatement pstmt = dbConn.prepareStatement(sql);
				
		StringBuilder updateData = new StringBuilder();
		String colData = null;
		for(int i=0; i < colNames.length; i++) {
			colData = rs.getString(colNames[i]);
			pstmt.setString(i+1, colData);
			updateData.append(colNames[i] + "=" + colData + ",");
		}
		pstmt.execute();
		pstmt.close();
		//add update log
		addUpdateLog(dateTime, rs.getString("UPDATEDT"), rs.getString("ALTKEY"), JOB_TARGET_USER, JOB_TYPE_ADD, updateData.toString());
	}
	
	private void updateUserInfo(String dateTime, ResultSet rs) throws Exception {
		String[] colNames = {
				"LOGINID",
				"USERNAME",
				"DEPTCD",
				"DEPTNM",
				"INSAJIKWICD",
				"INSAJIKWINM",
				"GRADE",
				"GRADENAME",
				"USERORDER",
				"ISDELETED",
				"BIRTHDT",
				"GENDER",
				"INITDT",
				"SEMAIL",
				"SOFFICEPHONE",
				"SMOBILEPHONE",
				"SOURCEORG",
				"BEFOREDEPTCD",
				"UPDT",
				"UPDATEDT",
				"UPDATEID"};
		
		String sql = makeUpdateSql(MST_USER_TABLE, colNames);
		sql += " where ALTKEY = ?";
		//log.info(">>MOD_SQL=[{}]", sql);
		
		PreparedStatement pstmt = dbConn.prepareStatement(sql);
				
		StringBuilder updateData = new StringBuilder();
		String colData = null;
		String altKey = rs.getString("ALTKEY");
		updateData.append("ALTKEY=" + altKey + ",");
		for(int i=0; i < colNames.length; i++) {
			colData = rs.getString(colNames[i]);
			pstmt.setString(i+1, colData);
			updateData.append(colNames[i] + "=" + colData + ",");
		}
		
		pstmt.setString(colNames.length+1 , altKey);
		pstmt.execute();
		pstmt.close();
		//add update log
		addUpdateLog(dateTime, rs.getString("UPDATEDT"), altKey, JOB_TARGET_USER, JOB_TYPE_MOD, updateData.toString());
	}
	
	private void delUserInfo(String dateTime, ResultSet rs) throws Exception {
		String[] colNames = {
				"ISDELETED",
				"UPDT",
				"UPDATEDT",
				"UPDATEID"};
		
		String sql = makeUpdateSql(MST_USER_TABLE, colNames);
		sql += " where ALTKEY = ?";
		//log.info("DEL_SQL=[{}]", sql);
		
		PreparedStatement pstmt = dbConn.prepareStatement(sql);
				
		StringBuilder updateData = new StringBuilder();
		String colData = null;
		String altKey = rs.getString("ALTKEY");
		updateData.append("ALTKEY=" + altKey + ",");
		for(int i=0; i < colNames.length; i++) {
			colData = rs.getString(colNames[i]);
			pstmt.setString(i+1, colData);
			updateData.append(colNames[i] + "=" + colData + ",");
		}
		
		pstmt.setString(colNames.length+1 , altKey);
		pstmt.execute();
		//add update log
		addUpdateLog(dateTime, rs.getString("UPDATEDT"), altKey, JOB_TARGET_USER, JOB_TYPE_DEL, updateData.toString());	
	}

	private void insDeptInfo(String dateTime, ResultSet rs) throws Exception {
		String[] colNames = {
				"DEPTCD",
				"DEPTNM",
				"PARENTDEPTCD",
				"DEPTORDER",
				"TOPDEPTCD",
				"DISPLAYNAME",
				"REPREDEPTCD",
				"UPDT",
				"ISDELETED",
				"DEPTDEPTH",
				"SOURCEORG",
				"REGDT",
				"UPDATEDT",
				"UPDATEID"};
		
		String sql = makeInsertSql(MST_ORG_TABLE, colNames);
		//log.info("SQL=[{}]", sql);
		
		PreparedStatement pstmt = dbConn.prepareStatement(sql);
				
		StringBuilder updateData = new StringBuilder();
		String colData = null;
		for(int i=0; i < colNames.length; i++) {
			colData = rs.getString(colNames[i]);
			pstmt.setString(i+1, colData);
			updateData.append(colNames[i] + "=" + colData + ",");
		}
		pstmt.execute();
		pstmt.close();
		//add update log
		addUpdateLog(dateTime, rs.getString("UPDATEDT"), rs.getString("DEPTCD"), JOB_TARGET_ORG, JOB_TYPE_ADD, updateData.toString());
	}
	
	private void updateDeptInfo(String dateTime, ResultSet rs) throws Exception {
		String[] colNames = {
				"DEPTNM",
				"PARENTDEPTCD",
				//"DEPTORDER",
				"TOPDEPTCD",
				"DISPLAYNAME",
				"REPREDEPTCD",
				"UPDT",
				"ISDELETED",
				"DEPTDEPTH",
				"SOURCEORG",
				"UPDATEDT",
				"UPDATEID"};
		
		String sql = makeUpdateSql(MST_ORG_TABLE, colNames);
		sql += " where DEPTCD = ?";
		//log.info(">>MOD_SQL=[{}]", sql);
		
		PreparedStatement pstmt = dbConn.prepareStatement(sql);
				
		StringBuilder updateData = new StringBuilder();
		String colData = null;
		String deptCd = rs.getString("DEPTCD");
		updateData.append("DEPTCD=" + deptCd + ",");
		for(int i=0; i < colNames.length; i++) {
			colData = rs.getString(colNames[i]);
			pstmt.setString(i+1, colData);
			updateData.append(colNames[i] + "=" + colData + ",");
		}
		
		pstmt.setString(colNames.length+1 , deptCd);
		pstmt.execute();
		pstmt.close();
		//add update log
		addUpdateLog(dateTime, rs.getString("UPDATEDT"), deptCd, JOB_TARGET_ORG, JOB_TYPE_MOD, updateData.toString());
	}
}
