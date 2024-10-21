package scheduler.jobs;

import scheduler.model.JobAndTrigger;
import scheduler.model.JobStatusVo;
import scheduler.service.IJobAndTriggerService;
import scheduler.service.JobService;
import scheduler.tool.SpringUtil;
import com.github.pagehelper.PageInfo;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.mybatis.spring.SqlSessionTemplate;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

@Slf4j
@Component("insaUpdateJob")
public class InsaUpdateJob extends JobService {

	@Autowired
	private SqlSessionTemplate sqlSessionTemplate;	// transaction 사용 안할 경우 사용
	
	public InsaUpdateJob() {
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		/*
		 * IJobAndTriggerService iJobAndTriggerService = (IJobAndTriggerService)
		 * SpringUtil.getBean("IJobAndTriggerServiceImpl"); PageInfo<JobAndTrigger>
		 * jobAndTriggerDetails = iJobAndTriggerService.getJobAndTriggerDetails(1, 10);
		 * System.out.println(jobAndTriggerDetails.getTotal());
		 */

		//Date prevJobFireTime = context.getPreviousFireTime();
		Date jobFireTime = context.getFireTime();
		log.info("> insaUpdateJob Execution Time: " + DateFormatUtils.format(jobFireTime, "yyyy-MM-dd HH:mm:ss"));

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

			log.info(">> insaUpdateJob - Data Retrieve Time: "
							+ DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyy-MM-dd HH:mm:ss"));
					
			processUserInfo(DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyyMMddHHmmss"));
			processDeptInfo(DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyyMMddHHmmss"));
			//processUserInfo(DateFormatUtils.format(new Date(), "yyyyMMddHHmmss"));
			//processDeptInfo(DateFormatUtils.format(new Date(), "yyyyMMddHHmmss"));
			
			jobStatus.setLastFireTime(jobFireTime);
			jobStatus.setLastSuccessTime(jobFireTime);
			jobStatus.setResultMsg("job successfully completed.");
			sqlSessionTemplate.update("jobtrigger.updateJobStatus", jobStatus);

			//commit 한다.
			dbConn.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// 작업 실행결과를 업데이트한다.
			jobStatus.setLastFireTime(jobFireTime);
			jobStatus.setResultMsg("job failed.");
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
		PreparedStatement pstmt = dbConn.prepareStatement("select unew.* from (\n"
				+ "select \n"
				+ " a.ALTKEY        ,\n"
				+ " a.ALTKEYDECODE  ,\n"
				+ " a.USERNAME      ,\n"
				+ " a.DEPTCD        ,\n"
				+ " omst.DEPTNM  as DEPTNM   ,\n"
				+ " a.DEPTNM     as INSADEPTNM  ,\n"
				+ " a.JIKWICD    as INSAJIKWICD  ,\n"
				+ " a.JIKWINM    as INSAJIKWINM  ,\n"
				+ " a.GRADE         ,\n"
				+ " a.GRADENM    as GRADENAME    ,\n"
				+ " a.BIRTHDT       ,\n"
				+ " a.GENDER        ,\n"
				+ " a.INITDT        ,\n"
				+ " a.DEPTCD1       ,\n"
				+ " a.DEPTCD2       ,\n"
				+ " a.JIKWICD1      ,\n"
				+ " a.JIKWICD2      ,\n"
				+ " a.JIKWINM1      ,\n"
				+ " a.JIKWINM2      ,\n"
				+ " NVL(TRIM(a.ISDELETED),'0')  as ISDELETED ,\n"
				+ " '00' AS SOURCEORG   ,\n"
				+ " CASE\n"
				+ "     WHEN (SELECT COUNT(*) from com_user_mst_new where ALTKEY = a.ALTKEY AND DEPTCD != a.DEPTCD) = 1\n"
				+ "    	   THEN (SELECT DEPTCD from com_user_mst_new where ALTKEY = a.ALTKEY)\n"
				+ "     ELSE '' END as BEFOREDEPTCD,\n"
				+ " a.UPDT          ,\n"
				+ " a.TRANSINSDT    ,\n"
				+ " CASE\n"
				+ "    WHEN (SELECT COUNT(*) from com_user_mst_new where ALTKEY = a.ALTKEY) = 0\n"
				+ "   	  THEN TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') \n"
				+ "	   ELSE '' END as REGDT,\n"
				+ " TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') as UPDATEDT,\n"
				+ " '' as UPDATEID,\n"
				+ " CASE\n"
				+ "     WHEN (SELECT COUNT(*) from com_user_mst_new where ALTKEY = a.ALTKEY) = 0\n"
				+ "        THEN 'ADD'\n"
				+ "	    WHEN (SELECT COUNT(*) from com_user_mst_new where ALTKEY = a.ALTKEY) = 1\n"
				+ "        THEN 'MOD'\n"
				+ "	    WHEN a.ISDELETED <> '0'\n"
				+ "        THEN 'DEL'\n"
				+ "	    ELSE 'MOD' END as GUBUN\n"
		        + "from com_insa_user_new a, com_org_mst_new omst\n"
				+ "where a.TRANSINSDT >= ? AND a.DEPTCD = omst.DEPTCD(+)\n"
				+ ") unew, com_user_mst_new umst\n"
				+ " where unew.ALTKEY = umst.ALTKEY(+) AND (unew.DEPTCD LIKE '61%' OR (unew.DEPTCD LIKE '3%' AND umst.DEPTCD != '6199903' AND umst.DEPTCD NOT LIKE '3%'))\n"
		        + "UNION ALL\n"
				+ "SELECT * from (\n"
				+ "SELECT \n"
				+ "	b.ALT_KEY AS ALTKEY ,\n"
				+ "	b.ALTKEYDECODE AS ALTKEYDECODE,\n"
				+ "	b.NAME AS USERNAME,\n"
				+ "	substr(b.DEPT_CD,0,7) AS DEPTCD,\n"
				+ "	mst.DEPTNM AS DEPTNM,\n"
				+ "	'' AS INSADEPTNM,\n"
				+ "	'' AS INSAJIKWICD,\n"
				+ "	'' AS INSAJIKWINM,\n"
				+ "	b.RANK_CODE AS GRADE, \n"
				+ "	'' AS GRADENAME,\n"
				+ "	b.BIRTH_DT AS BIRTHDT,\n"
				+ "	b.GENDER,\n"
				+ "	b.INIT_DT AS INITDT,\n"
				+ "	'' AS DEPTCD1       ,\n"
				+ "	'' AS DEPTCD2       ,\n"
				+ "	'' AS JIKWICD1      ,\n"
				+ "	'' AS JIKWICD2      ,\n"
				+ "	'' AS JIKWINM1      ,\n"
				+ "	'' AS JIKWINM2      ,\n"
				+ "	'0' AS ISDELETED    ,\n"
				+ " '01' AS SOURCEORG   ,\n"
				+ "	CASE\n"
				+ "	    WHEN (SELECT COUNT(*) from com_user_mst_new where ALTKEY = b.ALT_KEY AND DEPT_CD != substr(b.DEPT_CD,0,7)) = 1\n"
				+ "	   	  THEN (SELECT DEPTCD from com_user_mst_new where ALTKEY = b.ALT_KEY)\n"
				+ "	    ELSE '' END as BEFOREDEPTCD,\n"
				+ "	b.CHANGE_DT AS UPDT          ,\n"
				+ "	''  AS TRANSINSDT    ,\n"
				+ " CASE\n"
				+ "    WHEN (SELECT COUNT(*) from com_user_mst_new where ALTKEY = b.ALT_KEY) = 0\n"
				+ "   	  THEN TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') \n"
				+ "	ELSE '' END as REGDT,\n"
				+ "	TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') as UPDATEDT,\n"
				+ "	'' as UPDATEID,\n"
				+ " CASE\n"
				+ "	    WHEN (SELECT COUNT(*) from com_user_mst_new where ALTKEY = b.ALT_KEY) = 0\n"
				+ "	   	  THEN 'ADD'\n"
				+ "		   WHEN (SELECT COUNT(*) from com_user_mst_new where ALTKEY = b.ALT_KEY AND\n"
				+ "		     (ALTKEYDECODE != b.ALTKEYDECODE OR USERNAME != b.NAME OR DEPTCD != substr(b.DEPT_CD,0,7) OR GRADE != b.RANK_CODE OR BIRTHDT != b.BIRTH_DT OR GENDER != b.GENDER OR INITDT != b.INIT_DT)) = 1\n"
				+ "	  	  THEN 'MOD'\n"
				+ "	    ELSE '' END as GUBUN\n"
				+ "FROM com_sobanginsa_mst@sobanginsa_link b, com_org_mst_new mst\n"
				+ "WHERE b.CHANGE_DT >= ? AND substr(b.DEPT_CD,0,7) = mst.DEPTCD(+)) sobang\n"
				+ "WHERE sobang.GUBUN IS NOT null");

		// bind param
		pstmt.setString(1, dateTime);
		pstmt.setString(2, dateTime.substring(0,8));

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
		PreparedStatement pstmt = dbConn.prepareStatement("SELECT * FROM ("
				+ " select \n"
				+ "  DEPTCD,\n"
				+ "  DEPTNM,\n"
				+ "  NEXTDEPTCD  as PARENTDEPTCD,\n"
				+ "  PREDEPTCD   as TOPDEPTCD   ,\n"
				+ "  RPRSNTATVDEPTCD  as REPREDEPTCD,\n"
				+ "  UPDT,\n"
				+ "  '0' AS ISDELETED,\n"
				+ "  '00' AS SOURCEORG,\n"
				+ "  'N' CONFIRMYN,\n"
				+ "  CASE\n"
				+ "	    WHEN (SELECT COUNT(*) from com_org_mst_new where DEPTCD = a.DEPTCD) = 0\n"
				+ "	   	  THEN TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') \n"
				+ "	   	ELSE '' END as REGDT,\n"
				+ "  TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') as UPDATEDT,\n"
				+ "  '' as UPDATEID,\n"
				+ "  CASE\n"
				+ "    WHEN (SELECT COUNT(*) from com_org_mst_new where DEPTCD = a.DEPTCD) = 0\n"
				+ "   	 THEN 'ADD'\n"
				+ "	   WHEN (SELECT COUNT(*) from com_org_mst_new where DEPTCD = a.DEPTCD\n"
				+ "		  AND (DEPTNM != a.DEPTNM OR PARENTDEPTCD != a.NEXTDEPTCD OR TOPDEPTCD != a.PREDEPTCD OR REPREDEPTCD != a.RPRSNTATVDEPTCD)) = 1\n"
				+ "  	 THEN 'MOD'\n"
				//+ "    WHEN GUBUN = 'DEL'\n"
				//+ "      THEN 'DEL'\n"
				+ "	   ELSE '' END as GUBUN\n"
		        + " from com_insa_org_new a\n"
				+ " where TRANSINSDT >= ?) insa\n"
				+ "WHERE insa.gubun IS NOT null\n"
				+ "UNION ALL\n"
				+ "SELECT * FROM (\n"
				+ " SELECT \n"
				+ "	 substr(DEPT_CD,0,7) AS DEPTCD,\n"
				+ "	 DEPT_NM AS DEPTNM,\n"
				+ "	 substr(NEXT_DEPT_CD,0,7)  as PARENTDEPTCD,\n"
				+ "	 substr(PRE_DEPT_CD,0,7)   as TOPDEPTCD   ,\n"
				+ "	 substr(RPRSNTATV_DEPT_CD,0,7)  as REPREDEPTCD,\n"
				+ "	 TO_CHAR(SYSDATE,'YYYYMMDD') AS UPDT,\n"
				+ "	 '0' AS ISDELETED,\n"
				+ "  '01' AS SOURCEORG,\n"
				+ "  'N' CONFIRMYN,\n"
				+ " 	CASE\n"
				+ "	    WHEN (SELECT COUNT(*) from com_org_mst_new where DEPTCD = substr(b.DEPT_CD,0,7)) = 0\n"
				+ "	   	  THEN TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') \n"
				+ "	   	ELSE '' END as REGDT,\n"
				+ "	 TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') as UPDATEDT,\n"
				+ "	 '' as UPDATEID,\n"
				+ "	 CASE\n"
				+ "	    WHEN (SELECT COUNT(*) from com_org_mst_new where DEPTCD = substr(b.DEPT_CD,0,7)) = 0\n"
				+ "	   	  THEN 'ADD'\n"
				+ "		WHEN (SELECT COUNT(*) from com_org_mst_new where DEPTCD = substr(b.DEPT_CD,0,7) \n"
				+ "		   AND (DEPTNM != b.DEPT_NM OR PARENTDEPTCD != substr(b.NEXT_DEPT_CD,0,7) OR TOPDEPTCD != substr(b.PRE_DEPT_CD,0,7) OR REPREDEPTCD != substr(b.RPRSNTATV_DEPT_CD,0,7))) = 1\n"
				+ "	  	  THEN 'MOD'\n"
				+ "		ELSE '' END as GUBUN\n"
				+ " from com_sobanginsa_org@sobanginsa_link b) sobang \n"
				+ "WHERE sobang.gubun IS NOT null");

		// bind param
		pstmt.setString(1, dateTime);

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
				"USERNAME",
				"DEPTCD",
				"DEPTNM",
				"INSADEPTNM",
				"INSAJIKWICD",
				"INSAJIKWINM",
				"GRADE",
				"GRADENAME",
				"BIRTHDT",
				"GENDER",
				"INITDT",
				"DEPTCD1",
				"DEPTCD2",
				"JIKWICD1",
				"JIKWICD2",
				"JIKWINM1",
				"JIKWINM2",
				"ISDELETED",
				"SOURCEORG",
				"BEFOREDEPTCD",
				"UPDT",
				"REGDT",
				"UPDATEDT"};
		
		String sql = makeInsertSql("com_user_mst_new", colNames);
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
				"USERNAME",
				"DEPTCD",
				"DEPTNM",
				"INSADEPTNM",
				"INSAJIKWICD",
				"INSAJIKWINM",
				"GRADE",
				"GRADENAME",
				"BIRTHDT",
				"GENDER",
				"INITDT",
				"DEPTCD1",
				"DEPTCD2",
				"JIKWICD1",
				"JIKWICD2",
				"JIKWINM1",
				"JIKWINM2",
				"ISDELETED",
				"SOURCEORG",
				"BEFOREDEPTCD",
				"UPDT",
				"UPDATEDT",
				"UPDATEID"};
		
		String sql = makeUpdateSql("com_user_mst_new", colNames);
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
		
		String sql = makeUpdateSql("com_user_mst_new", colNames);
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
				"TOPDEPTCD",
				"REPREDEPTCD",
				"UPDT",
				"ISDELETED",
				"SOURCEORG",
				"CONFIRMYN",
				"REGDT",
				"UPDATEDT",
				"UPDATEID"};
		
		String sql = makeInsertSql("com_org_mst_new", colNames);
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
				"TOPDEPTCD",
				"REPREDEPTCD",
				"UPDT",
				"ISDELETED",
				"SOURCEORG",
				"CONFIRMYN",
				"UPDATEDT",
				"UPDATEID"};
		
		String sql = makeUpdateSql("com_org_mst_new", colNames);
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
