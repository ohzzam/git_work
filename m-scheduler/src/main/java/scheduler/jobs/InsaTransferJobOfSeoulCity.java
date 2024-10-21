package scheduler.jobs;

import scheduler.model.JobStatusVo;
import scheduler.service.JobService;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.mybatis.spring.SqlSessionTemplate;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ndata.handler.CSVHandler;
import com.ndata.handler.CSVHandler.CSVWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.UUID;

@Slf4j
@Component("insaTransferJobOfSeoulCity")
public class InsaTransferJobOfSeoulCity extends JobService {
	
	private final static String DEPTCD_SEOUL = "61";
	private final static String DEPTCD_SEOUL_FILTER = "'" + DEPTCD_SEOUL + "%'";
	private final static String OUT_FOLDER = "/" + DEPTCD_SEOUL + "10000/receive/";
	
	@Autowired
	private SqlSessionTemplate sqlSessionTemplate;	// transaction 사용 안할 경우 사용

	public InsaTransferJobOfSeoulCity() {
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		//Date prevJobFireTime = context.getPreviousFireTime();
		Date jobFireTime = context.getFireTime();
		log.info("> insaTransferJobOfSeoulCity Execution Time: " + DateFormatUtils.format(jobFireTime, "yyyy-MM-dd HH:mm:ss"));

		Trigger trigger = context.getTrigger();
		JobStatusVo jobStatus = new JobStatusVo(trigger.getJobKey().getName(), 
				trigger.getJobKey().getGroup(), null, null, null);

		try {
			connectDB();
			
			// 마지막 작업 실행 결과를 조회한다.
			jobStatus = sqlSessionTemplate.selectOne("jobtrigger.getJobStatus", jobStatus);
			if (jobStatus == null) {
				jobStatus = new JobStatusVo(trigger.getJobKey().getName(), 
								trigger.getJobKey().getGroup(), jobFireTime, jobFireTime, null);
			} else if (jobStatus.getLastSuccessTime() == null) {
				jobStatus.setLastSuccessTime(jobFireTime);
			}

			log.info(">> insaTransferJobOfSeoulCity - Data Retrieve Time: "
							+ DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyy-MM-dd HH:mm:ss"));
					
			processUserInfo(DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyyMMddHHmmss"));
			processDeptInfo(DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyyMMddHHmmss"));
			
			jobStatus.setLastFireTime(jobFireTime);
			jobStatus.setLastSuccessTime(jobFireTime);
			jobStatus.setResultMsg("job successfully completed.");
			sqlSessionTemplate.update("jobtrigger.updateJobStatus", jobStatus);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			// 작업 실행결과를 업데이트한다.
			jobStatus.setLastFireTime(jobFireTime);
			jobStatus.setResultMsg("job failed.");
			sqlSessionTemplate.update("jobtrigger.updateJobStatus", jobStatus);
			
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
		//TYPE^ALTKEYDECODE^LOGINID^USERNAME^DEPTID^SUSERID^INSADEPTNAME^INSASPOSITION^GRADE^USERORDER^ISDELETED^ISCONCURRENT^INSAPOSITIONDETAIL^INDT^UPDT^BIRTHDT^GENDER^INITDT^
		//ALTKEY^GPKICN^SEMAIL^HOMEPAGE^SOFFICEPHONE^SMOBILEPHONE^GRADENAME^DEPTID2^DEPTID3^SPOSITION2^SPOSITION3^POSITIONDETAIL2^POSITIONDETAIL3^SOURCEORG^SPOSITION^POSITIONDETAIL^DEPTNAME
		CSVWriter csvWriter = null;
		
		try {
			//prepare sql statement
			PreparedStatement pstmt = dbConn
					.prepareStatement("select CASE\n"
							+ "    WHEN REGDT > ? AND DEPTCD LIKE " + DEPTCD_SEOUL_FILTER + "\n"
							+ "   	  THEN 'ADD' \n"
							+ "    WHEN (REGDT < ? OR REGDT IS NULL) AND DEPTCD LIKE " + DEPTCD_SEOUL_FILTER + "\n"
							+ "   	  THEN 'MOD' \n"
							+ "    WHEN (DEPTCD NOT LIKE " + DEPTCD_SEOUL_FILTER + " AND BEFOREDEPTCD LIKE " + DEPTCD_SEOUL_FILTER + ")\n"
							+ "		  THEN 'DEL'"
							+ "	   ELSE 'MOD' END as TYPE, \n"
							+ " ALTKEYDECODE   ,LOGINID        ,\n"
							+ " USERNAME       ,DEPTCD as DEPTID ,USERID as SUSERID ,DEPTNM as DEPTNAME,\n"
							+ " '' as SPOSITION ,GRADE ,USERORDER    ,TRIM(ISDELETED) as ISDELETED ,\n" 
							+ " ISCONCURRENT   ,POSITIONDETAIL ,''  as INDT ,UPDT ,\n"
							+ " BIRTHDT        ,GENDER         ,INITDT         ,ALTKEY , '' as GPKICN ,\n"
							+ " SEMAIL         ,HOMEPAGE       ,SOFFICEPHONE   ,SMOBILEPHONE ,GRADENAME,\n"
							+ " DEPTCD1  as DEPTID2 ,DEPTCD2  as DEPTID3 ,JIKWICD1 as SPOSITION2 ,JIKWICD2 as SPOSITION3, \n" 
							+ " JIKWINM1 as POSITIONDETAIL2, JIKWINM2 as POSITIONDETAIL3 ,SOURCEORG ,INSAJIKWICD as INSASPOSITION ,INSAJIKWINM as INSAPOSITIONDETAIL,\n"
							+ " INSADEPTNM as INSADEPTNAME, '' as INSAUSERORDER ,BEFOREDEPTCD ,REGDT ,REGID ,\n"
							+ " UPDATEDT ,UPDATEID ,MOBILEYN as MOBILEUSE ,ELECMEETYN as WORKTYPE from com_user_mst_new \n" 
							+ "where UPDATEDT >= ? AND (DEPTCD LIKE " + DEPTCD_SEOUL_FILTER + " OR BEFOREDEPTCD LIKE " + DEPTCD_SEOUL_FILTER + "\n)"
							+ "      AND UPDATEID is null"
							, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			//bind param
			pstmt.setString(1, dateTime);
			pstmt.setString(2, dateTime);
			pstmt.setString(3, dateTime);
	
			//execute query
			ResultSet rs = pstmt.executeQuery();
	
			//make csv file
			if (rs != null && getDataRowCnt(rs) > 0) {
				// Create my temporary file
                Path tmpFile = Files.createTempFile(UUID.randomUUID().toString(), "csv");
                // Delete the file on exit
                tmpFile.toFile().deleteOnExit();
                // make csv
                csvWriter = new CSVWriter(tmpFile.toAbsolutePath().toString(), "UTF-8", "^", Character.MIN_VALUE);
				writeDataFile(dateTime, csvWriter, rs, JOB_TARGET_USER);
				csvWriter.close();
				//copy file
				//File dstFile = new File(CSV_BASE_FOLDER + OUT_FOLDER + "user" + dateTime + ".csv");
				//Files.move(tmpFile, dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				String dstFilePath = CSV_BASE_FOLDER + OUT_FOLDER + "user" + dateTime + ".csv";
				moveFile(tmpFile, dstFilePath);
				log.info(dstFilePath + " file transfered.");
			}
			
		} catch (Exception e) {
			if (csvWriter != null) {
				csvWriter.close();
			}
			throw new Exception(e.getMessage());
		} finally {
		}

	}

	private void processDeptInfo(String dateTime) throws Exception {
		//TYPE^ORGID^ORGNAME^PARENTORGID^ORGTYPE^ORGORDER^TOPORGID^DISPLAYNAME^WHENDELETED^ISDELETED^UPDT^DOCDEPTID^ORGDEPTH^BOXFLAG^FIREFLAG^SDESCRIPTION^REPREORGID^SOURCEORG
		CSVWriter csvWriter = null;
		
		try {
			//prepare sql statement
			PreparedStatement pstmt = dbConn
					.prepareStatement("select * from (\n"
							+ "  select * FROM (\n"
							+ "    select CASE\n"
							+ "      WHEN REGDT IS NOT NULL AND CONFIRMYN = 'Y'\n"
							+ "   	  THEN 'ADD'\n"
							+ "      WHEN (REGDT < ? OR REGDT IS NULL) AND (CONFIRMYN is null OR CONFIRMYN = 'N')\n"
							+ "   	  THEN 'MOD'\n"
							+ "      ELSE '' END as TYPE,\n"
							+ "    DEPTCD as ORGID, \n"
							+ "    DEPTNM        as ORGNAME,\n"
							+ "    PARENTDEPTCD  as PARENTORGID,\n"
							+ "    DEPTTYPE      as ORGTYPE,\n"
							+ "    DEPTORDER     as ORGORDER,\n"
							+ "    TOPDEPTCD     as TOPORGID,\n"
							+ "    DISPLAYNAME   as DISPLAYNAME,\n"
							+ "    WHENDELETED   as WHENDELETED,\n"
							+ "    ISDELETED     as ISDELETED,\n"
							+ "    UPDT          as UPDT,\n"
							+ "    ''            as DOCDEPTID,\n"
							+ "    LEVEL-1       as ORGDEPTH ,\n"
							+ "    ARISUFLAG     as BOXFLAG,\n"
							+ "    FIREFLAG      as FIREFLAG,\n"
							+ "    TRIM(SYS_CONNECT_BY_PATH(DEPTCD,' ')) as SDESCRIPTION,\n"
							+ "    REPREDEPTCD   as REPREORGID,\n"
							+ "    SOURCEORG     as SOURCEORG,\n"
							+ "    REGDT       ,\n"
							+ "    REGID       ,\n"
							+ "    UPDATEDT    ,\n"
							+ "    UPDATEID     \n"
							+ "    from com_org_mst_new\n"
							+ "    where ISDELETED = '0'\n"
							+ "      start with DEPTCD = '" + DEPTCD_SEOUL + "10000'\n"
							+ "      connect by prior DEPTCD = PARENTDEPTCD) insa\n"
							+ "  where insa.TYPE is not null\n"
							+ "UNION ALL \n"
							+ "  select 'DEL'  as TYPE,\n"
							+ "  DEPTCD as ORGID, \n"
							+ "  DEPTNM        as ORGNAME,\n"
							+ "  PARENTDEPTCD  as PARENTORGID,\n"
							+ "  DEPTTYPE      as ORGTYPE,\n"
							+ "  DEPTORDER     as ORGORDER,\n"
							+ "  TOPDEPTCD     as TOPORGID,\n"
							+ "  DISPLAYNAME   as DISPLAYNAME,\n"
							+ "  WHENDELETED   as WHENDELETED,\n"
							+ "  ISDELETED     as ISDELETED,\n"
							+ "  UPDT          as UPDT,\n"
							+ "  ''            as DOCDEPTID,\n"
							+ "  -1            as ORGDEPTH ,\n"
							+ "  ARISUFLAG     as BOXFLAG,\n"
							+ "  FIREFLAG      as FIREFLAG,\n"
							+ "  ''            as SDESCRIPTION,\n"
							+ "  REPREDEPTCD   as REPREORGID,\n"
							+ "  SOURCEORG     as SOURCEORG,\n"
							+ "  REGDT       ,\n"
							+ "  REGID       ,\n"
							+ "  UPDATEDT    ,\n"
							+ "  UPDATEID     \n"
							+ "  from com_org_mst_new\n"
							+ "  where ISDELETED <> '0' AND DEPTCD LIKE " + DEPTCD_SEOUL_FILTER + "\n"
							+ ") where UPDATEDT >= ? AND UPDATEID is null"
							, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			//bind param
			pstmt.setString(1, dateTime);
			pstmt.setString(2, dateTime);
	
			//execute query
			ResultSet rs = pstmt.executeQuery();
			
			//make csv file
			if (rs != null && getDataRowCnt(rs) > 0) {
				// Create my temporary file
                Path tmpFile = Files.createTempFile(UUID.randomUUID().toString(), "csv");
                // Delete the file on exit
                tmpFile.toFile().deleteOnExit();
                // make csv
				csvWriter = new CSVWriter(tmpFile.toAbsolutePath().toString(), "UTF-8", "^", Character.MIN_VALUE);
				writeDataFile(dateTime, csvWriter, rs, JOB_TARGET_ORG);
				csvWriter.close();
				
				//copy file
				//File dstFile = new File(CSV_BASE_FOLDER + OUT_FOLDER + "org" + dateTime + ".csv");
				//Files.move(tmpFile, dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				String dstFilePath = CSV_BASE_FOLDER + OUT_FOLDER + "org" + dateTime + ".csv";
				moveFile(tmpFile, dstFilePath);
				log.info(dstFilePath + " file transfered.");
			}

		} catch (Exception e) {
			if (csvWriter != null) {
				csvWriter.close();
			}
			throw new Exception(e.getMessage());
		} finally {
		}

	}

}
