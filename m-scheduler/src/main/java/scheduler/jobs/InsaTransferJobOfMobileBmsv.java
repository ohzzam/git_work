package scheduler.jobs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.mybatis.spring.SqlSessionTemplate;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ndata.handler.CSVHandler.CSVWriter;

import lombok.extern.slf4j.Slf4j;
import scheduler.model.JobStatusVo;
import scheduler.service.JobService;

@Slf4j
@Component("insaTransferJobOfMobileBmsv")
public class InsaTransferJobOfMobileBmsv extends JobService {
	private final static String OUT_FOLDER_MOBILE = "/mobile/";
	private final static String OUT_FOLDER_SDRIVE = "/sdrive/";
	private final static String BEFORE_JOB_NAME = "insaUpdateJobBmsv";
	
	@Autowired
	private SqlSessionTemplate sqlSessionTemplate;	// transaction 사용 안할 경우 사용
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		//Date prevJobFireTime = context.getPreviousFireTime();
		Date jobFireTime = context.getFireTime();
		log.info("> insaTransferJobOfMobileBmsv Execution Time: " + DateFormatUtils.format(jobFireTime, "yyyy-MM-dd HH:mm:ss"));

		Trigger trigger = context.getTrigger();
		JobStatusVo jobStatus = new JobStatusVo(trigger.getJobKey().getName(), 
				trigger.getJobKey().getGroup(), null, null, null);

		try {
			connectDB();
			
			// 사전작업이 끝날때까지 대기한다.
			waitBeforeJobFinished(BEFORE_JOB_NAME, sqlSessionTemplate);
						
			// 마지막 작업 실행 결과를 조회한다.
			jobStatus = sqlSessionTemplate.selectOne("jobtrigger.getJobStatus", jobStatus);
			if (jobStatus == null) {
				jobStatus = new JobStatusVo(trigger.getJobKey().getName(), 
								trigger.getJobKey().getGroup(), jobFireTime, jobFireTime, null);
			} else if (jobStatus.getLastSuccessTime() == null) {
				jobStatus.setLastSuccessTime(jobFireTime);
			}

			log.info(">> insaTransferJobOfMobileBmsv - Data Retrieve Time: "
							+ DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyy-MM-dd HH:mm:ss"));
					
			processUserInfo(DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyyMMddHHmmss"));
			processDeptInfoForMobile(DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyyMMddHHmmss"));
			processDeptInfoForSdrive(DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyyMMddHHmmss"));
			
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
							+ "    WHEN REGDT > ? AND ISDELETED != '2'\n"
							+ "   	  THEN 'ADD' \n"
							+ "    WHEN (REGDT < ? OR REGDT IS NULL) AND ISDELETED != '2'\n"
							+ "   	  THEN 'MOD' \n"
							+ "    WHEN ISDELETED = '2'\n"
							+ "		  THEN 'DEL'"
							+ "	   ELSE 'MOD' END as TYPE, \n"
							+ "  ALTKEYDECODE   ,LOGINID        ,\n"
							+ "  USERNAME       ,DEPTCD as DEPTID ,USERID as SUSERID ,DEPTNM as DEPTNAME,\n"
							+ "  SPOSITION ,GRADE ,USERORDER    ,TRIM(ISDELETED) as ISDELETED ,\n" 
							+ "  ISCONCURRENT   ,POSITIONDETAIL ,'' as INDT ,UPDT ,\n"
							+ "  BIRTHDT        ,GENDER         ,INITDT         ,ALTKEY , '' as GPKICN ,\n"
							+ "  SEMAIL         ,HOMEPAGE       ,SOFFICEPHONE   ,SMOBILEPHONE ,GRADENAME,\n"
							+ "  DEPTCD1  as DEPTID2 ,DEPTCD2  as DEPTID3 ,JIKWICD1 as SPOSITION2 ,JIKWICD2 as SPOSITION3, \n" 
							+ "  JIKWINM1 as POSITIONDETAIL2, JIKWINM2 as POSITIONDETAIL3 ,SOURCEORG ,INSAJIKWICD as INSASPOSITION ,INSAJIKWINM as INSAPOSITIONDETAIL,\n"
							+ "  INSADEPTNM as INSADEPTNAME, '' as INSAUSERORDER ,BEFOREDEPTCD ,REGDT ,REGID ,\n"
							+ "  UPDATEDT ,UPDATEID ,MOBILEYN as MOBILEUSE ,ELECMEETYN as WORKTYPE from " + MST_USER_TABLE + "\n" 
							+ " where UPDATEDT >= ? \n"
							//+ "    AND UPDATEID is null"
							, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			//bind param
			pstmt.setString(1, dateTime);
			pstmt.setString(2, dateTime);
			pstmt.setString(3, dateTime);
	
			//execute query
			ResultSet rs = pstmt.executeQuery();
	
			//make csv file
			String filePathMobile = CSV_BASE_FOLDER_MOBILE + OUT_FOLDER_MOBILE + "user" + dateTime + "_" + ORG_CD + ".csv";
			String filePathSdrive = CSV_BASE_FOLDER + OUT_FOLDER_SDRIVE + "user" + dateTime + "_" + ORG_CD + ".csv";
			if (rs != null && getDataRowCnt(rs) > 0) {
				//1.make csv for mobile
				// Create my temporary file
                Path tmpMobileFile = Files.createTempFile(UUID.randomUUID().toString(), "csv");
                // Delete the file on exit
                tmpMobileFile.toFile().deleteOnExit();
                // write data to tmpFile
				csvWriter = new CSVWriter(tmpMobileFile.toAbsolutePath().toString(), "UTF-8", "^", Character.MIN_VALUE);
				writeDataFileForMobile(dateTime, csvWriter, rs, JOB_TARGET_USER);
				csvWriter.close();
				
				// copy file to mobile
				moveFile(tmpMobileFile, filePathMobile);
				log.info(filePathMobile + " file transfered.");
				
				//2.make csv for sdrive
				rs.beforeFirst(); //커서를 맨앞으로 이동한다.
				// Create my temporary file
                Path tmpSdriveFile = Files.createTempFile(UUID.randomUUID().toString(), "csv");
                // Delete the file on exit
                tmpSdriveFile.toFile().deleteOnExit();
                // write data to tmpFile
				csvWriter = new CSVWriter(tmpSdriveFile.toAbsolutePath().toString(), "UTF-8", "^", Character.MIN_VALUE);
				writeDataFile(dateTime, csvWriter, rs, JOB_TARGET_USER, false);
				csvWriter.close();
				
				//copy file to sdrive
				moveFile(tmpSdriveFile, filePathSdrive);
				log.info(filePathSdrive + " file transfered.");
			}

		} catch (Exception e) {
			if (csvWriter != null) {
				csvWriter.close();
			}
			throw new Exception(e.getMessage());
		} finally {
		}

	}

	private void processDeptInfoForMobile(String dateTime) throws Exception {
		//TYPE^ORGID^ORGNAME^PARENTORGID^ORGTYPE^ORGORDER^TOPORGID^DISPLAYNAME^WHENDELETED^ISDELETED^UPDT^DOCDEPTID^ORGDEPTH^BOXFLAG^FIREFLAG^SDESCRIPTION^REPREORGID^SOURCEORG
		CSVWriter csvWriter = null;
		
		try {
			//prepare sql statement
			PreparedStatement pstmt = dbConn
					.prepareStatement("select CASE\n"
							+ "      WHEN REGDT > ?\n"
							+ "   	  THEN 'ADD'\n"
							+ "      WHEN REGDT < ? OR REGDT IS NULL\n"
							+ "   	  THEN 'MOD'\n"
							+ "      ELSE 'MOD' END as TYPE,\n"
							+ "    DEPTCD as ORGID, \n"
							+ "    DEPTNM        as ORGNAME,\n"
							+ "    PARENTDEPTCD  as PARENTORGID,\n"
							+ "    ''            as ORGTYPE,\n"
							+ "    DEPTORDER     as ORGORDER,\n"
							+ "    TOPDEPTCD     as TOPORGID,\n"
							+ "    DISPLAYNAME   as DISPLAYNAME,\n"
							+ "    WHENDELETED   as WHENDELETED,\n"
							+ "    ISDELETED,\n"
							+ "    UPDT          as UPDT,\n"
							+ "    ''            as DOCDEPTID,\n"
							+ "    DEPTDEPTH     as ORGDEPTH ,\n"
							+ "    ''            as BOXFLAG,\n"
							+ "    ''            as FIREFLAG,\n"
							+ "    TRIM(SYS_CONNECT_BY_PATH(DEPTCD,' ')) as SDESCRIPTION,\n"
							+ "    REPREDEPTCD   as REPREORGID,\n"
							+ "    SOURCEORG     as SOURCEORG,\n"
							+ "    REGDT       ,\n"
							+ "    REGID       ,\n"
							+ "    UPDATEDT    ,\n"
							+ "    UPDATEID     \n"
							+ "  from " + MST_ORG_TABLE + "\n"
							+ "    where ISDELETED = '0' AND UPDATEDT >= ?\n" // AND UPDATEID is null\n"
							+ "      start with DEPTCD = '" + ORG_CD + "0000'\n"
							+ "      connect by prior DEPTCD = PARENTDEPTCD"
							, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			//bind param
			pstmt.setString(1, dateTime);
			pstmt.setString(2, dateTime);
			pstmt.setString(3, dateTime);
	
			//execute query
			ResultSet rs = pstmt.executeQuery();
			
			//make csv file
			String filePathMobile = CSV_BASE_FOLDER_MOBILE + OUT_FOLDER_MOBILE + "org" + dateTime + "_" + ORG_CD + ".csv";
			//String filePathSdrive = CSV_BASE_FOLDER + OUT_FOLDER_SDRIVE + "org" + dateTime + "_" + ORG_CD + ".csv";
			if (rs != null && getDataRowCnt(rs) > 0) {
				//1.make csv for mobile
				Path tmpMobileFile = Files.createTempFile(UUID.randomUUID().toString(), "csv");
                // Delete the file on exit
                tmpMobileFile.toFile().deleteOnExit();
                // write data to tmpFile
				csvWriter = new CSVWriter(tmpMobileFile.toAbsolutePath().toString(), "UTF-8", "^", Character.MIN_VALUE);
				writeDataFile(dateTime, csvWriter, rs, JOB_TARGET_ORG);
				csvWriter.close();
				
				// copy file to mobile
				moveFile(tmpMobileFile, filePathMobile);
				log.info(filePathMobile + " file transfered.");
			}

		} catch (Exception e) {
			if (csvWriter != null) {
				csvWriter.close();
			}
			throw new Exception(e.getMessage());
		} finally {
		}

	}
	
	private void processDeptInfoForSdrive(String dateTime) throws Exception {
		//TYPE^ORGID^ORGNAME^PARENTORGID^ORGTYPE^ORGORDER^TOPORGID^DISPLAYNAME^WHENDELETED^ISDELETED^UPDT^DOCDEPTID^ORGDEPTH^BOXFLAG^FIREFLAG^SDESCRIPTION^REPREORGID^SOURCEORG
		CSVWriter csvWriter = null;
		
		try {
			//prepare sql statement
			PreparedStatement pstmt = dbConn
					.prepareStatement("select CASE\n"
							+ "      WHEN REGDT > ?\n"
							+ "   	  THEN 'ADD'\n"
							+ "      WHEN REGDT < ? OR REGDT IS NULL\n"
							+ "   	  THEN 'MOD'\n"
							+ "      ELSE 'MOD' END as TYPE,\n"
							+ "    DEPTCD as ORGID, \n"
							+ "    DEPTNM        as ORGNAME,\n"
							+ "    PARENTDEPTCD  as PARENTORGID,\n"
							+ "    ''            as ORGTYPE,\n"
							+ "    DEPTORDER     as ORGORDER,\n"
							+ "    TOPDEPTCD     as TOPORGID,\n"
							+ "    DISPLAYNAME   as DISPLAYNAME,\n"
							+ "    WHENDELETED   as WHENDELETED,\n"
							+ "    ISDELETED,\n"
							+ "    UPDT          as UPDT,\n"
							+ "    ''            as DOCDEPTID,\n"
							+ "    DEPTDEPTH+1   as ORGDEPTH ,\n"
							+ "    ''            as BOXFLAG,\n"
							+ "    ''            as FIREFLAG,\n"
							+ "    '6110000 ' || TRIM(SYS_CONNECT_BY_PATH(DEPTCD,' ')) as SDESCRIPTION,\n"
							+ "    REPREDEPTCD   as REPREORGID,\n"
							+ "    SOURCEORG     as SOURCEORG,\n"
							+ "    REGDT       ,\n"
							+ "    REGID       ,\n"
							+ "    UPDATEDT    ,\n"
							+ "    UPDATEID     \n"
							+ "  from " + MST_ORG_TABLE + "\n"
							+ "    where ISDELETED = '0' AND UPDATEDT >= ?\n" // AND UPDATEID is null\n"
							+ "      start with DEPTCD = '" + ORG_CD + "0000'\n"
							+ "      connect by prior DEPTCD = PARENTDEPTCD"
							, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			//bind param
			pstmt.setString(1, dateTime);
			pstmt.setString(2, dateTime);
			pstmt.setString(3, dateTime);
	
			//execute query
			ResultSet rs = pstmt.executeQuery();
			
			//make csv file
			//String filePathMobile = CSV_BASE_FOLDER_MOBILE + OUT_FOLDER_MOBILE + "org" + dateTime + "_" + ORG_CD + ".csv";
			String filePathSdrive = CSV_BASE_FOLDER + OUT_FOLDER_SDRIVE + "org" + dateTime + "_" + ORG_CD + ".csv";
			if (rs != null && getDataRowCnt(rs) > 0) {
				//2.make csv for sdrive
				rs.beforeFirst(); //커서를 맨앞으로 이동한다.
				// Create my temporary file
                Path tmpSdriveFile = Files.createTempFile(UUID.randomUUID().toString(), "csv");
                // Delete the file on exit
                tmpSdriveFile.toFile().deleteOnExit();
                // write data to tmpFile
				csvWriter = new CSVWriter(tmpSdriveFile.toAbsolutePath().toString(), "UTF-8", "^", Character.MIN_VALUE);
				writeDataFile(dateTime, csvWriter, rs, JOB_TARGET_ORG, false);
				csvWriter.close();
				
				//copy file to sdrive
				moveFile(tmpSdriveFile, filePathSdrive);
				log.info(filePathSdrive + " file transfered.");
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
