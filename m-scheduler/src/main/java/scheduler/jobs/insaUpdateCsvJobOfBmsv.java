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

import com.ndata.handler.CSVHandler.CSVReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Slf4j
@Component("insaUpdateCsvJobOfBmsv")
public class insaUpdateCsvJobOfBmsv extends JobService {
	private static String IN_FOLDER = null;
	private static String MOVE_FOLDER = null;
	private static String ERROR_FOLDER = null;

	@Autowired
	private SqlSessionTemplate sqlSessionTemplate;	// transaction 사용 안할 경우 사용
	
	public insaUpdateCsvJobOfBmsv() {
		//get run envs
		//getRunEnvs();
		// 파일 폴더 설정
		IN_FOLDER = "/" + ORG_CD + "0000/send/";
		MOVE_FOLDER = "/" + ORG_CD + "0000/backup/send/";
		ERROR_FOLDER = "/" + ORG_CD + "0000/send/errorFile/";
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		Date jobFireTime = context.getFireTime();
		log.info("> insaUpdateCsvJobOfBmsv Execution Time: " + DateFormatUtils.format(jobFireTime, "yyyy-MM-dd HH:mm:ss"));

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

			log.info(">> insaUpdateCsvJobOfBmsv - Data Retrieve Time: "
							+ DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyy-MM-dd HH:mm:ss"));

			String lastSuccessTime = DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyyMMddHHmmss");
			
			long loopCnt = 0;
			//set backup path
			String sYear = DateFormatUtils.format(jobStatus.getLastSuccessTime(), "yyyy/");
			String sMonth = DateFormatUtils.format(jobStatus.getLastSuccessTime(), "MM/");
			String sDay = DateFormatUtils.format(jobStatus.getLastSuccessTime(), "dd/");
			String backupPath = CSV_BASE_FOLDER + MOVE_FOLDER + sYear + sMonth + sDay;
			String errorPath = CSV_BASE_FOLDER + ERROR_FOLDER;
			//get csv file paths
			List<Path> filePaths = listFilesOldestFirst(CSV_BASE_FOLDER + IN_FOLDER);
			
			for (Path filePath : filePaths) {
				String csvFileAbsolutePath = filePath.toAbsolutePath().toString();
				String csvFileName = csvFileAbsolutePath
						.substring(csvFileAbsolutePath.lastIndexOf(File.separatorChar) + 1);

				if (csvFileAbsolutePath != null) {
					try {
						//파일명으로 user or org 처리 구분
						if (csvFileAbsolutePath.indexOf("user") != -1) {
							loopCnt++;
							processUserInfo(lastSuccessTime, csvFileAbsolutePath);
							log.info(loopCnt + "> " + csvFileAbsolutePath + " successfully processed.");
							//move file to backup path
							moveFile(filePath, backupPath + csvFileName);
						} else if (csvFileAbsolutePath.indexOf("org") != -1) {
							loopCnt++;
							processDeptInfo(lastSuccessTime, csvFileAbsolutePath);
							log.info(loopCnt + "> " + csvFileAbsolutePath + " successfully processed.");
							//move file to backup path
							moveFile(filePath, backupPath + csvFileName);
						}
						//commit 한다.
						dbConn.commit();
					} catch (Exception e) {
						log.error(loopCnt + "> " + csvFileAbsolutePath + " failed to process.", e);
						//move file to error file path
						moveFile(filePath, errorPath + csvFileName);
						//rollback 한다.
						try {
							dbConn.rollback();
						} catch (SQLException e1) {
							// TODO Auto-generated catch block
							log.error(e1.getMessage());
						}
					}
				}
			}
					
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
	
	private void processUserInfo(String dateTime, String filePath) throws Exception {
		// read csv
		CSVReader csvReader = new CSVReader(filePath, "UTF-8", "^", Character.MIN_VALUE);
		List<Map<String,String>> csvDatas = csvReader.readAllWithHeader();
				
		for (Map<String,String> csvData : csvDatas) {
			String type = csvData.get("TYPE");
			switch(type) {
				case "ADD":
					mergeUserInfo(dateTime, csvData);
					break;
				case "MOD":
					updateUserInfo(dateTime, csvData);
					break;
				case "DEL":
					updateUserInfo(dateTime, csvData);
					break;
			}
		}
		csvReader.close();
	}

	private void processDeptInfo(String dateTime, String filePath) throws Exception {
		// read csv
		CSVReader csvReader = new CSVReader(filePath, "UTF-8", "^", Character.MIN_VALUE);
		List<Map<String,String>> csvDatas = csvReader.readAllWithHeader();
		
		for (Map<String,String> csvData : csvDatas) {
			String type = csvData.get("TYPE");//csvData[0]; 
			switch(type) {
				case "ADD":
					mergeDeptInfo(dateTime, csvData);
					break;
				case "MOD":
					updateDeptInfo(dateTime, csvData);
					break;
				case "DEL":
					updateDeptInfo(dateTime, csvData);
					break;
			}
		}
		csvReader.close();
	}
	
	private void mergeUserInfo(String dateTime, Map<String,String> csvData) throws Exception {
		String sql = "MERGE INTO " + MST_USER_TABLE + " mst USING dual\n"
		             + "    ON (mst.ALTKEY = ?)\n"
				     + "  WHEN MATCHED THEN\n"
		             + "    UPDATE SET\n"
		             + "      mst.USERNAME = ?,\n"
		             + "      mst.DEPTCD = ?,\n"
		             + "      mst.USERID = ?,\n"
		             + "      mst.DEPTNM = ?,\n"
		             + "      mst.SPOSITION = ?,\n"
		             + "      mst.POSITIONDETAIL = ?,\n"
		             + "      mst.GRADE = ?,\n"
		             + "      mst.GRADENAME = ?,\n"
		             + "      mst.USERORDER = ?,\n"
		             + "      mst.ISDELETED = ?,\n"
		             + "      mst.ISCONCURRENT = ?,\n"
		             + "      mst.SEMAIL = ?,\n"
		             + "      mst.HOMEPAGE = ?,\n"
		             + "      mst.SOFFICEPHONE = ?,\n"
		             + "      mst.SMOBILEPHONE = ?,\n"
		             + "      mst.SOURCEORG = ?,\n"
		             + "      mst.MOBILEYN = ?,\n"
		             + "      mst.ELECMEETYN = ?,\n"
		             + "      mst.UPDATEDT = ?,\n"
		             + "      mst.UPDATEID = ?\n"
		             + "  WHEN NOT MATCHED THEN\n"
		             + "    INSERT (\n"
		             + "      mst.ALTKEYDECODE,\n"
		             + "      mst.LOGINID,\n"
		             + "      mst.USERNAME,\n"
		             + "      mst.DEPTCD,\n"
		             + "      mst.USERID,\n"
		             + "      mst.DEPTNM,\n"
		             + "      mst.SPOSITION,\n"
		             + "      mst.GRADE,\n"
		             + "      mst.USERORDER,\n"
		             + "      mst.ISDELETED,\n"
		             + "      mst.ISCONCURRENT,\n"
		             + "      mst.POSITIONDETAIL,\n"
		             + "      mst.UPDT,\n"
		             + "      mst.BIRTHDT,\n"
		             + "      mst.GENDER,\n"
		             + "      mst.INITDT,\n"
		             + "      mst.ALTKEY,\n"
		             + "      mst.SEMAIL,\n"
		             + "      mst.HOMEPAGE,\n"
		             + "      mst.SOFFICEPHONE,\n"
		             + "      mst.SMOBILEPHONE,\n"
		             + "      mst.GRADENAME,\n"
		             + "      mst.DEPTCD1,\n"
		             + "      mst.DEPTCD2,\n"
		             + "      mst.JIKWICD1,\n"
		             + "      mst.JIKWICD2,\n"
		             + "      mst.JIKWINM1,\n"
		             + "      mst.JIKWINM2,\n"
		             + "      mst.SOURCEORG,\n"
		             + "      mst.INSAJIKWICD,\n"
		             + "      mst.INSAJIKWINM,\n"
		             + "      mst.INSADEPTNM,\n"
		             + "      mst.BEFOREDEPTCD,\n"
		             + "      mst.REGDT,\n"
		             + "      mst.REGID,\n"
		             + "      mst.UPDATEDT,\n"
		             + "      mst.UPDATEID,\n"
		             + "      mst.MOBILEYN,\n"
		             + "      mst.ELECMEETYN) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pstmt = dbConn.prepareStatement(sql);
				
		//StringBuilder updateData = new StringBuilder();
		String updateDt = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
		String altKey = csvData.get("ALTKEY");
		//updateData.append("ALTKEY=" + altKey + ",");
		
		//bind columns
		pstmt.setString(1, altKey);
		pstmt.setString(2, csvData.get("USERNAME"));
		pstmt.setString(3, csvData.get("DEPTID"));
		pstmt.setString(4, csvData.get("SUSERID"));
		pstmt.setString(5, csvData.get("DEPTNAME"));
		pstmt.setString(6, csvData.get("SPOSITION"));
		pstmt.setString(7, csvData.get("POSITIONDETAIL"));
		pstmt.setString(8, csvData.get("GRADE"));
		pstmt.setString(9, csvData.get("GRADENAME"));
		pstmt.setString(10, csvData.get("USERORDER"));
		pstmt.setString(11, csvData.get("ISDELETED"));
		pstmt.setString(12, csvData.get("ISCONCURRENT"));
		pstmt.setString(13, csvData.get("SEMAIL"));
		pstmt.setString(14, csvData.get("HOMEPAGE"));
		pstmt.setString(15, csvData.get("SOFFICEPHONE"));
		pstmt.setString(16, csvData.get("SMOBILEPHONE"));
		pstmt.setString(17, csvData.get("SOURCEORG"));
		pstmt.setString(18, csvData.get("MOBILEUSE"));
		pstmt.setString(19, csvData.get("WORKTYPE"));
		pstmt.setString(20, updateDt);
		pstmt.setString(21, "csv");
		pstmt.setString(22, csvData.get("ALTKEYDECODE"));
		pstmt.setString(23, csvData.get("LOGINID"));
		pstmt.setString(24, csvData.get("USERNAME"));
		pstmt.setString(25, csvData.get("DEPTID"));
		pstmt.setString(26, csvData.get("SUSERID"));
		pstmt.setString(27, csvData.get("DEPTNAME"));
		pstmt.setString(28, csvData.get("SPOSITION"));
		pstmt.setString(29, csvData.get("GRADE"));
		pstmt.setString(30, csvData.get("USERORDER"));
		pstmt.setString(31, csvData.get("ISDELETED"));
		pstmt.setString(32, csvData.get("ISCONCURRENT"));
		pstmt.setString(33, csvData.get("POSITIONDETAIL"));
		pstmt.setString(34, csvData.get("UPDT"));
		pstmt.setString(35, csvData.get("BIRTHDT"));
		pstmt.setString(36, csvData.get("GENDER"));
		pstmt.setString(37, csvData.get("INITDT"));
		pstmt.setString(38, csvData.get("ALTKEY"));
		pstmt.setString(39, csvData.get("SEMAIL"));
		pstmt.setString(40, csvData.get("HOMEPAGE"));
		pstmt.setString(41, csvData.get("SOFFICEPHONE"));
		pstmt.setString(42, csvData.get("SMOBILEPHONE"));
		pstmt.setString(43, csvData.get("GRADENAME"));
		pstmt.setString(44, csvData.get("DEPTID2"));
		pstmt.setString(45, csvData.get("DEPTID3"));
		pstmt.setString(46, csvData.get("SPOSITION2"));
		pstmt.setString(47, csvData.get("SPOSITION3"));
		pstmt.setString(48, csvData.get("POSITIONDETAIL2"));
		pstmt.setString(49, csvData.get("POSITIONDETAIL3"));
		pstmt.setString(50, csvData.get("SOURCEORG"));
		pstmt.setString(51, csvData.get("INSASPOSITION"));
		pstmt.setString(52, csvData.get("INSAPOSITIONDETAIL"));
		pstmt.setString(53, csvData.get("INSADEPTNAME"));
		pstmt.setString(54, csvData.get("BEFOREDEPTCD"));
		pstmt.setString(55, updateDt);
		pstmt.setString(56, csvData.get("REGID"));
		pstmt.setString(57, updateDt);
		pstmt.setString(58, "csv");
		pstmt.setString(59, csvData.get("MOBILEUSE"));
		pstmt.setString(60, csvData.get("WORKTYPE"));
		
		//
		pstmt.execute();
		pstmt.close();
		//add update log
		addUpdateLog(dateTime, updateDt, altKey, JOB_TARGET_USER, JOB_TYPE_ADD, csvData.toString());
	}
	
	private void updateUserInfo(String dateTime, Map<String,String> csvData) throws Exception {
		String updateDt = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
		String altKey = csvData.get("ALTKEY");
		csvData.put("UPDATEDT", updateDt);
		csvData.put("UPDATEID", "csv");
		
		PreparedStatement pstmt = null;
		
		List<String[]> colNames = Arrays.asList(
			    new String [] {"USERNAME",       "USERNAME"       },
			    new String [] {"DEPTCD",         "DEPTID"         },
			    new String [] {"USERID",         "SUSERID"        },
			    new String [] {"DEPTNM",         "DEPTNAME"       },
			    new String [] {"SPOSITION",      "SPOSITION"      },
			    new String [] {"POSITIONDETAIL", "POSITIONDETAIL" },
			    new String [] {"GRADE",          "GRADE"          },
			    new String [] {"GRADENAME",      "GRADENAME"      },
			    new String [] {"USERORDER",      "USERORDER"      },
			    new String [] {"ISDELETED",      "ISDELETED"      },
			    new String [] {"ISCONCURRENT",   "ISCONCURRENT"   },
			    new String [] {"SEMAIL",         "SEMAIL"         },
			    new String [] {"HOMEPAGE",       "HOMEPAGE"       },
			    new String [] {"SOFFICEPHONE",   "SOFFICEPHONE"   },
			    new String [] {"SMOBILEPHONE",   "SMOBILEPHONE"   },
			    new String [] {"SOURCEORG",      "SOURCEORG"      },
			    new String [] {"MOBILEYN",       "MOBILEUSE"      },
			    new String [] {"ELECMEETYN",     "WORKTYPE"       },
			    new String [] {"UPDATEDT",       "UPDATEDT"       },
			    new String [] {"UPDATEID",       "UPDATEID"       }
		    );

		String sql = makeUpdateFromCsvSql(MST_USER_TABLE, colNames, csvData);
		sql += " where ALTKEY = ?";
		//log.info(">>MOD_FROM_CSV_SQL=[{}]", sql);
			
		pstmt = dbConn.prepareStatement(sql);		
		
		//bind columns
		int idx = 0;
		for (String[] colName : colNames) {
			String colData = csvData.get(colName[1]);
			if (colData != null && !"".equals(colData)) {
				pstmt.setString(++idx, colData);
			}
		}

		//
		pstmt.setString(idx+1, altKey);
		pstmt.execute();
		pstmt.close();
		//add update log
		addUpdateLog(dateTime, updateDt, altKey, JOB_TARGET_USER, JOB_TYPE_MOD, csvData.toString());
	}
	
	private void delUserInfo(String dateTime, String[] csvData) throws Exception {
	}

	private void mergeDeptInfo(String dateTime, Map<String,String> csvData) throws Exception {
		String updateDt = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
		String deptCd = csvData.get("ORGID");
		
		String sql = "MERGE INTO " + MST_ORG_TABLE + " mst USING dual\n"
	             + "    ON (mst.DEPTCD = ?)\n"
			     + "  WHEN MATCHED THEN\n"
	             + "    UPDATE SET\n"
	             + "      mst.DEPTNM = ?,\n"
	             + "      mst.PARENTDEPTCD = ?,\n"
	             + "      mst.DEPTTYPE = ?,\n"
	             + "      mst.DEPTORDER = ?,\n"
	             + "      mst.WHENDELETED = ?,\n"
	             + "      mst.ISDELETED = ?,\n"
	             + "      mst.DEPTDEPTH = ?,\n"
	             + "      mst.FULLDEPTCD = ?,\n"
	             + "      mst.UPDATEDT = ?,\n"
	             + "      mst.UPDATEID = ?\n"
	             + "  WHEN NOT MATCHED THEN\n"
	             + "    INSERT (\n"
	             + "      mst.DEPTCD,\n"
	             + "      mst.DEPTNM,\n"
	             + "      mst.PARENTDEPTCD,\n"
	             + "      mst.DEPTTYPE,\n"
	             + "      mst.DEPTORDER,\n"
	             + "      mst.TOPDEPTCD,\n"
	             + "      mst.DISPLAYNAME,\n"
	             + "      mst.WHENDELETED,\n"
	             + "      mst.ISDELETED,\n"
	             + "      mst.UPDT,\n"
	             + "      mst.DEPTDEPTH,\n"
	             + "      mst.ARISUFLAG,\n"
	             + "      mst.FIREFLAG,\n"
	             + "      mst.FULLDEPTCD,\n"
	             + "      mst.REPREDEPTCD,\n"
	             + "      mst.SOURCEORG,\n"
	             + "      mst.REGDT,\n"
	             + "      mst.REGID,\n"
	             + "      mst.UPDATEDT,\n"
	             + "      mst.UPDATEID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pstmt = dbConn.prepareStatement(sql);
		
		pstmt.setString(1, deptCd);
		//update
		pstmt.setString(2, csvData.get("ORGNAME"));
		pstmt.setString(3, csvData.get("PARENTORGID"));
		pstmt.setString(4, csvData.get("ORGTYPE"));
		pstmt.setString(5, csvData.get("ORGORDER"));
		pstmt.setString(6, csvData.get("WHENDELETED"));
		pstmt.setString(7, csvData.get("ISDELETED"));
		pstmt.setString(8, csvData.get("ORGDEPTH"));
		pstmt.setString(9, csvData.get("SDESCRIPTION"));
		pstmt.setString(10, updateDt);
		pstmt.setString(11, "csv");
		//insert
		pstmt.setString(12, deptCd);
		pstmt.setString(13, csvData.get("ORGNAME"));
		pstmt.setString(14, csvData.get("PARENTORGID"));
		pstmt.setString(15, csvData.get("ORGTYPE"));
		pstmt.setString(16, csvData.get("ORGORDER"));
		pstmt.setString(17, csvData.get("TOPORGID"));
		pstmt.setString(18, csvData.get("DISPLAYNAME"));
		pstmt.setString(19, csvData.get("WHENDELETED"));
		pstmt.setString(20, csvData.get("ISDELETED"));
		pstmt.setString(21, csvData.get("UPDT"));
		pstmt.setString(22, csvData.get("ORGDEPTH"));
		pstmt.setString(23, csvData.get("BOXFLAG"));
		pstmt.setString(24, csvData.get("FIREFLAG"));
		pstmt.setString(25, csvData.get("SDESCRIPTION"));
		pstmt.setString(26, csvData.get("REPREORGID"));
		pstmt.setString(27, csvData.get("SOURCEORG"));
		pstmt.setString(28, updateDt);
		pstmt.setString(29, csvData.get("REGID"));
		pstmt.setString(30, updateDt);
		pstmt.setString(31, "csv");
		
		//
		pstmt.execute();
		pstmt.close();
		//add update log
		addUpdateLog(dateTime, updateDt, deptCd, JOB_TARGET_ORG, JOB_TYPE_MOD, csvData.toString());
	}
	
	private void updateDeptInfo(String dateTime, Map<String,String> csvData) throws Exception {
		String updateDt = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
		String deptCd = csvData.get("ORGID");
		csvData.put("UPDATEDT", updateDt);
		csvData.put("UPDATEID", "csv");
		
		List<String[]> colNames = Arrays.asList(
				new String[] {"DEPTNM",      "ORGNAME"}, 
				new String[] {"PARENTDEPTCD","PARENTORGID"},
				new String[] {"DEPTTYPE",    "ORGTYPE"},
				new String[] {"DEPTORDER",   "ORGORDER"},
				new String[] {"WHENDELETED", "WHENDELETED"},
				new String[] {"ISDELETED",   "ISDELETED"},
				new String[] {"DEPTDEPTH",   "ORGDEPTH"},
				new String[] {"FULLDEPTCD",  "SDESCRIPTION"},
				new String[] {"UPDATEDT",    "UPDATEDT"},
				new String[] {"UPDATEID",    "UPDATEID"}
			);
		
		String sql = makeUpdateFromCsvSql(MST_ORG_TABLE, colNames, csvData);
		sql += " where DEPTCD = ?";
		//log.info(">>MOD_FROM_CSV_SQL=[{}]", sql);
		
		PreparedStatement pstmt = dbConn.prepareStatement(sql);
				
		//bind columns
		int idx = 0;
		for (String[] colName : colNames) {
			String colData = csvData.get(colName[1]);
			if (colData != null && !"".equals(colData)) {
				pstmt.setString(++idx, colData);
			}
		}
		
		//
		pstmt.setString(idx+1, deptCd);
		pstmt.execute();
		pstmt.close();
		//add update log
		addUpdateLog(dateTime, updateDt, deptCd, JOB_TARGET_ORG, JOB_TYPE_MOD, csvData.toString());
	}
}
