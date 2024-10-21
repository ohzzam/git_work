package scheduler.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import com.ndata.common.NDConstant;
import com.ndata.datasource.dbms.DBConnection;
import com.ndata.handler.CSVHandler;
import com.ndata.handler.CSVHandler.CSVWriter;
import com.ndata.model.DataSourceVo;
import com.ndata.module.StringUtils;
import com.ndata.property.PropertyManager;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import scheduler.model.JobStatusVo;

@Data
@Slf4j
public abstract class JobService implements BaseJob {
	
	public final static String JOB_TARGET_USER = "U"; 
	public final static String JOB_TARGET_ORG = "O"; 
	public final static String JOB_TYPE_ADD = "ADD";
	public final static String JOB_TYPE_MOD = "MOD";
	public final static String JOB_TYPE_DEL = "DEL";
	public final static String JOB_STATUS_COMPLETED = "COMPLETED";
	public final static String JOB_STATUS_EXECUTING = "EXECUTING";
	public final static String JOB_STATUS_FAILED = "FAILED";
	public final static long WAIT_TIME = 1000;
	public static String CSV_BASE_FOLDER = "/FILE_DATA/yullinsync";
	public static String CSV_BASE_FOLDER_MOBILE = "/FILE_DATA_MOBILE/yullinsync";
	public static String OS_NAME = null;
	public static String INSA_USER_TABLE = null;
	public static String INSA_ORG_TABLE = null;
	public static String MST_USER_TABLE = null;
	public static String MST_ORG_TABLE = null;
	public static String UPDATE_LOG_TABLE = null;
	public static String TRANS_LOG_TABLE = null;
	public static String ORG_CD = null;
	public static String SOURCE_ORG = null;
	private PreparedStatement updateLogStmt = null;
	private PreparedStatement transferLogStmt = null;
	
	@Value("${insa.master.dbmsTp}")
    private String mDbmsTp;
	@Value("${insa.master.svrAddr}")
    private String mSvrAddr;
	@Value("${insa.master.port}")
    private Integer mPort;
	@Value("${insa.master.userId}")
    private String mUserId;
	@Value("${insa.master.pwd}")
    private String mPwd;
	@Value("${insa.master.dbName}")
    private String mDbName;
	
	@Autowired
	private ApplicationContext appContext;
	
	public JobService() {
		OS_NAME = System.getProperty("os.name").toLowerCase();
		//get run envs
		getRunEnvs();
	}
	
	public DBConnection dbConn = null;
	
	public void connectDB() throws Exception {
		DataSourceVo dataSourceVo = new DataSourceVo();
		
		dataSourceVo.setDbmsTp(mDbmsTp);
		dataSourceVo.setSvrAddr(mSvrAddr);
		dataSourceVo.setPort(mPort);
		dataSourceVo.setUserId(mUserId);
		dataSourceVo.setPwd(mPwd);
		dataSourceVo.setDbName(mDbName);
	
		dbConn = new DBConnection(dataSourceVo, false);
		dbConn.connect();
		
		//prepare stmts for logging
		//prepareUpdateLog();
		//prepareTransLog();
	}
	
	public void getRunEnvs() {
		PropertyManager propMan = new PropertyManager("env.properties", false);
		
		// folder info
		CSV_BASE_FOLDER = propMan.getConfigValue("baseFolder");
		CSV_BASE_FOLDER_MOBILE = propMan.getConfigValue("baseFolderMobile");
		// table info
		INSA_USER_TABLE = propMan.getConfigValue("insaUserTable");
		INSA_ORG_TABLE = propMan.getConfigValue("insaOrgTable");
		MST_USER_TABLE = propMan.getConfigValue("mstUserTable");
		MST_ORG_TABLE = propMan.getConfigValue("mstOrgTable");
		UPDATE_LOG_TABLE = propMan.getConfigValue("updateLogTable");
		TRANS_LOG_TABLE = propMan.getConfigValue("transLogTable");
		ORG_CD = propMan.getConfigValue("orgCd");
		SOURCE_ORG = propMan.getConfigValue("sourceOrg");
		
		log.info("INSA_USER_TABLE={},INSA_ORG_TABLE={},MST_USER_TABLE={},MST_ORG_TABLE={},UPDATE_LOG_TABLE={},TRANS_LOG_TABLE={},ORG_CD={},SOURCE_ORG={}",
				INSA_USER_TABLE, INSA_ORG_TABLE, MST_USER_TABLE, MST_ORG_TABLE, UPDATE_LOG_TABLE, TRANS_LOG_TABLE, ORG_CD, SOURCE_ORG);
	}
	
	public void writeDataFile(CSVHandler csvHandler, ResultSet rs) throws Exception {
		int count = 0;

		// write header row into csv
		writeHeadData(csvHandler, rs);
		
		while (rs.next()) {
			try {
				// write data row into csv
				writeRowData(csvHandler, rs, ++count);
			} catch (Exception e) {
				log.error("Writing to data file fail : ", e);
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public void writeDataFile(String procDt, CSVWriter csvWriter, ResultSet rs, String target) throws Exception {
		writeDataFile(procDt, csvWriter, rs, target, true);
	}
	
	public void writeDataFile(String procDt, CSVWriter csvWriter, ResultSet rs, String target, boolean logging) throws Exception {
		int count = 0;

		// write header row into csv
		writeHeadData(csvWriter, rs);
		
		while (rs.next()) {
			try {
				// write data row into csv
				writeRowData(procDt, csvWriter, rs, ++count, target, logging);
			} catch (Exception e) {
				log.error("Writing to data file fail : ", e);
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public void writeDataFileForMobile(String procDt, CSVWriter csvWriter, ResultSet rs, String target) throws Exception {
		int count = 0;

		// write header row into csv
		writeHeadData(csvWriter, rs);
		
		while (rs.next()) {
			try {
				// write data row into csv
				writeRowDataForMobile(procDt, csvWriter, rs, ++count, target);
			} catch (Exception e) {
				log.error("Writing to data file fail : ", e);
				throw new Exception(e.getMessage());
			}
		}
	}

	private void writeRowData(CSVHandler csvHandler, ResultSet rs, int row) throws Exception {
		ResultSetMetaData rsMetaData = rs.getMetaData();
		int columnCnt = rsMetaData.getColumnCount();
		String[] aData = new String[columnCnt];

		StringBuilder transData = new StringBuilder();
		String colName = null;
		String colData = null;
		for (int i = 1; i <= columnCnt; i++) {
			colName = rsMetaData.getColumnName(i);
			colData = rs.getString(colName);
			aData[i-1] = colData;
			transData.append(colName + "=" + colData + ",");
			//log.info(i+"[{}]>{}", colName, colData);
		}
		csvHandler.getWriter().writeNext(aData);
	}
	
	private void writeRowData(String procDt, CSVWriter csvWriter, ResultSet rs, int row, String target, boolean logging) throws Exception {
		ResultSetMetaData rsMetaData = rs.getMetaData();
		int columnCnt = rsMetaData.getColumnCount();
		String[] aData = new String[columnCnt];

		StringBuilder transData = new StringBuilder();
		String colName = null;
		String colData = null;
		for (int i = 1; i <= columnCnt; i++) {
			colName = rsMetaData.getColumnName(i);
			colData = rs.getString(colName);
			aData[i-1] = colData;
			transData.append(colName + "=" + colData + ",");
			//log.info(i+"[{}]>{}", colName, colData);
		}
		csvWriter.writeNext(aData);
		//add trans log
		if (logging) {
			addTransLog(procDt, rs.getString("UPDATEDT"), target.equals(JOB_TARGET_USER) ? rs.getString("ALTKEY") : rs.getString("ORGID"), 
					target, rs.getString("TYPE"), transData.toString());
		}
	}
	
	private void writeRowDataForMobile(String procDt, CSVWriter csvWriter, ResultSet rs, int row, String target) throws Exception {
		ResultSetMetaData rsMetaData = rs.getMetaData();
		int columnCnt = rsMetaData.getColumnCount();
		String[] aData = new String[columnCnt];

		StringBuilder transData = new StringBuilder();
		String colName = null;
		String colData = null;
		//TYPE 처리
		String mobileYn = rs.getString("MOBILEUSE");
		if ("Y".equals(mobileYn)) {
			aData[0] = "ADD";
		} else {
			aData[0] = "DEL";
		}
		//TYPE이후 데이터 처리 : from 2nd column
		for (int i = 2; i <= columnCnt; i++) {
			colName = rsMetaData.getColumnName(i);
			colData = rs.getString(colName);
			aData[i-1] = colData;
			transData.append(colName + "=" + colData + ",");
			//log.info(i+"[{}]>{}", colName, colData);
		}
		csvWriter.writeNext(aData);
		//add trans log
		addTransLog(procDt, rs.getString("UPDATEDT"), target.equals(JOB_TARGET_USER) ? rs.getString("ALTKEY") : rs.getString("ORGID"), 
				target, rs.getString("TYPE"), transData.toString());
	}
	
	private void writeHeadData(CSVHandler csvHandler, ResultSet rs) throws Exception {
		ResultSetMetaData rsMetaData = rs.getMetaData();
		int columnCnt = rsMetaData.getColumnCount();
		String[] aData = new String[columnCnt];

		for (int i = 1; i <= columnCnt; i++) {
			aData[i-1] = rsMetaData.getColumnName(i);
		}
		csvHandler.getWriter().writeNext(aData);
	}
	
	private void writeHeadData(CSVWriter csvWriter, ResultSet rs) throws Exception {
		ResultSetMetaData rsMetaData = rs.getMetaData();
		int columnCnt = rsMetaData.getColumnCount();
		String[] aData = new String[columnCnt];

		for (int i = 1; i <= columnCnt; i++) {
			aData[i-1] = rsMetaData.getColumnName(i);
		}
		csvWriter.writeNext(aData);
	}

	public int getDataRowCnt(ResultSet rs) {
		int rowCnt = 0;
		try {
			rs.last();
			rowCnt = rs.getRow();
			rs.beforeFirst();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowCnt;
	}
	
	public String makeInsertSql (String tableName, String [] colNames) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into " + tableName + " (");
		int loop = 0;
		for (String colName : colNames) {
			sb.append(colName + (++loop < colNames.length ? "," : "")); 
		}
		sb.append(") values (");
		loop = 0;
		for (int i=0; i<colNames.length; i++) {
			sb.append("?" + (++loop < colNames.length ? "," : ""));
		}
		sb.append(")");
		return sb.toString();
	}
	
	public String makeUpdateSql (String tableName, String [] colNames) {
		StringBuilder sb = new StringBuilder();
		sb.append("update " + tableName + " set\n");
		int loop = 0;
		for (String colName : colNames) {
			sb.append(colName + " = " + (++loop < colNames.length ? "?,\n" : "? ")); 
		}
		return sb.toString();
	}
	
	public String makeUpdateFromCsvSql (String tableName, List<String[]> colNames, Map<String,String> csvData) {
		StringBuilder sb = new StringBuilder();
		sb.append("update " + tableName + " set\n");
		for (String[] colName : colNames) {
			String colData = csvData.get(colName[1]);
			if (colData != null && !"".equals(colData)) {
				sb.append(colName[0] + " = " + "?,\n"); 
			}
		}
		sb.delete(sb.length()-2, sb.length()-1);
		return sb.toString();
	}
	
	private void prepareUpdateLog() {
		String[] colNames = {
				"PROCDT",
				"UPDATEDT",
				"UPDATEKEY",
				"TARGET",
				"GUBUN",
				"UPDATEDATA",
				"UUID"};
		
		String sql = makeInsertSql(UPDATE_LOG_TABLE, colNames);
		try {
			updateLogStmt = dbConn.prepareStatement(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error("failed to prepare update sql",e);
		}
	}
	
	public void addUpdateLog(String procDt, String updateDt, String updateKey, String target, String gubun, String updateData) throws Exception {
		//String curDtStr = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
		
		try {
			/*if (updateLogStmt != null) {
				updateLogStmt.setString(1, procDt);
				updateLogStmt.setString(2, updateDt);
				updateLogStmt.setString(3, updateKey);
				updateLogStmt.setString(4, target);
				updateLogStmt.setString(5, gubun);
				updateLogStmt.setString(6, updateData);
				updateLogStmt.setString(7, UUID.randomUUID().toString());
				
				updateLogStmt.execute();
			}*/
			log.info(">>UPDATE_LOG=[{}]-[{}]-[{}]-[{}]-[{}]", procDt, updateDt, target, gubun, updateData);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("failed to insert update log=[{}] : {}", updateData, e.getMessage());
			throw new Exception("failed to insert update log");
		}
	}
	
	private void prepareTransLog() {
		String[] colNames = {
				"PROCDT",
				"TRANSDT",
				"TRANSKEY",
				"TARGET",
				"GUBUN",
				"TRANSDATA",
				"UUID"};
		
		String sql = makeInsertSql(TRANS_LOG_TABLE, colNames);
		try {
			transferLogStmt = dbConn.prepareStatement(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error("failed to prepare update sql",e);
		}
	}
	
	public void addTransLog(String procDt, String transDt, String transKey, String target, String gubun, String transData) {
		//String curDtStr = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");

		try {
			/*if (transferLogStmt != null) {
				transferLogStmt.setString(1, procDt);
				transferLogStmt.setString(2, transDt);
				transferLogStmt.setString(3, transKey);
				transferLogStmt.setString(4, target);
				transferLogStmt.setString(5, gubun);
				transferLogStmt.setString(6, transData);
				transferLogStmt.setString(7, UUID.randomUUID().toString());
				
				transferLogStmt.execute();
			}*/
			log.info(">>TRANS_LOG=[{}]-[{}]-[{}]-[{}]-[{}]", procDt, transDt, target, gubun, transData);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("failed to insert trans log=[{}] : {}", transData, e.getMessage());
		}
	}
	
	public static List<Path> listFilesOldestFirst(final String directoryPath) throws IOException {
	    try (final Stream<Path> fileStream = Files.list(Paths.get(directoryPath))) {
	        return fileStream
	            .map(Path::toFile)
	            .filter(v->v.getAbsolutePath().contains(".csv"))
	            .collect(Collectors.toMap(Function.identity(), File::lastModified))
	            .entrySet()
	            .stream()
	            .sorted(Map.Entry.comparingByValue())
	            //.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))  // replace the previous line with this line if you would prefer files listed newest first
	            .map(Map.Entry::getKey)
	            .map(File::toPath)  // remove this line if you would rather work with a List<File> instead of List<Path>
	            .collect(Collectors.toList());
	    }
	}
	
	public static void moveFile(Path srcFile, String dstFile) throws IOException {
		String outFolder = StringUtils.getFolder(dstFile);
		Path path = Paths.get(outFolder);
		if (Files.exists(path) == false) {
			// cFolder.mkdir();
			Files.createDirectories(path);
		}
		
		File csvMoveDir = new File(dstFile);
		Files.move(srcFile, csvMoveDir.toPath(), StandardCopyOption.REPLACE_EXISTING);
		
		//파일 권한 666으로 변경
		if (!OS_NAME.contains("win")) {
			Runtime.getRuntime().exec("chmod 666 " + dstFile);
		}
	}

	public void waitBeforeJobFinished(String jobName, SqlSessionTemplate sqlSessionTemplate) throws Exception {
		int retryCnt = 0;
		while(true) {
			// 사전작업의 실행상태를 조회한다.
			JobStatusVo beforeJobStatus = sqlSessionTemplate.selectOne("jobtrigger.getJobStatusByName", jobName);
			
			// 사전작업이 실행중인 경우에는 wait한다.
			if (JOB_STATUS_EXECUTING.equals(beforeJobStatus.getResultMsg())) {
				retryCnt++;
				Thread.sleep(WAIT_TIME);
			} else {
				log.info(">>> Before Job({}) Status: {} - retryCount={}", jobName, beforeJobStatus.getResultMsg(), retryCnt);
				break;
			}
		}
	}
}
