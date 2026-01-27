package kama.daemon.main;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

public class ParseComisLgtExtData {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
	private final String insertParseComisDataInfo = 
			
			" INSERT INTO AAMI.PARSE_COMIS_DATA_HIS(FILE_DT, PROC_DT, FILE_NAME, FILE_CD) VALUES " + 
			" (TO_DATE('{fileDt}', 'YYYYMMDDHH24MI'), SYSDATE, '{fileName}', 'LGT_EXT') "; 
	
	private final String selectParseComisDataInfoList = 
			
			" SELECT 												"+
			" 	TO_CHAR((FILE_DT), 'YYYYMMDDHH24') AS FILE_DT,		"+
			" 	FILE_NAME											"+
			" FROM AAMI.PARSE_COMIS_DATA_HIS						"+
			" WHERE FILE_DT >= TO_DATE('{targetDt}', 'YYYYMMDD')	"+
			" AND FILE_CD = 'LGT_EXT'									";
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
	
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : ParseComisLgtExtData.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");		
		
		if(!this.initialize()) {
			
			System.out.println("Error : MakeUmLdpsImage.process -> initialize failed");
			return;
		}
		
		String storePath = this.config.getString("global.comisPath.unix");
		
		//String storePath = "C:/data/COMIS_DATA";
		
		Calendar cal = new GregorianCalendar();
		
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_MONTH, -1);
		// 하루전꺼부터 체크
		
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");		
		
		try {
			
			String targetDtStr = sdf2.format(cal.getTime());
			
			String query = this.selectParseComisDataInfoList.replaceAll("\\{targetDt\\}", targetDtStr);
			
			
			List<Map<String, Object>> parsedFileInfoList = new ArrayList<Map<String, Object>>();
			
			ResultSet resultSet = dbManager.executeQuery(query);
			
			while(resultSet.next()) {
				
				Map<String, Object> parsedFileInfo = DaemonUtils.getCamelcaseResultSetData(resultSet);
				
				parsedFileInfoList.add(parsedFileInfo);
			}
				
			SimpleDateFormat sdf3 = new SimpleDateFormat("yyyyMM/dd");
			
			for(int i=0 ; i<3 ; i++) {
				
				Date targetDt = cal.getTime();			
				
				String targetDirStr = storePath + File.separator + "/LGT/EXT/" + sdf3.format(targetDt);
				
				File targetDir = new File(targetDirStr);
				
				if(targetDir.exists()) {
					
					File[] lgtFiles = targetDir.listFiles();
						
					for(int j=0 ; j<lgtFiles.length ; j++) {
						
						boolean isParsed = false;
						
						for(int k=0 ; k<parsedFileInfoList.size() ; k++) {
							
							Map<String, Object> parsedFileInfo = parsedFileInfoList.get(k);
							
							if(lgtFiles[j].getName().equals((String)parsedFileInfo.get("fileName"))) {
								isParsed = true;
								continue;
							}
						}
						
						if(isParsed) {
							continue;
						}
						
						this.parseLgtExtFile(lgtFiles[j]);
					}
					
				}
				
				cal.add(Calendar.DAY_OF_MONTH, 1);	
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		this.destroy();
	}
	
	private void parseLgtExtFile(File file)  {
		
		System.out.println("\t-> Parse File [" + file.getAbsolutePath() + "]");
		
		LineNumberReader reader = null;
		
		try {
				
			reader = new LineNumberReader(new FileReader(file));
			
			String line = "";
		
			while((line = reader.readLine()) != null) {
				
				String[] tokens = line.split(",");
				
				String query = this.buildQuery(new String[] {
					tokens[0].trim() + " " + tokens[1],
					tokens[2].trim(),
					tokens[3].trim(),
					tokens[4].trim(),
					tokens[5].trim()
				});
				
				this.dbManager.executeQuery(query);
			}
			
			String fileDt = file.getName().split("_")[3].split("\\.")[0];
				
			String query = this.insertParseComisDataInfo.replaceAll("\\{fileDt\\}", fileDt)
														.replaceAll("\\{fileName\\}", file.getName());
			
			this.dbManager.executeQuery(query);
			
		} catch (Exception e) {
			
			e.printStackTrace();
			
			this.dbManager.rollback();
			return;
			
		} finally {
			
			try {
				
				reader.close();
				
			} catch (Exception e) {
				
			}
		}
		
		this.dbManager.commit();
	}
	
	private String buildQuery(String[] valueList) {
		
		String query = "INSERT INTO AAMI.KMA_LGT_EXT_WWLLN VALUES (";
		
		for(int i=0 ; i<valueList.length ; i++) {
			
			String value = valueList[i];
			
			if(i == 0) {				
				query += "TO_TIMESTAMP('" + value + "','YYYY/MM/DD HH24:MI:SS.FF6')";
			} else {
				query += "'" + value + "'";
			}
			
			if(i < valueList.length-1) {
				query += ",";
			}
		}
		
		query += ")";
		
		return query;
	}
	
	public static void main(String[] args) {

		new ParseComisLgtExtData().process();
	}
}