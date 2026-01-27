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

public class ParseSanlimArmyData {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
	private final String insertParseComisDataInfo = 
			
			" INSERT INTO AAMI.PARSE_COMIS_DATA_HIS(FILE_DT, PROC_DT, FILE_NAME, FILE_CD) VALUES " + 
			" (TO_DATE('{fileDt}', 'YYYYMMDDHH24MI'), SYSDATE, '{fileName}', 'SANLIM_ARMY') "; 
	
	private final String selectParseComisDataInfoList = 
			
			" SELECT 												"+
			" 	TO_CHAR((FILE_DT), 'YYYYMMDDHH24') AS FILE_DT,		"+
			" 	FILE_NAME											"+
			" FROM AAMI.PARSE_COMIS_DATA_HIS						"+
			" WHERE FILE_DT >= TO_DATE('{targetDt}', 'YYYYMMDD')	"+
			" AND FILE_CD = 'SANLIM_ARMY'							";
		
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
			
			System.out.println("Error : ParseComisSanlimArmyData.initialize -> " + e);
			
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
			
			System.out.println("Error : ParseSanlimArmyData.process -> initialize failed");
			return;
		}
		
		String storePath = this.config.getString("global.storePath.unix");
		
		System.out.println("Store Path : " + storePath);
		
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
				
				String targetDirStr = storePath + File.separator + "/SANLIM/" + sdf3.format(targetDt);
				
				File targetDir = new File(targetDirStr);
				
				System.out.println("Target Dir : " + targetDir.getAbsolutePath());
				
				if(targetDir.exists()) {
					
					File[] sanlimArmyFiles = targetDir.listFiles();
						
					for(int j=0 ; j<sanlimArmyFiles.length ; j++) {
						
						boolean isParsed = false;
						
						for(int k=0 ; k<parsedFileInfoList.size() ; k++) {
							
							Map<String, Object> parsedFileInfo = parsedFileInfoList.get(k);
							
							if(sanlimArmyFiles[j].getName().equals((String)parsedFileInfo.get("fileName"))) {
								isParsed = true;
								continue;
							}
						}
						
						if(isParsed) {
							//System.out.println("\t-> File [" + sanlimArmyFiles[j].getAbsolutePath() + "] is already parsed.");
							continue;
						}
						
						if (sanlimArmyFiles[j].getName().matches("SANLIM_ARMY_[0-9]{12}.csv")) {
							this.parseSanlimArmyFile(sanlimArmyFiles[j]);
						}
					}
					
				} else {
					System.out.println("\t -> not exist directory");
				}
				
				cal.add(Calendar.DAY_OF_MONTH, 1);	
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		this.destroy();
	}
	
	private void parseSanlimArmyFile(File file)  {
		
		System.out.println("\t-> Parse File [" + file.getAbsolutePath() + "]");
		
		LineNumberReader reader = null;
		
		try {
				
			reader = new LineNumberReader(new FileReader(file));
			
			String line = "";
		
			while((line = reader.readLine()) != null) {
				
				if (reader.getLineNumber() == 1 || line.trim().isEmpty()) {
					// Skip header line
					continue;
				}
				
				String[] tokens = line.split("\\|");

				if (tokens.length < 8) {
					System.out.println("Invalid line format: " + line);
					continue;
				}
				
				String cloudHeight = tokens[0].trim();
				String cloudAmount = tokens[1].trim();
				String visibility = tokens[2].trim();
				String windVlct = tokens[3].trim();
				String windDrct = tokens[4].trim();
				String northSky = tokens[5].trim();
				String dispDate = tokens[6].trim();
				String seq = tokens[7].trim();
				
				if(!DaemonUtils.isNumber(cloudHeight)) {
					cloudHeight = "-9999";
				}
				
				cloudHeight = Integer.valueOf(cloudHeight) + "";
				
				if (visibility.matches("[0-9]{1} [0-9]{1}\\/[0-9]{1,2}")) {
					// 앞에는 정수부, 뒤에는 분수형태 두개를 더해야함
					String[] visibilityParts = visibility.split(" ");
					
					visibility = (Double.valueOf(visibilityParts[0]) + (Double.valueOf(visibilityParts[1].split("/")[0]) / Double.valueOf(visibilityParts[1].split("/")[1])))+"";
					
				} else if(visibility.matches("[0-9]{1}\\/[0-9]{1,2}")) {
					// 앞에는 정수부, 뒤에는 분수형태 두개를 더해야함
					String[] visibilityParts = new String[] {"0", visibility};
					
					visibility = (Double.valueOf(visibilityParts[0]) + (Double.valueOf(visibilityParts[1].split("/")[0]) / Double.valueOf(visibilityParts[1].split("/")[1])))+"";					
				}
				
				String query = this.buildQuery(new String[] { 
					seq,
					dispDate,
					windVlct,
					windDrct,
					visibility,
					northSky,
					cloudHeight,
					cloudAmount					
				});
				
				this.dbManager.executeQuery(query);
			}
			
			String fileDt = file.getName().split("_")[2].split("\\.")[0];
				
			String query = this.insertParseComisDataInfo.replaceAll("\\{fileDt\\}", fileDt)
														.replaceAll("\\{fileName\\}", file.getName());
			
			this.dbManager.executeQuery(query);
			
		} catch (Exception e) {
			
			e.printStackTrace();
			
			System.out.println(file.getAbsolutePath() + " parse error");
			
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
		
		String query = "INSERT INTO AAMI.SANLIM_ARMY_DATA VALUES (";
		
		for(int i=0 ; i<valueList.length ; i++) {
			
			String value = valueList[i];
			
			if(i == 1) {				
				query += "TO_DATE('" + value + "','YYYYMMDDHH24MISS')";
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

		new ParseSanlimArmyData().process();
	}
}