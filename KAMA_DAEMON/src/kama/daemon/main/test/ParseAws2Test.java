package kama.daemon.main.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

public class ParseAws2Test {
	
	private DatabaseManager dbManager;
	
	private Configuration config;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
		} catch (ConfigurationException e ) {
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();   
	}

	private void parseAws2File(File file) {
		
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
				
			Calendar cal = new GregorianCalendar();
			
			String line = null;
			
			for (int i=0 ; (line = br.readLine()) != null; i++) {
				
				if(i == 0) {
					continue;
				}
				
				Map<String, String> map = new HashMap<String, String>();
				
				String[] tokens = line.split(",");
					
				String time = sdf.format(sdf2.parse(tokens[1].replaceAll("T", " ").replaceAll("Z", "")));
				String stn_id = tokens[35];
				String ca_tot = tokens[2];
				String ch_1yr_1 = tokens[3];
				String ch_1yr_2 = tokens[4];
				String ch_1yr_3 = tokens[5];
				String vi = tokens[72];
				String wd = tokens[74];
				String wd_10min = tokens[75];
				String wd_gst = tokens[76];
				String ws = tokens[77];
				String ws_10min = tokens[78];
				String ws_gst = tokens[79];
				
				String query = "INSERT INTO AAMI.KMA_AWS2_1MIN(TIME, STN_ID, CH_LYR_1, CH_LYR_2, CH_LYR_3, VI, WD, WD_10MIN, WD_GST, WS, WS_10MIN, WS_GST, CA_TOT ) VALUES " +
				" (TO_DATE(''{0}'', ''YYYYMMDDHH24MISS''),''{1}'',''{2}'',''{3}'',''{4}'',''{5}'',''{6}'',''{7}'',''{8}'',''{9}'',''{10}'',''{11}'',''{12}'') ";
				
				String q = MessageFormat.format(query, new Object[] {
					time, stn_id, ch_1yr_1, ch_1yr_2, ch_1yr_3, vi, wd, wd_10min, wd_gst, ws, ws_10min, ws_gst,ca_tot
				});
				
				q = q.replaceAll("''", "null");
			
				this.dbManager.executeQuery(q);
				
				
			}
			
			this.dbManager.commit();
			
			br.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	public static void main(String[] args) throws Exception {
		
		File file = new File("F:/KAMA_AAMI/2023/tbl_obs_aws/tbl_obs_aws.csv");
		
		ParseAws2Test test = new ParseAws2Test();
		test.initialize();		
		test.parseAws2File(file);
		test.destroy();
	}

}
