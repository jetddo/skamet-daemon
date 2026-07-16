package kama.daemon.main.test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

public class DeleteHugeTableData {
	
	String deleteQuery = 
			
			" DELETE aami.kma_nph_aws3_min " +
			" WHERE ROWID IN ( " +
			" SELECT ROWID FROM aami.kma_nph_aws3_min "+
			" WHERE TM < TO_DATE('20240701', 'YYYYMMDD') AND ROWNUM <= 100000) ";
	
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

	private void process() {
		
		try {
			
			for(int i=0 ; i<10 ; i++) {
				
				Date start_time = new Date(System.currentTimeMillis());
				
				this.dbManager.executeQuery(this.deleteQuery);		
				this.dbManager.commit();
				
				Date end_time = new Date(System.currentTimeMillis());
				
				float elapsed_sec = (float) ((end_time.getTime() - start_time.getTime()) / 1000.0);
				
				System.out.println("Elapsed Sec: " + elapsed_sec);
				
				Thread.sleep(1000);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	public static void main(String[] args) throws Exception {
		
		DeleteHugeTableData test = new DeleteHugeTableData();
		test.initialize();		
		test.process();
		test.destroy();
	}

}
