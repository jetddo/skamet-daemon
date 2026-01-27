package kama.daemon.main;

import java.io.File;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import kama.daemon.common.db.AmisDataBaseManager;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

/**
 * @author jetddo
 * Created on 2017-01-03
 */
public class UpdateAmisLLWAS {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
	private final String insertAmisLLWASQuery = 
			
			" INSERT INTO AAMI.LLWAS_ALERT(TM, STN_ID, RWY_DIR, RWY_AD, ALERT, LG, LGID, LOCATION, TH_WD, TH_WSPD) VALUES "+
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24MISS''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'') ";
	
	private final String selectAmisLLWASQuery = 
			
			" SELECT TO_CHAR(TM, ''YYYYMMDDHH24MISS'') AS TM, STN_ID, RWY_DIR, RWY_AD, ALERT, LG, LGID, LOCATION, TH_WD, TH_WSPD FROM AMISUSER.LLWAS_ALERT WHERE TM >= SYSDATE - 1/24/60*10 ORDER BY TM ASC";
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private AmisDataBaseManager amisDbmanager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			this.amisDbmanager = new AmisDataBaseManager(this.config);
			this.amisDbmanager.setAutoCommit(false);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : UpdateAmisLLWAS.initialize -> " + e);
			
			this.dbManager.safeClose();
			this.amisDbmanager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
		this.amisDbmanager.safeClose();
	}
	
	private void process() {
				
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start :::::");
		
		if(!this.initialize()) {
			
			System.out.println("Error : UpdateAmisLLWAS.process -> initialize failed");
			return;
		}
		
		List<Map<String, Object>> amisLLWASDataList = this.amisDbmanager.select(MessageFormat.format(this.selectAmisLLWASQuery, new Object[]{					
									
		}));
		
		for(int i=0 ; i<amisLLWASDataList.size() ; i++) {
			
			Map<String, Object> amisLLWASData = amisLLWASDataList.get(i);
			
			String query = MessageFormat.format(this.insertAmisLLWASQuery, new Object[]{
					amisLLWASData.get("tm"),
					amisLLWASData.get("stnId"),
					amisLLWASData.get("rwyDir"),
					amisLLWASData.get("rwyAd"),
					amisLLWASData.get("alert"),
					amisLLWASData.get("lg"),
					amisLLWASData.get("lgid"),
					amisLLWASData.get("location"),
					amisLLWASData.get("thWd"),
					amisLLWASData.get("thWspd")
			}).replaceAll("'null'", "null");
			
			this.dbManager.executeUpdate(query);
		}
		
		this.dbManager.commit();

		this.destroy();
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End :::::");
	}
	
	
	public static void main(String[] args) {

		new UpdateAmisLLWAS().process();
	}

}