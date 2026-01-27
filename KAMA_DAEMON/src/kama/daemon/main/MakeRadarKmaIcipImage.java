package kama.daemon.main;

import java.io.File;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.model.observation.adopt.RDR.proc.RadarKmaIcipConverter;

public class MakeRadarKmaIcipImage {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private final String insertKmaIcipFileInfo = 
			
			" INSERT INTO AAMI_TEST.DMON_FILE_PROC_H(DH_SEQ, FILE_DT, FILE_STR_LOC, PROC_DT, FILE_CD) VALUES " + 
			" (AAMI.DMON_FILE_PROC_H_SEQ.NEXTVAL, TO_DATE(''{0}'', ''YYYYMMDDHH24MI''), ''{1}'', SYSDATE, ''{2}'') "; 
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
				
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : MakeRadarKmaIcipImage.initialize -> " + e);
			
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
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd");	
		
		if(!this.initialize()) {
			
			System.out.println("Error : MakeRadarKmaIcipImage.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		try {
			
			// 실행시점으로 부터 30분전까지 찾는다
			Calendar cal = new GregorianCalendar();
			
			cal.setTime(new Date());
			cal.add(Calendar.MINUTE, -20);
			// 20분정도의 차이가 있다
			
			//cal.setTime(sdf.parse("202507170050"));
			
			cal.add(Calendar.MINUTE, -cal.get(Calendar.MINUTE)%5);
			
			Date endDate = cal.getTime();
			
			cal.add(Calendar.MINUTE, -5);
			
			Date startTm = cal.getTime();
			
			while(cal.getTime().getTime() < endDate.getTime()) {
				
				Date tm = cal.getTime();
				                        
				String kmaIcipFileName = "RDR_R2D_EXT_ICIP_" + sdf.format(tm) + ".nc";
				
				File kmaIcipFile = new File(storePath + File.separator + "RDR" + File.separator + sdf2.format(tm) + File.separator + kmaIcipFileName);
				
				if(kmaIcipFile.exists()) {
					
					Date fileDate = sdf.parse(kmaIcipFile.getName().split("\\.")[0].split("_")[4]);
					
					RadarKmaIcipConverter converter = new RadarKmaIcipConverter();
					converter.parseRadarFile(kmaIcipFile, fileDate, storePath + File.separator + "RDR_IMG/KMA_ICIP");
					
					String query = MessageFormat.format(this.insertKmaIcipFileInfo, new Object[]{
						sdf.format(fileDate), kmaIcipFile.getAbsoluteFile(), "KMA_ICIP"	
					});
					
					this.dbManager.executeUpdate(query);
					this.dbManager.commit();
				}
				
				cal.add(Calendar.MINUTE, 5);
			}
			
		} catch (Exception e) {
			
		}
		
		this.destroy();
	}
	
	
	public static void main(String[] args) {

		new MakeRadarKmaIcipImage().process();
	}
}