package kama.daemon.main;

import java.io.File;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
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
public class UpdateAmosTailWind {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[][] amosTailWindConfig = new String[][]{
		
		{"113", "16/34|16R/34L|16L/34R|15L/33R|15R/33L", "144.66|144.66|144.66|144.66|144.66"},
		{"92", "15/33", "141.16"},
		{"167", "17/35", "158.03"},
		{"110", "14R/32L|14L/32R", "135.01|135.01"},
		{"182", "07/25|13/31", "58.46|125.62"},
		{"163", "01/19", "359.67"},
		{"151", "18/36", "176.1"},
		{"153", "18/36|18R/36L", "173.95|173.95"},
		{"128", "06/24", "52.42"},
		{"142", "13/31", "124.23"},
		{"158", "04/22", "29.93"},
		{"139", "10/28", "89.23"},
		{"161", "06/24", "55.59"},
		{"118", "03/21", "100"}
	};
	
	private final String getAmosDataQuery = 
			
			" SELECT TO_CHAR(TM, ''YYYYMMDDHH24MI'') AS TM, STN_ID, RWY_DIR, WD_1MIN_AVG, WSPD_1MIN_MAX, WD_2MIN_AVG, WSPD_2MIN_MAX FROM AMISUSER.AMOS WHERE TM >= TO_DATE(''{0}'', ''YYYYMMDDHH24MI'') AND TM <= TO_DATE(''{1}'', ''YYYYMMDDHH24MI'') ";
		
	private final String deleteAmosTailWindQuery = 
			
			"DELETE AAMI_TEST.AMOS_TAILWIND WHERE TM >= TO_DATE(''{0}'', ''YYYYMMDDHH24MI'') AND TM <= TO_DATE(''{1}'', ''YYYYMMDDHH24MI'') ";
	
	private final String insertAmosTailWindQuery = 
			
			" INSERT INTO AAMI_TEST.AMOS_TAILWIND(TM, RWY_DIR_SET, STN_ID, VARIANCE, WD1, WD2, WSPD1, WSPD2) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24MI''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'') "; 
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private AmisDataBaseManager amisDbmanager;
	
	public boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			this.amisDbmanager = new AmisDataBaseManager(this.config);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : UpdateAmosTailWind.initialize -> " + e);
			
			this.dbManager.safeClose();
			this.amisDbmanager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	public void destroy() {
		
		this.dbManager.safeClose();                                             
		this.amisDbmanager.safeClose();
	}
	
	private void process(Date startDate, Date endDate) {
		
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
			
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		
		String stDtStr = sdf.format(startDate);
		
		String edDtStr = sdf.format(endDate);
		
		this.dbManager.executeQuery(MessageFormat.format(this.deleteAmosTailWindQuery, new Object[]{
			stDtStr, edDtStr 
		}));
		
		List<String> tmList = this.getTmList(stDtStr, edDtStr, sdf);
		
		System.out.println("Info : UpdateAmosTailWind.process -> Delete AmosTailWind Data [" + stDtStr + " ~ " + edDtStr + "]");
		
		List<Map<String, Object>> amosDataList = this.getAmosData(stDtStr, edDtStr);
		
		System.out.println("Info : UpdateAmosTailWind.process -> amosDataList Size [" + amosDataList.size() + "]");
	
		for(int i=0 ; i<this.amosTailWindConfig.length ; i++) {
			
			String targetStnId = this.amosTailWindConfig[i][0];
			
			String[] rwyDirSets = this.amosTailWindConfig[i][1].split("\\|");
			String[] rwyDirAngles = this.amosTailWindConfig[i][2].split("\\|");
			
			for(int j=0 ; j<rwyDirSets.length ; j++) {
		
				String rwyDirSet = rwyDirSets[j];
				
				String targetRwyDir1 = rwyDirSet.split("\\/")[0];
				String targetRwyDir2 = rwyDirSet.split("\\/")[1];
				
				float rwyDirAngle = Float.valueOf(rwyDirAngles[j]);
				
				for(int k=0 ; k<tmList.size() ; k++) {
					
					String targetTmStr = tmList.get(k);
					float wd1 = -9999f;
					float wd2 = -9999f;
					float wspd1 = -9999f;
					float wspd2 = -9999f;
					
					for(int l=0 ; l<amosDataList.size() ; l++) {
					
						Map<String, Object> amosData = amosDataList.get(l);
						
						String tm = (String)amosData.get("tm");
						String stnId = (String)amosData.get("stnId");
						String rwyDir = (String)amosData.get("rwyDir");		
						
						if(!targetStnId.equals(stnId) || !targetTmStr.equals(tm)) {
							continue;
						}
						
						float wd1MinAvg = Float.valueOf((String)amosData.get("wd1minAvg"));
						float wspd1MinMax = Float.valueOf((String)amosData.get("wspd1minMax"));
						
						if(wd1MinAvg == -9999f || wspd1MinMax == -9999f) {
							wd1MinAvg = Float.valueOf((String)amosData.get("wd2minAvg"));
							wspd1MinMax = Float.valueOf((String)amosData.get("wspd2minMax"));
						}
						
						if(targetTmStr.equals(tm) && targetRwyDir1.equals(rwyDir)) {
							wd1 = wd1MinAvg;
							wspd1 = wspd1MinMax;
						}
						
						if(targetTmStr.equals(tm) && targetRwyDir2.equals(rwyDir)) {
							wd2 = wd1MinAvg;
							wspd2 = wspd1MinMax;
						}
					}
					
					if(wd1 == -9999f || wd2 == -9999f || wspd1 == -9999f || wspd2 == -9999f) {
						continue;
					}
										
					double tailWind1 = wspd1 * Math.cos(Math.toRadians(rwyDirAngle - wd1));
					double tailWind2 = wspd2 * Math.cos(Math.toRadians(rwyDirAngle - wd2));
					
					this.dbManager.executeQuery(MessageFormat.format(this.insertAmosTailWindQuery, new Object[]{
						targetTmStr, targetRwyDir1 + "/" + targetRwyDir2, targetStnId, (tailWind1 - tailWind2)/10, wd1, wd2, wspd1, wspd2 
					}));
				}
			}
		}
		
		this.dbManager.commit();
	}
	
	private double getWindShear(double wspd1, double wspd2, double wd1, double wd2) {
		return 0d;
	}
	
	private List<String> getTmList(String stDtStr, String edDtStr, SimpleDateFormat sdf) {
		
		List<String> tmList = new ArrayList<String>();
		
		int cnt = 0;
		
		try {
			
			Calendar cal = new GregorianCalendar();
			cal.setTime(sdf.parse(stDtStr));
			
			while(cnt < 100 && cal.getTime().getTime() <= sdf.parse(edDtStr).getTime()) {
				
				tmList.add(sdf.format(cal.getTime()));
				cal.add(Calendar.MINUTE, 1);
				cnt++;
			}
			
		} catch (Exception e) {
			
		}
		
		return tmList;
	}
	
	private List<Map<String, Object>> getAmosData(String stDtStr, String edDtStr) {
		
		try {
			
			List<Map<String, Object>> amosDataList = this.amisDbmanager.select(MessageFormat.format(this.getAmosDataQuery, new Object[]{
					stDtStr, edDtStr
			}));
			
			return amosDataList;
			
		} catch (Exception e) {
			
			e.printStackTrace();
		
			return new ArrayList<Map<String, Object>>();
		}
	}
	
	public static void main(String[] args) {
		
		try {
			
			UpdateAmosTailWind program = new UpdateAmosTailWind();
			
			if(!program.initialize()) {
				
				System.out.println("Error : UpdateAmosTailWind.process -> initialize failed");
				return;
			}
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			
			Date startDate = sdf.parse("202201010000");
			Date endDate =   sdf.parse("202501010000");
			
			Calendar cal = new GregorianCalendar();
			
			cal.setTime(startDate);
			
			while(cal.getTime().getTime() < endDate.getTime()) {
				
				Date subStartDate = cal.getTime();
				
				cal.add(Calendar.HOUR_OF_DAY, 1);
				
				Date subEndDate = cal.getTime();
				
				program.process(subStartDate, subEndDate);
			}
			
			program.destroy();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}