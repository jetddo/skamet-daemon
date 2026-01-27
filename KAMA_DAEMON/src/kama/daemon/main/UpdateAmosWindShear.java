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
public class UpdateAmosWindShear {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[][] amosWindShearConfig = new String[][]{
		
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
			" SELECT TO_CHAR(TM, ''YYYYMMDDHH24MI'') AS TM, STN_ID, RWY_DIR, WD_1MIN_AVG, WSPD_1MIN_MAX, WD_2MIN_AVG, WSPD_2MIN_MAX, WD_10MIN_AVG, WSPD_10MIN_MAX FROM AMISUSER.AMOS WHERE TM >= TO_DATE(''{0}'', ''YYYYMMDDHH24MI'') AND TM <= TO_DATE(''{1}'', ''YYYYMMDDHH24MI'') ";
		
	private final String deleteAmosWindShearQuery = 
			
			"DELETE AAMI_TEST.AMOS_WINDSHEAR WHERE TM >= TO_DATE(''{0}'', ''YYYYMMDDHH24MI'') AND TM <= TO_DATE(''{1}'', ''YYYYMMDDHH24MI'') ";
	
	private final String insertAmosWindShearQuery = 
			
			" INSERT INTO AAMI_TEST.AMOS_WINDSHEAR(TM,RWY_DIR_SET,STN_ID,SHEAR_1MIN,SHEAR_2MIN,SHEAR_10MIN,WSPD_1MIN_MAX_RWY1,WSPD_2MIN_MAX_RWY1,WSPD_10MIN_MAX_RWY1,WSPD_1MIN_MAX_RWY2,WSPD_2MIN_MAX_RWY2,WSPD_10MIN_MAX_RWY2,WD_1MIN_AVG_RWY1,WD_2MIN_AVG_RWY1,WD_10MIN_AVG_RWY1,WD_1MIN_AVG_RWY2,WD_2MIN_AVG_RWY2,WD_10MIN_AVG_RWY2) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24MI''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'', ''{17}'') "; 
		
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
			
			System.out.println("Error : UpdateAmosWindShear.initialize -> " + e);
			
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
		
		this.dbManager.executeQuery(MessageFormat.format(this.deleteAmosWindShearQuery, new Object[]{
			stDtStr, edDtStr 
		}));
		
		List<String> tmList = this.getTmList(stDtStr, edDtStr, sdf);
		
		System.out.println("Info : UpdateAmosWindShear.process -> Delete AmosWindShear Data [" + stDtStr + " ~ " + edDtStr + "]");
		
		List<Map<String, Object>> amosDataList = this.getAmosData(stDtStr, edDtStr);
		
		System.out.println("Info : UpdateAmosWindShear.process -> amosDataList Size [" + amosDataList.size() + "]");
	
		for(int i=0 ; i<this.amosWindShearConfig.length ; i++) {
			
			String targetStnId = this.amosWindShearConfig[i][0];
			
			String[] rwyDirSets = this.amosWindShearConfig[i][1].split("\\|");
			String[] rwyDirAngles = this.amosWindShearConfig[i][2].split("\\|");
			String[] windTypes = new String[] {"1","2","10"};
			
			for(int j=0 ; j<rwyDirSets.length ; j++) {
		
				String rwyDirSet = rwyDirSets[j];
				
				String targetRwyDir1 = rwyDirSet.split("\\/")[0];
				String targetRwyDir2 = rwyDirSet.split("\\/")[1];
				
				float rwyDirAngle = Float.valueOf(rwyDirAngles[j]);
				
				for(int k=0 ; k<tmList.size() ; k++) {
					
					String targetTmStr = tmList.get(k);
					
					float[] wd1 = new float[] {-9999f,-9999f,-9999f};
					float[] wd2 = new float[] {-9999f,-9999f,-9999f};
					float[] wspd1 = new float[] {-9999f,-9999f,-9999f};
					float[] wspd2 = new float[] {-9999f,-9999f,-9999f};
					double[] shear = new double[]{-9999d,-9999d,-9999d};
					
					for(int l=0 ; l<amosDataList.size() ; l++) {
					
						Map<String, Object> amosData = amosDataList.get(l);
						
						String tm = (String)amosData.get("tm");
						String stnId = (String)amosData.get("stnId");
						String rwyDir = (String)amosData.get("rwyDir");		
						
						if(!targetStnId.equals(stnId) || !targetTmStr.equals(tm)) {
							continue;
						}
						
						if(targetTmStr.equals(tm) && targetRwyDir1.equals(rwyDir)) {
							
							for(int m=0 ; m<windTypes.length ; m++) {
								wd1[m] = Float.valueOf((String)amosData.get("wd"+ windTypes[m] +"minAvg"));
								wspd1[m] = Float.valueOf((String)amosData.get("wspd"+ windTypes[m] +"minMax"));
							}
						}
						
						if(targetTmStr.equals(tm) && targetRwyDir2.equals(rwyDir)) {
							
							for(int m=0 ; m<windTypes.length ; m++) {
								wd2[m] = Float.valueOf((String)amosData.get("wd"+ windTypes[m] +"minAvg"));
								wspd2[m] = Float.valueOf((String)amosData.get("wspd"+ windTypes[m] +"minMax"));
							}
						}
					}
					
					if(!checkWindData(wd1, wd2, wspd1, wspd2)) {
						continue;
					}
					
					for(int l=0 ; l<windTypes.length ; l++) {						
						shear[l] = getWindShear(wd1[l], wd2[l], wspd1[l], wspd2[l], rwyDirAngle);						
					}
					
					String query = MessageFormat.format(this.insertAmosWindShearQuery, new Object[]{
						targetTmStr, 
						targetRwyDir1 + "/" + targetRwyDir2, 
						targetStnId, 
						shear[0],
						shear[1],
						shear[2],						
						wspd1[0],
						wspd1[1],
						wspd1[2],
						wspd2[0],
						wspd2[1],
						wspd2[2],
						wd1[0],
						wd1[1],
						wd1[2],
						wd2[0],
						wd2[1],
						wd2[2],
					});
					
					this.dbManager.executeQuery(query);
				}
			}
		}
		
		this.dbManager.commit();
	}
	
	private boolean checkWindData(float[] wd1, float[] wd2, float[] wspd1, float[] wspd2) {
		
		for(int i=0 ; i<3 ; i++) {
			
			if(wd1[i] == -9999f || wd2[i] == -9999f || wspd1[i] == -9999f || wspd2[i] == -9999f) {
				return false;
			}
		}
		
		return true;
	}
	
	private double getWindShear(double wd1, double wd2, double wspd1, double wspd2, float rwyDirAngle) {
		
		double v1 = wspd1/10 * ( Math.sin(Math.toRadians(wd1))*Math.sin(Math.toRadians(rwyDirAngle)) + Math.cos(Math.toRadians(wd1))*Math.cos(Math.toRadians(rwyDirAngle)));
		double v2 = wspd2/10 * ( Math.sin(Math.toRadians(wd2))*Math.sin(Math.toRadians(rwyDirAngle)) + Math.cos(Math.toRadians(wd2))*Math.cos(Math.toRadians(rwyDirAngle)));
		
		return v1-v2;
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
			
			UpdateAmosWindShear program = new UpdateAmosWindShear();
			
			if(!program.initialize()) {
				
				System.out.println("Error : UpdateAmosWindShear.process -> initialize failed");
				return;
			}
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			
			Calendar cal = new GregorianCalendar();
			
			Date startDate = null;
			Date endDate = new Date();
			cal.setTime(endDate);
			cal.add(Calendar.MINUTE, -5);
			startDate = cal.getTime();
			
			program.process(startDate, endDate);
						
//			Date startDate = sdf.parse("202505010000");
//			Date endDate =   sdf.parse("202601010000");
			
//			cal.setTime(startDate);
//			
//			while(cal.getTime().getTime() < endDate.getTime()) {
//				
//				Date subStartDate = cal.getTime();
//				
//				cal.add(Calendar.HOUR_OF_DAY, 1);
//				
//				Date subEndDate = cal.getTime();
//				
//				program.process(subStartDate, subEndDate);
//			}
			
			program.destroy();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}