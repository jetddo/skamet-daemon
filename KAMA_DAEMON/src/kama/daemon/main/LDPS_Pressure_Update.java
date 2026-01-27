package kama.daemon.main;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.PointXY;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class LDPS_Pressure_Update {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private SimpleDateFormat modelDateFormat = new SimpleDateFormat("yyyyMMddHH");

	private final String modelDirName = "UM_LOA_PC";
	
	private final String[] subDirPatterns = new String[]{modelDirName, "[0-9]{4}", "[0-9]{2}", "[0-9]{2}", "[0-9]{2}"};
	
	private ModelGridUtil modelGridUtil;
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private Statement stmt;
	
	private Object[][] stnInfo = {
		{"113", 37.459682, 126.433906},
		{"92", 38.058909, 128.663433},
		{"167", 34.840029, 127.614272},
		{"110", 37.558328, 126.802935},		
		{"182", 33.510351, 126.491439},
		{"163", 34.993433, 126.387845},
		{"151", 35.592997, 129.355615},		
		{"153", 35.173273, 128.946202},
		{"128", 36.721994, 127.495685},		
		{"142", 35.899504, 128.638623},		
		{"158", 35.139808, 126.810784},		
		{"139", 35.984659, 129.433654},
		{"161", 35.092310, 128.086236}		
	};
		
	private final String insertLdpsPressureOracleQuery = 
			
			" INSERT INTO AMIS.FCT_PRESSURE(TM, FCT_TM, STN_ID, PRESSURE_NOW, PRESSURE_3H) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24''), TO_DATE(''{1}'', ''YYYYMMDDHH24''), ''{2}'', ''{3}'', ''{4}'') ";
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			String coordinatesLatPath = this.config.getString("um_loa.coordinates.lat.path");
			String coordinatesLonPath = this.config.getString("um_loa.coordinates.lon.path");
			
			modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS, null, coordinatesLatPath, coordinatesLonPath);
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			stmt = this.dbManager.getConnection().createStatement();
			
		} catch (ConfigurationException | SQLException e ) {
			
			System.out.println("Error : LDPS_Pressure_Update.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			try {
				stmt.close();	
			} catch (Exception e2) { }
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
		
		try {
			stmt.close();	
		} catch (Exception e2) { }
	}
	
	public static void main(String[] args) {
		
		try {
			
			LDPS_Pressure_Update ldpsPressureUpdate = new LDPS_Pressure_Update();
			ldpsPressureUpdate.process();
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	public void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		if(!this.initialize()) {
			
			System.out.println("Error : LDPS_Pressure_Update.process -> initialize failed");
			return;
		}
		
		String rootPath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												 : this.config.getString("global.storePath.unix");
		
		List<File> ldpsDirList = new ArrayList<File>();
		
		this.searchLdpsDirs(new File(rootPath), ldpsDirList, 0);
		
		Collections.sort(ldpsDirList, new Comparator<File>(){

			@Override
			public int compare(File o1, File o2) {

				return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
			}
		});
		
		this.processAmosStnLDPS(ldpsDirList, modelGridUtil, config);
	}
	
	private void searchLdpsDirs(File rootDir, List<File> ldpsDirList, int depth) {
		
		if(rootDir.exists()) {

			final String regex = this.subDirPatterns[depth];
			
			File[] subDirs = rootDir.listFiles(new FilenameFilter(){
			
				public boolean accept(File dir, String name) {

					return name.matches(regex);
				}
			});
			
			if(depth == this.subDirPatterns.length-1) {
				
				String[] tokens = rootDir.getAbsolutePath().split("\\"+File.separator);
				
				for(int i=0 ; i<subDirs.length ; i++) {
					
					try {
					
						Date issuedDate = this.modelDateFormat.parse(tokens[tokens.length-3]+tokens[tokens.length-2]+tokens[tokens.length-1]+subDirs[i].getName());
				
						Date nowDate = new Date();
						
						if((nowDate.getTime() - issuedDate.getTime()) < 2 * 24 * 60 * 60 * 1000) {
							ldpsDirList.add(subDirs[i]);	
						}
						
					} catch (Exception e) {
					
						continue;
					}
				}
				
			} else {
				
				for(File subDir : subDirs) {
					searchLdpsDirs(subDir, ldpsDirList, depth+1);
				}	
			}
		}
	}
	
	private void processAmosStnLDPS(List<File> ldpsDirList, ModelGridUtil modelGridUtil, Configuration config) {
		
		System.out.println(logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Get LDPS Model Data :::::");
		
		for(int i=0 ; i<ldpsDirList.size() ; i++) {
			
			File ldpsDir = ldpsDirList.get(i);
			
			System.out.println(logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Target LDPS Directory : " + ldpsDir.getAbsolutePath());
			
			String ldpsDirPath = ldpsDirList.get(i).getAbsolutePath();
			
			String issuedDateStr = ldpsDirPath.substring(ldpsDirPath.indexOf(modelDirName) + modelDirName.length()).replaceAll("\\"+File.separator, "");
			
			Date issuedDate = null;
		
			try {
				
				issuedDate = modelDateFormat.parse(issuedDateStr);
				
			} catch (ParseException e1) {
				
				System.out.println(logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Wrong LDPS Directory : " + ldpsDir.getAbsolutePath());
				continue;
			}
			
			Calendar cal = new GregorianCalendar();
			cal.setTime(issuedDate);
			cal.add(Calendar.HOUR_OF_DAY, 9);
			
			final String regex = "qwumloa_pc[0-9]{3}.nc";
			
			File[] ldpsFiles = ldpsDir.listFiles(new FilenameFilter(){
			
				public boolean accept(File dir, String name) {

					return name.matches(regex);
				}
			});
			
			if(ldpsFiles.length != 19) {
				continue;
			}
			
			Arrays.sort(ldpsFiles, new Comparator<File>(){

				@Override
				public int compare(File o1, File o2) {
					return o1.getName().compareTo(o2.getName());
				}
			});
		
			List<List<Map<String, Object>>> allDataList = new ArrayList<List<Map<String, Object>>>();
			
			for(int j=0 ; j<this.stnInfo.length ; j++) {				
				allDataList.add(new ArrayList<Map<String, Object>>());
			}
			
			for(int j=0 ; j<ldpsFiles.length ; j++) {
				
				try {
					
					System.out.println(logDateFormat.format(new Date(System.currentTimeMillis())) + "\t\t -> Target LDPS File : " + ldpsFiles[j].getAbsolutePath());
					
					NetcdfDataset ncFile = NetcdfDataset.openDataset(ldpsFiles[j].getAbsolutePath());
					
					String[] variableNames = new String[]{"p"};
					
					Date firstTime = cal.getTime();
				
					for(String variableName : variableNames) {
						
						Variable targetVariable = ncFile.findVariable(variableName);
						
						List<Range> originRangeList = targetVariable.getRanges();
						
						if(originRangeList.size() != 4) {
							continue;
						}
						
						Variable tVariable = ncFile.findVariable(originRangeList.get(0).getName());
						
						double[] t = this.convertNetcdfValue(tVariable.read().getStorage());
						
						for(int k=0 ; k<this.stnInfo.length ; k++) {
							
							String stnId = (String)this.stnInfo[k][0];
							double lat = (double)this.stnInfo[k][1];
							double lon = (double)this.stnInfo[k][2];
							
							PointXY pointXY = modelGridUtil.getPointXY(lon, lat);
					
							List<Range> rangeList = new ArrayList<Range>();
							
							rangeList.add(new Range(0, t.length-1));
							rangeList.add(new Range(0, 0));
							rangeList.add(new Range(pointXY.getY(), pointXY.getY()));
							rangeList.add(new Range(pointXY.getX(), pointXY.getX()));
							
							double[] value = this.convertNetcdfValue(targetVariable.read(rangeList).getStorage());
							
							for(int l=0 ; l<t.length ; l++) {
								
								Date fcstDate = cal.getTime();
								
								Map<String, Object> dataMap = new HashMap<String, Object>();
								
								dataMap.put("stnId", stnId);
								dataMap.put("p", value[l]);
								dataMap.put("issuedDate", modelDateFormat.format(issuedDate));
								dataMap.put("fcstDate", modelDateFormat.format(fcstDate));
								allDataList.get(k).add(dataMap);
															
								cal.add(Calendar.HOUR_OF_DAY, 1);
							}
							
							if(k != this.stnInfo.length-1) {
								cal.add(Calendar.HOUR_OF_DAY, t.length * -1);
							}
						}
					}
					
					ncFile.close();
						
				} catch (IOException | InvalidRangeException e) {

					e.printStackTrace();
				}
			}
			
			insertLdpsPressureData(allDataList);
		}
		
		this.dbManager.commit();
		
		this.destroy();
	}
			
	private void insertLdpsPressureData(List<List<Map<String, Object>>> allDataList) {
		
		for(int i=0 ; i<allDataList.size() ; i++) {
			
			List<Map<String, Object>> dataList = allDataList.get(i);
			
			for(int j=0 ; j<dataList.size() ; j++) {
			
				Map<String, Object> dataMap = dataList.get(j);
				
				String stnId = (String)dataMap.get("stnId");
				double p = (double)dataMap.get("p");
				double p_3h = -1;
				String issuedDate = (String)dataMap.get("issuedDate");
				String fcstDate = (String)dataMap.get("fcstDate");
				
				try {
					
					p_3h = (double)(dataList.get(i-3).get("p"));
					
				} catch (Exception e) {
					p_3h = -1;
				}
				
				String query = MessageFormat.format(this.insertLdpsPressureOracleQuery, new Object[]{
					fcstDate,
					issuedDate,
					stnId,
					(p/100) + "",
					p_3h < 0 ? "null" : (p_3h/100) + "" 
				}).replaceAll("'null'", "null");

				System.out.println("QUERY => " + query);
				
				try {
					this.stmt.executeUpdate(query);	
				} catch (SQLException e) {
				//	e.printStackTrace();
				}
			}	
		}
	}
	
	private double[] convertNetcdfValue(Object value) {
		
		if(value.getClass() == double[].class) {
			return (double[])value;
		} else if(value.getClass() == float[].class) {
			
			float[] f = (float[])value;
			
			double[] d = new double[f.length];
			
			for(int i=0 ; i<f.length ; i++) {
				d[i] = f[i];
			}
			
			return d;
			
		} else {
			return new double[]{-999};
		}
	}
}