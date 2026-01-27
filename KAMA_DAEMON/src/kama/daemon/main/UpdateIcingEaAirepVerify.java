package kama.daemon.main;

import java.io.File;
import java.io.FilenameFilter;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kama.daemon.common.db.AmisDataBaseManager;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.PointXY;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

/**
 * @author jetddo
 * Created on 2017-01-03
 */
public class UpdateIcingEaAirepVerify {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[] icingEaModelDirPatterns = new String[]{"[0-9]{4}", "[0-9]{2}", "[0-9]{2}", "[0-9]{2}"};
	
	private final String getAirepDataFromOracleQuery = 
			
			"SELECT TO_CHAR(OBS_TM, 'YYYYMMDDHH24MISS') AS TM, STN_CD, ACFT_TYPE, LAT, LNGT, FLIGHT_LEVEL, ACFT_ICING_INTST FROM AMISUSER.AIREP WHERE TM >= SYSDATE - 10 ORDER BY TM ASC";
		
	private final String insertIcingEaAirepVerifyOracleQuery = 
			
			" INSERT INTO AAMI.ICING_EA_AIREP_VERIFY(TM, STN_CD, ACFT_TYPE, LAT, LNGT, FLIGHT_LEVEL, ICING, MODEL_ISSUED_DT, MODEL_FCST_DT, MODEL_ICING) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24MISS''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', TO_DATE(''{7}'', ''YYYYMMDDHH24MISS''), TO_DATE(''{8}'', ''YYYYMMDDHH24MISS''), ''{9}'') "; 
	
	private ModelGridUtil modelGridUtil;
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private AmisDataBaseManager amisDbmanager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			String coordinatesLatPath = this.config.getString("icing_ea.coordinates.lat.path");
			String coordinatesLonPath = this.config.getString("icing_ea.coordinates.lon.path");
			
			modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.ICING_EA, null, coordinatesLatPath, coordinatesLonPath);
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			this.amisDbmanager = new AmisDataBaseManager(this.config);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : UpdateIcingEaAirepVerify.initialize -> " + e);
			
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
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		if(!this.initialize()) {
			
			System.out.println("Error : UpdateIcingEaAirepVerify.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		storePath = storePath + File.separator + "ICING_EA";
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Get Airep Data :::::");
		
		List<Map<String, Object>> airepDataList = this.getAirepData();
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Insert IcingEa Verify Data :::::");
		
		for(int i=0 ; i<airepDataList.size() ; i++) {
			
			Map<String, Object> airepData = airepDataList.get(i);
			
			try {
				
				Date tm = sdf.parse(((String)airepData.get("tm")));
				String stnCd = (String)airepData.get("stnCd");
				String acftType = (String)airepData.get("acftType");
				String lat = (String)airepData.get("lat");
				String lngt = (String)airepData.get("lngt");
				int flightLevel = Integer.valueOf((String)airepData.get("flightLevel"));
				String icing = (String)airepData.get("acftIcingIntst");
				
				//UTC 로 변환하자
				
				double latitude = Double.valueOf(lat)/100;
				double longitude = Double.valueOf(lngt)/100;
				
//				tm = new Date(tm.getTime() - (1000 * 60 * 60 * 9));
				
				this.modelGridUtil.setSingleGridBoundInfoforDistanceGrid(Double.valueOf(lngt)/100, Double.valueOf(lat)/100);
				
				BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
				
				double latInterval = Math.abs(boundLonLat.getTop() - boundLonLat.getBottom());
				double lonInterval = Math.abs(boundLonLat.getRight() - boundLonLat.getLeft());
				
				if(Math.abs(boundLonLat.getTop() - latitude) > latInterval || Math.abs(boundLonLat.getLeft() - longitude) > lonInterval) {
					System.out.println("Error : UpdateIcingEaAirepVerify.process -> position is too far");
					continue;
				}
				
				int flightIndex = Math.min(40, flightLevel/10);
					
				PointXY pointXY = this.modelGridUtil.getPointXY(Double.valueOf(lngt)/100, Double.valueOf(lat)/100);
					
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(40-flightIndex, 40-flightIndex));
				rangeList.add(new Range(pointXY.getY(), pointXY.getY()));
				rangeList.add(new Range(pointXY.getX(), pointXY.getX()));
				
				List<Map<String, Object>> icingEaModelFileInfoList = getIcingEaModelFileInfoListbyTm(storePath, tm);
				
				for(int j=0 ; j<icingEaModelFileInfoList.size() ; j++) {
					
					Map<String, Object> icingEaModelFileInfo = icingEaModelFileInfoList.get(j);
					
					String icingEaFilePath = (String)icingEaModelFileInfo.get("icingEaFilePath");
					
					Date issuedDt = (Date)icingEaModelFileInfo.get("issuedDt");
					Date fcstDt = (Date)icingEaModelFileInfo.get("fcstDt");
					
					NetcdfDataset ncFile = NetcdfDataset.acquireDataset(icingEaFilePath, null);
					
					Variable var = ncFile.findVariable("RAP");
					
					float v = ((float[])var.read(rangeList).getStorage())[0];
					
					String query = MessageFormat.format(this.insertIcingEaAirepVerifyOracleQuery, new Object[]{
						sdf.format(tm),
						stnCd,
						acftType,
						lat,
						lngt,
						flightLevel,
						icing,
						sdf.format(issuedDt),
						sdf.format(fcstDt),
						v
					}).replaceAll("'null'", "null");

					System.out.println("QUERY => " + query);
					
					ncFile.close();
					
					this.dbManager.executeUpdate(query);
				} 
				
			} catch (Exception e) {
				
				System.out.println("Error : UpdateIcingEaAirepVerify.process -> " + e);
				e.printStackTrace();
				continue;
			}
		}
		
		this.dbManager.commit();
		
		this.destroy();
	}
	
	private List<Map<String, Object>> getIcingEaModelFileInfoListbyTm(String storePath, Date tm) {
		
		List<Map<String, Object>> icingEaModelFileInfoList = new ArrayList<Map<String, Object>>();
		
		File rootDir = new File(storePath);
		
		if(!rootDir.exists()) {
			return null;
		}
		
		recursiveProc(icingEaModelFileInfoList, rootDir, tm, 0);
		
		return icingEaModelFileInfoList;
	}
	
	private void recursiveProc(List<Map<String, Object>> icingEaModelFileInfoList, final File baseDir, final Date tm, int depth) {
		
		File[] dirs = baseDir.listFiles();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		String pattern = this.icingEaModelDirPatterns[depth];
		
		for(File dir : dirs) {
			
			if(dir.isDirectory() && dir.getName().matches(pattern)) {	
				
				if(depth == this.icingEaModelDirPatterns.length-1) {
					
					try {
						
						String[] tokens = dir.getAbsolutePath().split("\\" + File.separator);
						int len = tokens.length;
						
						Date issuedDt = sdf.parse(tokens[len-4]+tokens[len-3]+tokens[len-2]+tokens[len-1]);
						
						//(.+h)([0-9]{2})(.[0-9]{10}.nc)
						File[] icingEaModelFiles = dir.listFiles(new FilenameFilter() {

							@Override
							public boolean accept(File dir, String name) {
												
								if(name.matches("air_ice0_umrg_h[0-9]{2}.[0-9]{10}.nc")) {
									return true;
								}
								
								return false;
							}
						});
						
						Arrays.sort(icingEaModelFiles);
						
						for(int i=0 ; i<icingEaModelFiles.length ; i++) {
							
							File icingEaModelFile = icingEaModelFiles[i];
							
							Date fcstDt = new Date(issuedDt.getTime() + (1000 * 60 * 60) * 
									Integer.valueOf(icingEaModelFile.getName().replaceAll("(.+h)([0-9]{2})(.[0-9]{10}.nc)", "$2")));
								
							float interval = (tm.getTime() - fcstDt.getTime());
								
							if(interval >= 0 && interval < 3 * 60 * 60 * 1000) {
								
								Map<String, Object> icingEaModelFileInfo = new HashMap<String, Object>();
								
								icingEaModelFileInfo.put("icingEaFilePath", icingEaModelFile.getAbsolutePath());
								icingEaModelFileInfo.put("issuedDt", issuedDt);
								icingEaModelFileInfo.put("fcstDt", fcstDt);
								
								icingEaModelFileInfoList.add(icingEaModelFileInfo);
							}
						}						
						
					} catch (Exception e) {
						
					}
					
				} else if(depth < this.icingEaModelDirPatterns.length-1) {
					recursiveProc(icingEaModelFileInfoList, dir, tm, depth+1);
				}
			}
		}
	}
	
	private List<Map<String, Object>> getAirepData() {

		List<Map<String, Object>> airepDataList = this.amisDbmanager.select(this.getAirepDataFromOracleQuery);
		
		return airepDataList;
	}
	
	public static void main(String[] args) {
	
		new UpdateIcingEaAirepVerify().process();
	}
}