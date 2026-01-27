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
public class UpdateKtgAirepVerify {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[] ktgModelDirPatterns = new String[]{"[0-9]{4}", "[0-9]{2}", "[0-9]{2}", "[0-9]{2}"};
	
	private final String getAirepDataFromOracleQuery = 
			
			"SELECT TO_CHAR(OBS_TM, 'YYYYMMDDHH24MISS') AS TM, STN_CD, ACFT_TYPE, LAT, LNGT, FLIGHT_LEVEL, TURB FROM AMISUSER.AIREP WHERE TM >= SYSDATE - 10 ORDER BY TM ASC";
		
	private final String insertKtgAirepVerifyOracleQuery = 
			
			" INSERT INTO AAMI.KTG_AIREP_VERIFY(TM, STN_CD, ACFT_TYPE, LAT, LNGT, FLIGHT_LEVEL, TURB, KTG_ISSUED_DT, KTG_FCST_DT, KTG_TURB) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24MISS''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', TO_DATE(''{7}'', ''YYYYMMDDHH24MISS''), TO_DATE(''{8}'', ''YYYYMMDDHH24MISS''), ''{9}'') "; 
	
	private ModelGridUtil modelGridUtil;
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private AmisDataBaseManager amisDbmanager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			String coordinatesLatPath = this.config.getString("ktg.coordinates.lat.path");
			String coordinatesLonPath = this.config.getString("ktg.coordinates.lon.path");
			
			modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KTG, null, coordinatesLatPath, coordinatesLonPath);
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			this.amisDbmanager = new AmisDataBaseManager(this.config);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : UpdateKtgAirepVerify.initialize -> " + e);
			
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
			
			System.out.println("Error : UpdateKtgAirepVerify.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		//storePath = "//172.26.56.115/data_store/"; 
		
		storePath = storePath + File.separator + "KTG";
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Get Airep Data :::::");
		
		List<Map<String, Object>> airepDataList = this.getAirepData();
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Insert Ktg Verify Data :::::");
		
		for(int i=0 ; i<airepDataList.size() ; i++) {
			
			Map<String, Object> airepData = airepDataList.get(i);
			
			try {
				
				Date tm = sdf.parse(((String)airepData.get("tm")));
				String stnCd = (String)airepData.get("stnCd");
				String acftType = (String)airepData.get("acftType");
				String lat = (String)airepData.get("lat");
				String lngt = (String)airepData.get("lngt");
				int flightLevel = Integer.valueOf((String)airepData.get("flightLevel"));
				String turb = (String)airepData.get("turb");
				
				//UTC 로 변환하자
				
				System.out.println(airepData);
				
				double latitude = Double.valueOf(lat)/100;
				double longitude = Double.valueOf(lngt)/100;
				
//				tm = new Date(tm.getTime() - (1000 * 60 * 60 * 9));
				
				this.modelGridUtil.setSingleGridBoundInfoforDistanceGrid(Double.valueOf(lngt)/100, Double.valueOf(lat)/100);
				
				BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
				
				double latInterval = Math.abs(boundLonLat.getTop() - boundLonLat.getBottom());
				double lonInterval = Math.abs(boundLonLat.getRight() - boundLonLat.getLeft());
				
				if(Math.abs(boundLonLat.getTop() - latitude) > latInterval || Math.abs(boundLonLat.getLeft() - longitude) > lonInterval) {
					System.out.println("Error : UpdateKtgAirepVerify.process -> position is too far");
					continue;
				}
				
				int flightIndex = 0;
				String fileSuffix = "midl";
				
				if(flightLevel/10 > 19) {
					flightIndex = flightLevel/10 - 19;
					fileSuffix = "uppl";
				} else {
					flightIndex = flightLevel/10;
					fileSuffix = "midl";
				}
				
				PointXY pointXY = this.modelGridUtil.getPointXY(Double.valueOf(lngt)/100, Double.valueOf(lat)/100);
					
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(flightIndex, flightIndex));
				rangeList.add(new Range(pointXY.getY(), pointXY.getY()));
				rangeList.add(new Range(pointXY.getX(), pointXY.getX()));
				
				List<Map<String, Object>> ktgModelFileInfoList = getKtgModelFileInfoListbyTm(storePath, tm);
				
				for(int j=0 ; j<ktgModelFileInfoList.size() ; j++) {
					
					Map<String, Object> ktgModelFileInfo = ktgModelFileInfoList.get(j);
					
					String ktgFilePath = (String)ktgModelFileInfo.get("ktgFilePath");
					
					Date issuedDt = (Date)ktgModelFileInfo.get("issuedDt");
					Date fcstDt = (Date)ktgModelFileInfo.get("fcstDt");
					
					ktgFilePath = ktgFilePath.replaceAll("midl", fileSuffix);
					
					NetcdfDataset ncFile = NetcdfDataset.acquireDataset(ktgFilePath, null);
					
					Variable var = ncFile.findVariable("KTG_AUC_20");
					
					float v = ((float[])var.read(rangeList).getStorage())[0];
					
					String query = MessageFormat.format(this.insertKtgAirepVerifyOracleQuery, new Object[]{
						sdf.format(tm),
						stnCd,
						acftType,
						lat,
						lngt,
						flightLevel,
						turb,
						sdf.format(issuedDt),
						sdf.format(fcstDt),
						v
					}).replaceAll("'null'", "null");

					System.out.println("QUERY => " + query);
					
					ncFile.close();
					
					this.dbManager.executeUpdate(query);					
				} 
				
			} catch (Exception e) {
				
				System.out.println("Error : UpdateKtgAirepVerify.process -> " + e);
				e.printStackTrace();
				continue;
			}
		}
		
		this.dbManager.commit();
		
		this.destroy();
	}
	
	private List<Map<String, Object>> getKtgModelFileInfoListbyTm(String storePath, Date tm) {
		
		List<Map<String, Object>> ktgModelFileInfoList = new ArrayList<Map<String, Object>>();
		
		File rootDir = new File(storePath);
		
		if(!rootDir.exists()) {
			return null;
		}
		
		recursiveProc(ktgModelFileInfoList, rootDir, tm, 0);
		
		return ktgModelFileInfoList;
	}
	
	private void recursiveProc(List<Map<String, Object>> ktgModelFileInfoList, final File baseDir, final Date tm, int depth) {
		
		File[] dirs = baseDir.listFiles();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		String pattern = this.ktgModelDirPatterns[depth];
		
		for(File dir : dirs) {
			
			if(dir.isDirectory() && dir.getName().matches(pattern)) {	
				
				if(depth == this.ktgModelDirPatterns.length-1) {
					
					try {
						
						String[] tokens = dir.getAbsolutePath().split("\\" + File.separator);
						int len = tokens.length;
						
						Date issuedDt = sdf.parse(tokens[len-4]+tokens[len-3]+tokens[len-2]+tokens[len-1]);
						
						File[] ktgModelFiles = dir.listFiles(new FilenameFilter() {

							@Override
							public boolean accept(File dir, String name) {
								
								if(name.matches("amo_gdum_ktgm_midl_f[0-9]{2}_[0-9]{10}.nc")) {
									return true;
								}
								
								return false;
							}
						});
						
						Arrays.sort(ktgModelFiles);
						
						for(int i=0 ; i<ktgModelFiles.length ; i++) {
							
							File ktgModelFile = ktgModelFiles[i];
							
							if(!new File(ktgModelFile.getAbsolutePath().replaceAll("midl", "uppl")).exists()) {
								continue;
							}
							
							Date fcstDt = new Date(issuedDt.getTime() + (1000 * 60 * 60) * 
									Integer.valueOf(ktgModelFile.getName().replaceAll("(.+f)([0-9]{2})(_[0-9]{10}.nc)", "$2")));
								
							float interval = (tm.getTime() - fcstDt.getTime());
								
							if(interval >= 0 && interval < 3 * 60 * 60 * 1000) {
								
								Map<String, Object> ktgModelFileInfo = new HashMap<String, Object>();
								
								ktgModelFileInfo.put("ktgFilePath", ktgModelFile.getAbsolutePath());
								ktgModelFileInfo.put("issuedDt", issuedDt);
								ktgModelFileInfo.put("fcstDt", fcstDt);
								
								ktgModelFileInfoList.add(ktgModelFileInfo);
							}
						}						
						
					} catch (Exception e) {
						
					}
					
				} else if(depth < this.ktgModelDirPatterns.length-1) {
					recursiveProc(ktgModelFileInfoList, dir, tm, depth+1);
				}
			}
		}
	}
	
	private List<Map<String, Object>> getAirepData() {

		List<Map<String, Object>> airepDataList = this.amisDbmanager.select(this.getAirepDataFromOracleQuery);
		
		return airepDataList;
	}
	
	public static void main(String[] args) {
	
		new UpdateKtgAirepVerify().process();
	}
}