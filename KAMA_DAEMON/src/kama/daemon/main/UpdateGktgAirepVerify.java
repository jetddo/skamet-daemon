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
public class UpdateGktgAirepVerify {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[] gktgModelDirPatterns = new String[]{"[0-9]{4}", "[0-9]{2}", "[0-9]{2}", "[0-9]{2}"};
	
	private final String getAirepDataFromOracleQuery = 
			
			"SELECT TO_CHAR(OBS_TM, 'YYYYMMDDHH24MISS') AS TM, STN_CD, ACFT_TYPE, LAT, LNGT, FLIGHT_LEVEL, TURB FROM AMISUSER.AIREP WHERE TM >= SYSDATE - 10 ORDER BY TM ASC";
		
	private final String insertGktgAirepVerifyOracleQuery = 
			
			" INSERT INTO AAMI.GKTG_AIREP_VERIFY(TM, STN_CD, ACFT_TYPE, LAT, LNGT, FLIGHT_LEVEL, TURB, GKTG_ISSUED_DT, GKTG_FCST_DT, GKTG_TURB) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24MISS''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', TO_DATE(''{7}'', ''YYYYMMDDHH24MISS''), TO_DATE(''{8}'', ''YYYYMMDDHH24MISS''), ''{9}'') "; 
	
	private ModelGridUtil modelGridUtil;
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private AmisDataBaseManager amisDbmanager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			String coordinatesLatPath = this.config.getString("gktg.coordinates.lat.path");
			String coordinatesLonPath = this.config.getString("gktg.coordinates.lon.path");
			
			modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.GDPS, ModelGridUtil.Position.MIDDLE_CENTER, coordinatesLatPath, coordinatesLonPath, 180);
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			this.amisDbmanager = new AmisDataBaseManager(this.config);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : UpdateGktgAirepVerify.initialize -> " + e);
			
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
			
			System.out.println("Error : UpdateGktgAirepVerify.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		storePath = storePath + File.separator + "GKTG";
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Get Airep Data :::::");
		
		List<Map<String, Object>> airepDataList = this.getAirepData();
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Insert Gktg Verify Data :::::");
		
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
				
//				tm = new Date(tm.getTime() - (1000 * 60 * 60 * 9));
				
				this.modelGridUtil.setSingleGridBoundInfoforLatLonGrid(Double.valueOf(lngt)/100, Double.valueOf(lat)/100);
							
				int flightIndex = flightLevel/10;
				
				PointXY pointXY = this.modelGridUtil.getPointXY(Double.valueOf(lngt)/100, Double.valueOf(lat)/100);
					
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(flightIndex, flightIndex));
				rangeList.add(new Range(pointXY.getY(), pointXY.getY()));
				rangeList.add(new Range(pointXY.getX(), pointXY.getX()));
				
				List<Map<String, Object>> gktgModelFileInfoList = getGktgModelFileInfoListbyTm(storePath, tm);
				
				for(int j=0 ; j<gktgModelFileInfoList.size() ; j++) {
					
					Map<String, Object> gktgModelFileInfo = gktgModelFileInfoList.get(j);
					
					String gktgFilePath = (String)gktgModelFileInfo.get("gktgFilePath");
					
					Date issuedDt = (Date)gktgModelFileInfo.get("issuedDt");
					Date fcstDt = (Date)gktgModelFileInfo.get("fcstDt");
						
					NetcdfDataset ncFile = NetcdfDataset.acquireDataset(gktgFilePath, null);
					
					Variable var = ncFile.findVariable("GTGMAX");
					
					float v = ((float[])var.read(rangeList).getStorage())[0];
					
					String query = MessageFormat.format(this.insertGktgAirepVerifyOracleQuery, new Object[]{
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
				
				System.out.println("Error : UpdateGktgAirepVerify.process -> " + e);
				e.printStackTrace();
				continue;
			}
		}
		
		this.dbManager.commit();
		
		this.destroy();
	}
	
	private List<Map<String, Object>> getGktgModelFileInfoListbyTm(String storePath, Date tm) {
		
		List<Map<String, Object>> gktgModelFileInfoList = new ArrayList<Map<String, Object>>();
		
		File rootDir = new File(storePath);
		
		if(!rootDir.exists()) {
			return null;
		}
		
		recursiveProc(gktgModelFileInfoList, rootDir, tm, 0);
		
		return gktgModelFileInfoList;
	}
	
	private void recursiveProc(List<Map<String, Object>> gktgModelFileInfoList, final File baseDir, final Date tm, int depth) {
		
		File[] dirs = baseDir.listFiles();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		String pattern = this.gktgModelDirPatterns[depth];
		
		for(File dir : dirs) {
			
			if(dir.isDirectory() && dir.getName().matches(pattern)) {	
				
				if(depth == this.gktgModelDirPatterns.length-1) {
					
					try {
						
						String[] tokens = dir.getAbsolutePath().split("\\" + File.separator);
						int len = tokens.length;
						
						Date issuedDt = sdf.parse(tokens[len-4]+tokens[len-3]+tokens[len-2]+tokens[len-1]);
						
						File[] gktgModelFiles = dir.listFiles(new FilenameFilter() {

							@Override
							public boolean accept(File dir, String name) {
								
								if(name.matches("amo_gdum_gktg_max_f[0-9]{2}_[0-9]{10}.nc")) {
									return true;
								}
								
								return false;
							}
						});
						
						Arrays.sort(gktgModelFiles);
						
						for(int i=0 ; i<gktgModelFiles.length ; i++) {
							
							File gktgModelFile = gktgModelFiles[i];
							
							Date fcstDt = new Date(issuedDt.getTime() + (1000 * 60 * 60) * 
									Integer.valueOf(gktgModelFile.getName().replaceAll("(.+f)([0-9]{2})(_[0-9]{10}.nc)", "$2")));
								
							float interval = (tm.getTime() - fcstDt.getTime());
								
							if(interval >= 0 && interval < 3 * 60 * 60 * 1000) {
								
								Map<String, Object> gktgModelFileInfo = new HashMap<String, Object>();
								
								gktgModelFileInfo.put("gktgFilePath", gktgModelFile.getAbsolutePath());
								gktgModelFileInfo.put("issuedDt", issuedDt);
								gktgModelFileInfo.put("fcstDt", fcstDt);
								
								gktgModelFileInfoList.add(gktgModelFileInfo);
							}
						}						
						
					} catch (Exception e) {
						
					}
					
				} else if(depth < this.gktgModelDirPatterns.length-1) {
					recursiveProc(gktgModelFileInfoList, dir, tm, depth+1);
				}
			}
		}
	}
	
	private List<Map<String, Object>> getAirepData() {

		List<Map<String, Object>> airepDataList = this.amisDbmanager.select(this.getAirepDataFromOracleQuery);
		
		return airepDataList;
	}
	
	public static void main(String[] args) {
	
		new UpdateGktgAirepVerify().process();
	}
}