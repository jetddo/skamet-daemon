package kama.daemon.main;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.interpolation.MQInterpolation;
import kama.daemon.common.interpolation.ValuePoint;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.PointXY;

public class MakeAwsLdpsRegridBinary {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private final String insertAwsLdpsProcInfo = 
			
			" INSERT INTO AAMI.AWS_LDPS_REGRID_PROC_INFO(OBS_DT, PROC_TM) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24MI''), SYSDATE) "; 
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private ModelGridUtil modelGridUtil;
		
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			this.initCoordinates();
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : AwsLdpsRegridBinaryGenerator.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void initCoordinates() {
		
		System.out.println("AwsLdpsRegridBinaryGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.config.getString("um_loa_regrid.coordinates.lat.path");
		String coordinatesLonPath = this.config.getString("um_loa_regrid.coordinates.lon.path");
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS_REGRID, ModelGridUtil.Position.TOP_LEFT, coordinatesLatPath, coordinatesLonPath);
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");		
		
		if(!this.initialize()) {
			
			System.out.println("Error : AwsLdpsRegridBinaryGenerator.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		//storePath = "//172.26.56.115/data_store/";
		
		try {
			
			Calendar cal = new GregorianCalendar();
			
			cal.setTime(new Date());
			
			int min = cal.get(Calendar.MINUTE);
			min = ((int)min/10)*10;
			
			cal.set(Calendar.MINUTE, min);
			cal.add(Calendar.MINUTE, -30);
			
			for(int i=0 ; i<=3 ; i++) {
				
				cal.add(Calendar.MINUTE, 10);
				
				List<Map<String, Object>> resultList = this.getAwsDataList(cal.getTime());
				
				makeMQIAwsVisBinary(resultList, storePath, cal.getTime());
				makeMQIAwsCloudHeightBinary(resultList, storePath, cal.getTime());
				makeMQIAwsWindBinary(resultList, storePath, cal.getTime());
				makeMQIAwsCloudAmountBinary(resultList, storePath, cal.getTime());
				
				String query = MessageFormat.format(this.insertAwsLdpsProcInfo, new Object[]{
					sdf.format(cal.getTime())	
				});
				
				this.dbManager.executeUpdate(query);
				this.dbManager.commit();
			}
			
			this.destroy();
			
		} catch (Exception e) {
			e.printStackTrace();	
		}
	}
	
	private void makeMQIAwsVisBinary(List<Map<String, Object>> resultList, String storePath, Date time) throws Exception {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");	
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");	
		
		List<ValuePoint> valuePointList = new ArrayList<ValuePoint>();
		
		for(int i=0 ; i<resultList.size() ; i++) {
		
			String stnId = (String)resultList.get(i).get("STN_ID");
			String latitude = (String)resultList.get(i).get("LAT");
			String longitude = (String)resultList.get(i).get("LON");
			String vis = (String)resultList.get(i).get("VI");
			
			if(vis != null) {		
				
				double _vis = Double.valueOf(vis);
				
				if(_vis <= 0) {
					continue;
				}
				
				valuePointList.add(new ValuePoint(Float.valueOf(longitude), Float.valueOf(latitude), Float.valueOf(vis)));
			}
		}
		
		String savePath = storePath + File.separator + "AWS_REGRID_LDPS_BIN" + File.separator + new SimpleDateFormat("yyyy/MM/dd").format(time);
			
		String binaryFileName = "aws_ldps_regrid_vis_" + (sdf.format(time)) + ".bin";
		
		createMQIBinaryFile(valuePointList, savePath, binaryFileName);
	}
	
	private void makeMQIAwsWindBinary(List<Map<String, Object>> resultList, String storePath, Date time) throws Exception {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");	
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");	
		
		List<ValuePoint> valuePointList1 = new ArrayList<ValuePoint>();
		List<ValuePoint> valuePointList2 = new ArrayList<ValuePoint>();
		
		for(int i=0 ; i<resultList.size() ; i++) {
		
			String stnId = (String)resultList.get(i).get("STN_ID");
			String latitude = (String)resultList.get(i).get("LAT");
			String longitude = (String)resultList.get(i).get("LON");
			String wd = (String)resultList.get(i).get("WD");
			String ws = (String)resultList.get(i).get("WS");
			
			if(wd != null && ws != null) {	
				
				double _wd = Double.valueOf(wd);
				double _ws = Double.valueOf(ws);
				
				// 풍속이 0인 경우는 없음
				if(_ws <= 0) {
					continue;
				}
				
				_ws = _ws/10;
				_wd = _wd/10;
					
				double u = -_ws*Math.sin(Math.PI/180*_wd);
				double v = -_ws*Math.cos(Math.PI/180*_wd);
				
				valuePointList1.add(new ValuePoint(Float.valueOf(longitude), Float.valueOf(latitude), (float)u));
				valuePointList2.add(new ValuePoint(Float.valueOf(longitude), Float.valueOf(latitude), (float)v));
			}
		}
		
		String savePath = storePath + File.separator + "AWS_REGRID_LDPS_BIN" + File.separator + new SimpleDateFormat("yyyy/MM/dd").format(time);
			
		String binaryFileName1 = "aws_ldps_regrid_u_" + (sdf.format(time)) + ".bin";
		String binaryFileName2 = "aws_ldps_regrid_v_" + (sdf.format(time)) + ".bin";
		
		createMQIBinaryFile(valuePointList1, savePath, binaryFileName1);
		createMQIBinaryFile(valuePointList2, savePath, binaryFileName2);
	}
	
	private void makeMQIAwsCloudHeightBinary(List<Map<String, Object>> resultList, String storePath, Date time) throws Exception {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");	
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");	
		
		List<ValuePoint> valuePointList = new ArrayList<ValuePoint>();
		
		for(int i=0 ; i<resultList.size() ; i++) {
		
			String stnId = (String)resultList.get(i).get("STN_ID");
			String latitude = (String)resultList.get(i).get("LAT");
			String longitude = (String)resultList.get(i).get("LON");
			String chLyr1 = (String)resultList.get(i).get("CH_LYR_1");
			
			if(chLyr1 != null) {	
				
				double _chLyr1 = Double.valueOf(chLyr1);
				
				if(_chLyr1 <= 0) {
					continue;
				}
				
				valuePointList.add(new ValuePoint(Float.valueOf(longitude), Float.valueOf(latitude), Float.valueOf(chLyr1)));
			}
		}
		
		String savePath = storePath + File.separator + "AWS_REGRID_LDPS_BIN" + File.separator + new SimpleDateFormat("yyyy/MM/dd").format(time);
			
		String binaryFileName = "aws_ldps_regrid_cloudheight_" + (sdf.format(time)) + ".bin";
		
		createMQIBinaryFile(valuePointList, savePath, binaryFileName);
	}
	
	private void makeMQIAwsCloudAmountBinary(List<Map<String, Object>> resultList, String storePath, Date time) throws Exception {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");	
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");	
		
		List<ValuePoint> valuePointList = new ArrayList<ValuePoint>();
		
		for(int i=0 ; i<resultList.size() ; i++) {
		
			String stnId = (String)resultList.get(i).get("STN_ID");
			String latitude = (String)resultList.get(i).get("LAT");
			String longitude = (String)resultList.get(i).get("LON");
			String caTot = (String)resultList.get(i).get("CA_TOT");
			
			if(caTot != null) {		
				
				double _caTot = Double.valueOf(caTot);
				
				if(_caTot <= 0) {
					continue;
				}
				
				valuePointList.add(new ValuePoint(Float.valueOf(longitude), Float.valueOf(latitude), Float.valueOf(caTot)));
			}
		}
		
		String savePath = storePath + File.separator + "AWS_REGRID_LDPS_BIN" + File.separator + new SimpleDateFormat("yyyy/MM/dd").format(time);
			
		String binaryFileName = "aws_ldps_regrid_cloudamount_" + (sdf.format(time)) + ".bin";
		
		createMQIBinaryFile(valuePointList, savePath, binaryFileName);
	}
	
	private void createMQIBinaryFile(List<ValuePoint> valuePointList, String savePath, String binaryFileName) throws Exception {
	
		MQInterpolation mqi = this.createMqi(valuePointList);
		
		float[][] values = mqi.interpolate();
		
		if(!new File(savePath).exists()) {
			new File(savePath).mkdirs();
		}
		
		System.out.println("\t\t-> Write Binary [" + savePath + File.separator + binaryFileName + "]");
		
		BufferedOutputStream dos = new BufferedOutputStream(new FileOutputStream(savePath + File.separator + binaryFileName));
		
		for(int k=0 ; k<values.length-1 ; k++) {						
			for(int l=0 ; l<values[k].length-1 ; l++) {	
				dos.write(ByteBuffer.allocate(4).putFloat(values[k][l]).array());
			}
		}
		
		dos.close();
	}
	
	// AWS 요소 내삽 자료 생성
	private List<Map<String, Object>> getAwsDataList(Date time) {
		
		try {
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			
			String query = 
				" SELECT A.TM AS TIME, 			"+ 
				" A.STN_ID, A.D24 AS CH_LYR_1, 	"+
				" A.D02 AS WS, 					"+
				" A.D01 AS WD, 					"+
				" A.D28 AS VI, 					"+
				" B.LON, 						"+
				" B.LAT 						"+
				" FROM AAMI.KMA_NPH_AWS3_MIN A 	"+			
				" INNER JOIN AAMI.STN_OBS B 	"+
				" ON A.STN_ID = B.STN_ID	 	"+
				" WHERE A.TM = TO_DATE('" + sdf.format(time) + "', 'YYYYMMDDHH24MI') ";
			
			ResultSet resultSet = this.dbManager.executeQuery(query);
			
			List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
			
			while (resultSet.next()) {
				resultList.add(DaemonUtils.getResultSetData(resultSet));
			}
			
			return resultList;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private MQInterpolation createMqi(List<ValuePoint> valuePointList) {
		
		System.out.println("-> Start Initailize MQInterpolation");
		
		for(int i=0 ; i<valuePointList.size() ; i++) {
			
			ValuePoint valuePoint = valuePointList.get(i);
			
			PointXY pointXY = this.modelGridUtil.getPointXY(valuePoint.x, valuePoint.y);
			
			valuePoint.x = pointXY.getX();
			valuePoint.y = pointXY.getY();
		}
		
		System.out.println("-> End Initailize MQInterpolation");
		
		return new MQInterpolation(this.modelGridUtil.getModelWidth(), this.modelGridUtil.getModelHeight(), valuePointList);
	}
	
	public static void main(String[] args) {

		new MakeAwsLdpsRegridBinary().process();
	}
}