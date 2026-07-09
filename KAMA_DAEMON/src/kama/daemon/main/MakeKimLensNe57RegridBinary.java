package kama.daemon.main;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.LineNumberReader;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class MakeKimLensNe57RegridBinary {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[] kimLensModelDirPatterns = new String[]{"[0-9]{4}", "[0-9]{2}", "[0-9]{2}", "[0-9]{2}"};
		
	private final String insertKimLensProcInfo = 
			
			" INSERT INTO AAMI.KIM_LENS_REGRID_PROC_INFO(ISSUED_DT, FCST_DT, MODEL_TYPE, PROC_TM) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24''), TO_DATE(''{1}'', ''YYYYMMDDHH24''), ''{2}'', SYSDATE) "; 
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private ModelGridUtil modelGridUtil;
	
	private FloatBuffer latitudeBuffer;
	private FloatBuffer longitudeBuffer;
	
	private Map<String, String> regridInfo = null;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			this.initCoordinates();
			
			readRegridInfo();
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : KimLensRegridBinaryGenerator.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void initCoordinates() {
		
		System.out.println("KimLensRegridBinaryGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.config.getString("kim_lens_ne57.coordinates.lat.path");
		String coordinatesLonPath = this.config.getString("kim_lens_ne57.coordinates.lon.path");
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_LENS_NE57, null, coordinatesLatPath, coordinatesLonPath);
		
		double[] mapBound = new double[]{50, 20, 110, 150};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		this.latitudeBuffer = modelGridUtil.getLatBuffer();
		this.longitudeBuffer = modelGridUtil.getLonBuffer();
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private void readRegridInfo() {
		
		System.out.println("\t-> Read KimLens Regrid Info");
		
		this.regridInfo = new HashMap<String, String>();
		
		File regridInfoFile = new File(String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/kim_lens_ne57_regrid_info.txt"));
		
		try {
			
			LineNumberReader reader = new LineNumberReader(new FileReader(regridInfoFile));
			
			String line = "";
			
			while((line = reader.readLine()) != null) {
				
				String key = line.split("\\|")[0];
				String value = line.split("\\|")[1];
				
				this.regridInfo.put(key, value);
			}
			
			reader.close();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");		
		
		if(!this.initialize()) {
			
			System.out.println("Error : KimLensRegridBinaryGenerator.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		//storePath = "//172.26.56.115/data_store/";
		
		Calendar cal = new GregorianCalendar();
		
		try {

//			cal.setTime(new Date());
			cal.setTime(sdf.parse("2026042812"));
			cal.add(Calendar.HOUR_OF_DAY, -24);
			
			Date startTm = cal.getTime();	
			
			this.processKimLensModel(storePath + File.separator + "KIM_LENS_UNIS_NE57", startTm);
			this.processKimLensModel(storePath + File.separator + "KIM_LENS_NE57", startTm);
			
		} catch (Exception e) {
			
		}
		
		this.destroy();
	}
	
	private void processKimLensModel(String storePath, Date startTm) {
		
		File rootDir = new File(storePath);
		
		if(!rootDir.exists()) {
			return;
		}
		
		fetchKimLensRecursive(rootDir, startTm, 0);
	}
	
	private void fetchKimLensRecursive(final File baseDir, Date startTm, int depth) {
		
		File[] dirs = baseDir.listFiles();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		String pattern = this.kimLensModelDirPatterns[depth];
		
		for(File dir : dirs) {
			
			if(dir.isDirectory() && dir.getName().matches(pattern)) {	
				
				if(depth == this.kimLensModelDirPatterns.length-1) {
					
					try {
						
						String[] tokens = dir.getAbsolutePath().split("\\" + File.separator);
						int len = tokens.length;
						
						Date issuedDt = sdf.parse(tokens[len-4]+tokens[len-3]+tokens[len-2]+tokens[len-1]);
						
						if(startTm.getTime() > issuedDt.getTime()) {
							continue;
						}
						
						File[] kimLensModelFiles = dir.listFiles(new FilenameFilter() {
							
							@Override
							public boolean accept(File dir, String name) {
								
								if(name.matches("l030_v040_m01_korea_(prs|etc).2byte.ft[0-9]{3}.[0-9]{10}.nc")) {
									return true;
								}
								
								return false;
							}
						});
						
						for(int i=0 ; i<kimLensModelFiles.length ; i++) {
							
							String modelType = kimLensModelFiles[i].getName().indexOf("etc") >= 0 ? "unis" : "pres";
							
							int fcstHour = Integer.valueOf(kimLensModelFiles[i].getName().split("\\.")[2].replaceAll("ft", ""));
								
							String savePath = kimLensModelFiles[i].getAbsolutePath().replaceAll(kimLensModelFiles[i].getName(), "");
							
							if("unis".equals(modelType)) {
								savePath = savePath.replaceAll("KIM_LENS_UNIS_NE57", "KIM_LENS_UNIS_NE57_BIN");
							} else {
								savePath = savePath.replaceAll("KIM_LENS_NE57", "KIM_LENS_NE57_BIN");
							}
							
							if(!new File(savePath).exists()) {
								new File(savePath).mkdirs();
							}
							
							this.generateBinary(kimLensModelFiles[i], modelType, issuedDt, fcstHour, savePath);
							
						}						
						
					} catch (Exception e) {
						
					}
					
				} else if(depth < this.kimLensModelDirPatterns.length-1) {
					fetchKimLensRecursive(dir, startTm, depth+1);
				}
			}
		}
	}
	
	public void generateBinary(File modelFile, String modelType, Date issuedDt, int fcstHour, String savePath) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		try {
			
			System.out.println("\t-> Target Model File [" + modelFile.getAbsolutePath() + "]");
			System.out.println("\t-> Open File");
			
			NetcdfDataset ncFile = NetcdfDataset.openDataset(modelFile.getAbsolutePath());
			
			String[][] settings = null;
			
			if("unis".equals(modelType)) {
				
				settings = new String[][]{
					{"VIS", "vis"}
				};				
				
			} else {
				
				settings = new String[][]{

				};
			}
			
			Calendar cal = new GregorianCalendar();
			cal.setTime(issuedDt);
			cal.add(Calendar.HOUR_OF_DAY, fcstHour);
			
			for(String[] setting : settings) {
				
				String layerName = setting[0];
				String aliasName = setting[1];
				
				switch(modelType) {
				
				
				case "unis":
					
					generateUnisBinaryFile(ncFile, savePath, modelType, layerName, aliasName, issuedDt, cal.getTime());
					
					break;
				}
			}
			
			String query = MessageFormat.format(this.insertKimLensProcInfo, new Object[]{
				sdf.format(issuedDt), sdf.format(cal.getTime()), modelType	
			});
			
			this.dbManager.executeUpdate(query);
			this.dbManager.commit();
			
			System.out.println("\t-> Close File");
			ncFile.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void generateUnisBinaryFile(NetcdfDataset ncFile, String savePath, String modelType, String layerName, String aliasName, Date issuedDt, Date fcstDt) throws Exception {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();			
		BoundXY boundXY = this.modelGridUtil.getBoundXY();
		
		int rows = this.modelGridUtil.getRows();
		int cols = this.modelGridUtil.getCols();
		
		Variable var = ncFile.findVariable(layerName);
		
		List<Range> rangeList = new ArrayList<Range>();
		rangeList.add(new Range(0, 0));					
		rangeList.add(new Range(boundXY.getBottom(), boundXY.getTop()));
		rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
		
		float[][] values = GridCalcUtil.convertStorageToPrimitiveValuesFromAttr(var, var.read(rangeList).getStorage(), rows, cols);
		
		Map<String, Object> regridData = this.getRegridData(values, boundLonLat, rows, cols);
		
		float[][] regridValues = (float[][])regridData.get("regridValues");
		
		String binaryFileName = savePath + File.separator + "kim_lens_" + modelType + "_ne57_regrid_" + aliasName + "_" + (sdf.format(issuedDt)) +  "_" + (sdf.format(fcstDt)) + "_00.bin";
		
		System.out.println("\t\t-> Write Binary [" + binaryFileName + "]");
		
		BufferedOutputStream dos = new BufferedOutputStream(new FileOutputStream(binaryFileName));
		
		for(int k=0 ; k<rows ; k++) {						
			for(int l=0 ; l<cols ; l++) {								
				dos.write(ByteBuffer.allocate(4).putFloat(regridValues[k][l]).array());
			}
		}
		
		dos.close();
	}
	
	private Map<String, Object> getRegridData(float[][] values, BoundLonLat maxBoundLonLat, int rows, int cols) {
		
		System.out.println("\t\t-> Start Regriding");
		
		double minLat = maxBoundLonLat.getBottom();
		double maxLat = maxBoundLonLat.getTop();
		double minLon = maxBoundLonLat.getLeft();
		double maxLon = maxBoundLonLat.getRight();
		
		double latTerm = (maxLat - minLat) / (rows-1);
		double lonTerm = (maxLon - minLon) / (cols-1);
		
		float[][] regridValues = new float[rows][];
		boolean[][] regridChecks = new boolean[rows][];
		
		for(int j=0 ; j<rows ; j++) {
			
			regridValues[j] = new float[cols];
			regridChecks[j] = new boolean[cols];
			
			for(int k=0 ; k<cols ; k++) {
				regridValues[j][k] = -999f;
				regridChecks[j][k] = false;
			}
		}
		
		for(int j=0 ; j<rows ; j++) {
			
			for(int k=0 ; k<cols ; k++) {
				
				float originLat = this.latitudeBuffer.get(j * cols + k);
				float originLon = this.longitudeBuffer.get(j * cols + k);
				
				int y = (int)((originLat - minLat) / latTerm);
				int x = (int)((originLon - minLon) / lonTerm);
				
				regridValues[y][x] = values[j][k];
				regridChecks[y][x] = true;
			}
		}
		
		for(int j=0 ; j<rows ; j++) {
			
			for(int k=0 ; k<cols ; k++) {
				
				if(regridChecks[j][k] == false) {
					
					double regridLat = minLat + latTerm * j;
					double regridLon = minLon + lonTerm * k;
					
					String[] regridInfoTokens = this.regridInfo.get(regridLat + "," + regridLon).split(",");
					
					float originLat = Float.valueOf(regridInfoTokens[0]);
					float originLon = Float.valueOf(regridInfoTokens[1]);
					int boundTop = Integer.valueOf(regridInfoTokens[2]);
					int boundLeft = Integer.valueOf(regridInfoTokens[3]);
					
					if(Math.abs(originLat - regridLat) < latTerm*3 && Math.abs(originLon - regridLon) < lonTerm*3) {
						regridValues[j][k] = values[boundTop][boundLeft];
					}	
				}
			}
		}
		
		Map<String, Object> regridData = new HashMap<String, Object>();
		regridData.put("regridValues", regridValues);
		regridData.put("latTerm", latTerm);
		regridData.put("lonTerm", lonTerm);
		
		System.out.println("\t\t-> End Regriding");
		
		return regridData;
	}
	
	public static void main(String[] args) {

		new MakeKimLensNe57RegridBinary().process();
	}
}