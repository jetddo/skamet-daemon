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

public class MakeKimRdpsRegridBinary {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[] kimRdpsModelDirPatterns = new String[]{"[0-9]{4}", "[0-9]{2}", "[0-9]{2}", "[0-9]{2}"};
		
	private final String insertKimRdpsProcInfo = 
			
			" INSERT INTO AAMI.KIM_RDPS_REGRID_PROC_INFO(ISSUED_DT, FCST_DT, MODEL_TYPE, PROC_TM) VALUES " + 
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
			
			System.out.println("Error : KimRdpsRegridBinaryGenerator.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void initCoordinates() {
		
		System.out.println("KimRdpsRegridBinaryGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.config.getString("kim_rdps.coordinates.lat.path");
		String coordinatesLonPath = this.config.getString("kim_rdps.coordinates.lon.path");
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_RDPS, null, coordinatesLatPath, coordinatesLonPath);
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		this.latitudeBuffer = modelGridUtil.getLatBuffer();
		this.longitudeBuffer = modelGridUtil.getLonBuffer();
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private void readRegridInfo() {
		
		System.out.println("\t-> Read KimRdps Regrid Info");
		
		this.regridInfo = new HashMap<String, String>();
		
		File regridInfoFile = new File(String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/kim_rdps_regrid_info.txt"));
		
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
			
			System.out.println("Error : KimRdpsRegridBinaryGenerator.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		//storePath = "//172.26.56.115/data_store/";
		
		Calendar cal = new GregorianCalendar();
		
		cal.setTime(new Date());
		cal.add(Calendar.HOUR_OF_DAY, -24);
		
		Date startTm = cal.getTime();	
		
		
		this.processKimRdpsPcModel(storePath, startTm);
		//this.processKimRdpsPbModel(storePath, startTm);
		
		this.destroy();
	}
	
	private void processKimRdpsPcModel(String storePath, Date startTm) {
		
		storePath = storePath + File.separator + "KIM_RDPS_UNIS";
		
		File rootDir = new File(storePath);
		
		if(!rootDir.exists()) {
			return;
		}
		
		fetchKimRdpsRecursive(rootDir, startTm, 0);
	}
	
	private void processKimRdpsPbModel(String storePath, Date startTm) {
		
		storePath = storePath + File.separator + "KIM_RDPS";
		
		File rootDir = new File(storePath);
		
		if(!rootDir.exists()) {
			return;
		}
		
		fetchKimRdpsRecursive(rootDir, startTm, 0);
	}
	
	private void fetchKimRdpsRecursive(final File baseDir, Date startTm, int depth) {
		
		File[] dirs = baseDir.listFiles();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		String pattern = this.kimRdpsModelDirPatterns[depth];
		
		for(File dir : dirs) {
			
			if(dir.isDirectory() && dir.getName().matches(pattern)) {	
				
				if(depth == this.kimRdpsModelDirPatterns.length-1) {
					
					try {
						
						String[] tokens = dir.getAbsolutePath().split("\\" + File.separator);
						int len = tokens.length;
						
						Date issuedDt = sdf.parse(tokens[len-4]+tokens[len-3]+tokens[len-2]+tokens[len-1]);
						
						if(startTm.getTime() > issuedDt.getTime()) {
							continue;
						}
						
						File[] kimRdpsModelFiles = dir.listFiles(new FilenameFilter() {

							@Override
							public boolean accept(File dir, String name) {
								
								if(name.matches("r030_v040_ne36_(pres|unis)_h[0-9]{3}.[0-9]{10}.gb2")) {
									return true;
								}
								
								return false;
							}
						});
						
						for(int i=0 ; i<kimRdpsModelFiles.length ; i++) {
							
							String modelType = kimRdpsModelFiles[i].getName().indexOf("unis") >= 0 ? "unis" : "pres";
							
							int fcstHour = Integer.valueOf(kimRdpsModelFiles[i].getName().split("_")[4].split("\\.")[0].replaceAll("h", ""));
								
							String savePath = kimRdpsModelFiles[i].getAbsolutePath().replaceAll(kimRdpsModelFiles[i].getName(), "");
							
							if("unis".equals(modelType)) {
								savePath = savePath.replaceAll("KIM_RDPS_UNIS", "KIM_RDPS_UNIS_BIN");
							} else {
								savePath = savePath.replaceAll("KIM_RDPS", "KIM_RDPS_BIN");
							}
							
							if(!new File(savePath).exists()) {
								new File(savePath).mkdirs();
							}
							
							this.generateBinary(kimRdpsModelFiles[i], modelType, issuedDt, fcstHour, savePath);
							
						}						
						
					} catch (Exception e) {
						
					}
					
				} else if(depth < this.kimRdpsModelDirPatterns.length-1) {
					fetchKimRdpsRecursive(dir, startTm, depth+1);
				}
			}
		}
	}
	
	public void generateBinary(File modelFile, String modelType, Date issuedDt, int fcstHour, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		try {
			
			System.out.println("\t-> Target Model File [" + modelFile.getAbsolutePath() + "]");
			System.out.println("\t-> Open File");
			
			NetcdfDataset ncFile = NetcdfDataset.openDataset(modelFile.getAbsolutePath());
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();			
			boundXY = this.modelGridUtil.getBoundXY();
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			String[][] settings = null;
			
			if("unis".equals(modelType)) {
				
				settings = new String[][]{
					
					{"Visibility_surface", "vis", "0"},
					{"Relative_humidity_height_above_ground", "rh", "1"},
					{"u-component_of_wind_height_above_ground", "u", "1"},
					{"v-component_of_wind_height_above_ground", "v", "1"},
					{"Temperature_height_above_ground", "temp", "1"}	
				};				
				
			} else {
				
				settings = new String[][]{
						
					{"Relative_humidity_isobaric", "rh", "1"},
					{"u-component_of_wind_isobaric", "u", "1"},
					{"v-component_of_wind_isobaric", "v", "1"}						
				};
			}
			
			Calendar cal = new GregorianCalendar();
			cal.setTime(issuedDt);
			cal.add(Calendar.HOUR_OF_DAY, fcstHour);
			
			for(String[] setting : settings) {
				
				String varName = setting[0];
				String legendName = setting[1];
				int p = Integer.valueOf(setting[2]);
				
				boolean useP = true;
				
				if(p == 0) {
					p = 1;
					useP = false;
				}
				
				for(int j=0 ; j<p ; j++) {
					
					Variable var = ncFile.findVariable(varName);
					
					List<Range> rangeList = new ArrayList<Range>();
					rangeList.add(new Range(0, 0));	
					
					if(useP) {
						rangeList.add(new Range(j, j));	
					}
											
					rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
					rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
					
					Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);
					
					Map<String, Object> regridData = this.getRegridData(values, boundLonLat, rows, cols);
					
					Float[][] regridValues = (Float[][])regridData.get("regridValues");
					
					String binaryFileName = savePath + File.separator + "kim_rdps_" + modelType + "_regrid_" + legendName + "_" + (sdf.format(issuedDt)) +  "_" + (sdf.format(cal.getTime())) + "_" + String.format("%02d", j) + ".bin";
					
					System.out.println("\t\t-> Write Binary [" + binaryFileName + "]");
					
					BufferedOutputStream dos = new BufferedOutputStream(new FileOutputStream(binaryFileName));
					
					for(int k=0 ; k<rows ; k++) {						
						for(int l=0 ; l<cols ; l++) {								
							dos.write(ByteBuffer.allocate(4).putFloat(regridValues[k][l]).array());
						}
					}
					
					dos.close();
				}
			}
			
			String query = MessageFormat.format(this.insertKimRdpsProcInfo, new Object[]{
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
	
	private Map<String, Object> getRegridData(Float[][] values, BoundLonLat maxBoundLonLat, int rows, int cols) {
		
		System.out.println("\t\t-> Start Regriding");
		
		String coordinatesLatPath = this.config.getString("kim_rdps.coordinates.lat.path");
		String coordinatesLonPath = this.config.getString("kim_rdps.coordinates.lon.path");
		
		ModelGridUtil modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_RDPS, null, coordinatesLatPath, coordinatesLonPath);
		
		double minLat = maxBoundLonLat.getBottom();
		double maxLat = maxBoundLonLat.getTop();
		double minLon = maxBoundLonLat.getLeft();
		double maxLon = maxBoundLonLat.getRight();
		
		double latTerm = (maxLat - minLat) / (rows-1);
		double lonTerm = (maxLon - minLon) / (cols-1);
		
		Float[][] regridValues = new Float[rows][];
		boolean[][] regridChecks = new boolean[rows][];
		
		for(int j=0 ; j<rows ; j++) {
			
			regridValues[j] = new Float[cols];
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

		new MakeKimRdpsRegridBinary().process();
	}
}