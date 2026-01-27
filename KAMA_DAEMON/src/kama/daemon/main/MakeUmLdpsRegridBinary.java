package kama.daemon.main;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
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

public class MakeUmLdpsRegridBinary {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[] ldpsModelDirPatterns = new String[]{"[0-9]{4}", "[0-9]{2}", "[0-9]{2}", "[0-9]{2}"};
		
	private final String insertUmLdpsProcInfo = 
			
			" INSERT INTO AAMI.UM_LDPS_REGRID_PROC_INFO(ISSUED_DT, FCST_DT, MODEL_TYPE, PROC_TM) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24''), TO_DATE(''{1}'', ''YYYYMMDDHH24''), ''{2}'', SYSDATE) "; 
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private ModelGridUtil modelGridUtil;
	
	private FloatBuffer latitudeBuffer;
	private FloatBuffer longitudeBuffer;
	
	private Map<String, Integer> recoveryMap;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			this.initCoordinates();
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : LdpsRegridBinaryGenerator.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void initCoordinates() {
		
		System.out.println("LdpsRegridBinaryGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.config.getString("um_loa.coordinates.lat.path");
		String coordinatesLonPath = this.config.getString("um_loa.coordinates.lon.path");
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS, null, coordinatesLatPath, coordinatesLonPath);
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		this.latitudeBuffer = modelGridUtil.getLatBuffer();
		this.longitudeBuffer = modelGridUtil.getLonBuffer();
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");		
		
		if(!this.initialize()) {
			
			System.out.println("Error : LdpsRegridBinaryGenerator.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		//storePath = "//172.26.56.115/data_store/";
		
		Calendar cal = new GregorianCalendar();
		
		cal.setTime(new Date());
		cal.add(Calendar.HOUR_OF_DAY, -24);
		
		Date startTm = cal.getTime();	
		
		this.processLdpsPcModel(storePath, startTm);
		//this.processLdpsPbModel(storePath, startTm);
		
		this.destroy();
	}
	
	private void processLdpsPcModel(String storePath, Date startTm) {
		
		storePath = storePath + File.separator + "UM_LOA_PC";
		
		File rootDir = new File(storePath);
		
		if(!rootDir.exists()) {
			return;
		}
		
		fetchLdpsRecursive(rootDir, startTm, 0);
	}
	
	private void processLdpsPbModel(String storePath, Date startTm) {
		
		storePath = storePath + File.separator + "UM_LOA";
		
		File rootDir = new File(storePath);
		
		if(!rootDir.exists()) {
			return;
		}
		
		fetchLdpsRecursive(rootDir, startTm, 0);
	}
	
	private void fetchLdpsRecursive(final File baseDir, Date startTm, int depth) {
		
		File[] dirs = baseDir.listFiles();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		String pattern = this.ldpsModelDirPatterns[depth];
		
		for(File dir : dirs) {
			
			if(dir.isDirectory() && dir.getName().matches(pattern)) {	
				
				if(depth == this.ldpsModelDirPatterns.length-1) {
					
					try {
						
						String[] tokens = dir.getAbsolutePath().split("\\" + File.separator);
						int len = tokens.length;
						
						Date issuedDt = sdf.parse(tokens[len-4]+tokens[len-3]+tokens[len-2]+tokens[len-1]);
						
						if(startTm.getTime() > issuedDt.getTime()) {
							continue;
						}
						
						File[] ldpsModelFiles = dir.listFiles(new FilenameFilter() {

							@Override
							public boolean accept(File dir, String name) {
								
								if(name.matches("qwumloa_(pc|pb)(000|002|004|006|008|010|012|014|016|018|020|022|024).nc")) {
									return true;
								}
								
								return false;
							}
						});
						
						for(int i=0 ; i<ldpsModelFiles.length ; i++) {
							
							String modelType = ldpsModelFiles[i].getName().indexOf("pc") >= 0 ? "pc" : "pb";
							
							int startIdx = Integer.valueOf(ldpsModelFiles[i].getName().split("\\.")[0].split(modelType)[1]);
							
							if(startIdx > 0) {
								startIdx--;
							}
							
							String savePath = ldpsModelFiles[i].getAbsolutePath().replaceAll(ldpsModelFiles[i].getName(), "");
							
							if("pc".equals(modelType)) {
								savePath = savePath.replaceAll("UM_LOA_PC", "UM_LOA_PC_BIN");
							} else {
								savePath = savePath.replaceAll("UM_LOA", "UM_LOA_BIN");
							}
							
							if(!new File(savePath).exists()) {
								new File(savePath).mkdirs();
							}
							
							this.generateBinary(ldpsModelFiles[i], modelType, issuedDt, startIdx, savePath);
							
						}						
						
					} catch (Exception e) {
						
					}
					
				} else if(depth < this.ldpsModelDirPatterns.length-1) {
					fetchLdpsRecursive(dir, startTm, depth+1);
				}
			}
		}
	}
	
	public void generateBinary(File modelFile, String modelType, Date issuedDt, int startIdx, String savePath) {
		
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
			
			int t = modelFile.getName().endsWith("000.nc") ? 1 : 2;
			int p = "pc".equals(modelType) ? 1 : 1;
			
			String[][] settings = null;
			
			if("pc".equals(modelType)) {
				
				settings = new String[][]{
					
					
					{"field25", "vis"},
					{"field30", "cloudamount"},
					{"field75", "cloudheight"},
					{"temp", "temp"},
					{"x-wind", "u"},
					{"y-wind", "v"}	
				};				
				
			} else {
				
				settings = new String[][]{
						
					{"rh", "rh"},
					{"x-wind", "u"},
					{"y-wind", "v"}						
				};
			}
			
			for(int i=0 ; i<t ; i++) {
					
				Calendar cal = new GregorianCalendar();
				cal.setTime(issuedDt);
				cal.add(Calendar.HOUR_OF_DAY, startIdx+i);
				
				for(int j=0 ; j<p ; j++) {
					
					for(String[] setting : settings) {
						
						String varName = setting[0];
						String legendName = setting[1];
						
						Variable var = ncFile.findVariable(varName);
						
						List<Range> rangeList = new ArrayList<Range>();
						rangeList.add(new Range(i, i));	
						rangeList.add(new Range(j, j));						
						rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
						rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
						
						Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);
						
						Map<String, Object> regridData = this.getRegridData(values, boundLonLat, rows, cols);
						
						Float[][] regridValues = (Float[][])regridData.get("regridValues");
						
						String binaryFileName = savePath + File.separator + "um_loa_" + modelType + "_regrid_" + legendName + "_" + (sdf.format(issuedDt)) +  "_" + (sdf.format(cal.getTime())) + "_" + String.format("%02d", j) + ".bin";
						
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
				
				String query = MessageFormat.format(this.insertUmLdpsProcInfo, new Object[]{
					sdf.format(issuedDt), sdf.format(cal.getTime()), modelType	
				});
				
				this.dbManager.executeUpdate(query);
				this.dbManager.commit();
			}
			
			System.out.println("\t-> Close File");
			ncFile.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private Map<String, Object> getRegridData(Float[][] values, BoundLonLat maxBoundLonLat, int rows, int cols) {
		
		System.out.println("\t\t-> Start Regriding");
		
		String coordinatesLatPath = this.config.getString("um_loa.coordinates.lat.path");
		String coordinatesLonPath = this.config.getString("um_loa.coordinates.lon.path");
		
		ModelGridUtil modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS, null, coordinatesLatPath, coordinatesLonPath);
		
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
					
					modelGridUtil.setSingleGridBoundInfoforDistanceGrid(regridLon, regridLat);
					
					BoundLonLat boundLonLat = modelGridUtil.getBoundLonLat();
					BoundXY boundXY = modelGridUtil.getBoundXY();
					
					float originLat = (float)boundLonLat.getTop();
					float originLon = (float)boundLonLat.getLeft();
					
					if(Math.abs(originLat - regridLat) < latTerm*3 && Math.abs(originLon - regridLon) < lonTerm*3) {
							
						regridValues[j][k] = values[boundXY.getTop()][boundXY.getLeft()];
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

		new MakeUmLdpsRegridBinary().process();
	}
}