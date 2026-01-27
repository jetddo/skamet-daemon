package kama.daemon.main;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FilenameFilter;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class MakeUmLdpsImage {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[] ldpsModelDirPatterns = new String[]{"[0-9]{4}", "[0-9]{2}", "[0-9]{2}", "[0-9]{2}"};
		
	private final String insertUmLdpsProcInfo = 
			
			" INSERT INTO AAMI.UM_LDPS_PROC_INFO(ISSUED_DT, FCST_DT, MODEL_TYPE, PROC_TM) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24''), TO_DATE(''{1}'', ''YYYYMMDDHH24''), ''{2}'', SYSDATE) "; 
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private ModelGridUtil modelGridUtil;
	
	private LdpsLegend ldpsLegend;
	
	private final int imageExpandFactor = 10;
	private final int imageResizeFactor = 4;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			ldpsLegend = new LdpsLegend();
			
			this.initCoordinates();
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : MakeUmLdpsImage.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void initCoordinates() {
		
		System.out.println("LdpsImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.config.getString("um_loa.coordinates.lat.path");
		String coordinatesLonPath = this.config.getString("um_loa.coordinates.lon.path");
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS, null, coordinatesLatPath, coordinatesLonPath);
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");		
		
		if(!this.initialize()) {
			
			System.out.println("Error : MakeUmLdpsImage.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		//storePath = "//172.26.56.115/data_store/";
		
		Calendar cal = new GregorianCalendar();
		
		cal.setTime(new Date());
		cal.add(Calendar.HOUR_OF_DAY, -24);
		
		Date startTm = cal.getTime();	
		
		this.processLdpsPbModel(storePath, startTm);
		this.processLdpsPcModel(storePath, startTm);
		
		this.destroy();
	}
	
	private void processLdpsPbModel(String storePath, Date startTm) {
		
		storePath = storePath + File.separator + "UM_LOA";
		
		File rootDir = new File(storePath);
		
		if(!rootDir.exists()) {
			return;
		}
		
		fetchLdpsRecursive(rootDir, startTm, 0);
	}
	
	private void processLdpsPcModel(String storePath, Date startTm) {
		
		storePath = storePath + File.separator + "UM_LOA_PC";
		
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
								
								if(name.matches("qwumloa_(pc|pb)[0-9]{3}.nc")) {
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
							//String savePath = "C:/backup/datastore";
							
							if("pc".equals(modelType)) {
								savePath = savePath.replaceAll("UM_LOA_PC", "UM_LOA_PC_IMG");
							} else {
								savePath = savePath.replaceAll("UM_LOA", "UM_LOA_IMG");
							}
							
							if(!new File(savePath).exists()) {
								new File(savePath).mkdirs();
							}
							
							this.generateImage(ldpsModelFiles[i], modelType, issuedDt, startIdx, savePath);
							
						}						
						
					} catch (Exception e) {
						
					}
					
				} else if(depth < this.ldpsModelDirPatterns.length-1) {
					fetchLdpsRecursive(dir, startTm, depth+1);
				}
			}
		}
	}
	
	public void generateImage(File modelFile, String modelType, Date issuedDt, int startIdx, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		try {
			
			System.out.println("\t-> Target Model File [" + modelFile.getAbsolutePath() + "]");
			System.out.println("\t-> Open File");
			
			NetcdfDataset ncFile = NetcdfDataset.openDataset(modelFile.getAbsolutePath());
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();			
			boundXY = this.modelGridUtil.getBoundXY();
			
			List<Map<String, Object>> polygonDataList = this.modelGridUtil.getPolygonDataList();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			int t = modelFile.getName().endsWith("000.nc") ? 1 : 2;
			int p = "pc".equals(modelType) ? 1 : 10;
			
			String[][] settings = null;
			
			if("pc".equals(modelType)) {
				
				settings = new String[][]{
					
					{"field25", "vis"},
					{"field75", "cloudheight"}
				};				
				
			} else {
				
				settings = new String[][]{
						
					{"temp", "temp"},
					{"rh", "rh"},
					{"ws", "ws"}						
				};
			}
			
			for(int i=0 ; i<t ; i++) {
				
				for(int j=0 ; j<p ; j++) {
					
					for(String[] setting : settings) {
						
						String varName = setting[0];
						String legendName = setting[1];
						
						Variable var1 = null;
						Variable var2 = null;
						
						// 풍속만 예외처리
						if("ws".equals(varName)) {
							
							var1 = ncFile.findVariable("x-wind");
							var2 = ncFile.findVariable("y-wind");	
							
						} else {
							var1 = ncFile.findVariable(varName);
						}
						
						List<Range> rangeList = new ArrayList<Range>();
						rangeList.add(new Range(i, i));	
						rangeList.add(new Range(j, j));						
						rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
						rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
					
						Float[][] values1 = null;
						Float[][] values2 = null;
						
						// 풍속만 예외처리
						if("ws".equals(legendName)) {
							
							values1 = GridCalcUtil.convertStorageToValues(var1.read(rangeList).getStorage(), rows, cols);
							values2 = GridCalcUtil.convertStorageToValues(var2.read(rangeList).getStorage(), rows, cols);
							
						} else {
							
							values1 = GridCalcUtil.convertStorageToValues(var1.read(rangeList).getStorage(), rows, cols);
						}
							
						File imageFile = new File(savePath + File.separator + "um_loa_" + modelType + "_" + legendName + "_" + (startIdx+i) + "_" + j + ".png");
						
						BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
						
						Graphics2D ig2 = bi.createGraphics();
						
						System.out.println("\t\t-> Write Image [" + imageFile.getAbsolutePath() + "]");
						
						for(int k=0 ; k<polygonDataList.size() ; k++) {
							
							Map<String, Object> polygonData = polygonDataList.get(k);
							
							int x = Integer.valueOf(polygonData.get("x").toString());
							int y = Integer.valueOf(polygonData.get("y").toString());
								
							float v = 0f;
							
							if("ws".equals(legendName)) {

								float v1 = values1[y-boundXY.getBottom()][x-boundXY.getLeft()];
								float v2 = values2[y-boundXY.getBottom()][x-boundXY.getLeft()];
									
								v = (float)Math.sqrt(v1*v1 + v2*v2);
									
							} else if("temp".equals(legendName)) {
								
								v = values1[y-boundXY.getBottom()][x-boundXY.getLeft()] - 273.15f;
								
							} else if("vis".equals(legendName)) {
								
								v = values1[y-boundXY.getBottom()][x-boundXY.getLeft()];
								
							} else if("cloudheight".equals(legendName)) {
								
								v = values1[y-boundXY.getBottom()][x-boundXY.getLeft()];
								
							} else {
								v = values1[y-boundXY.getBottom()][x-boundXY.getLeft()];
							} 
							
							Color c = null;
							
							if("temp".equals(legendName)) {
								c = ldpsLegend.getTempColor(v);
							} else if("ws".equals(legendName)) {
								c = ldpsLegend.getWsColor(v);
							} else if("vis".equals(legendName)) {
								c = ldpsLegend.getVisColor(v);
							} else if("cloudheight".equals(legendName)) {
								c = ldpsLegend.getCloudHeightColor(v);
							} else if("rh".equals(legendName)) {
								c = ldpsLegend.getRhColor(v);
							} 
					
							if(c == null) {
								continue;
							}
							
							double[][] coordinates = (double[][])polygonData.get("coordinates");
								
							int[] xPoints = new int[coordinates.length];
							int[] yPoints = new int[coordinates.length];
							
							for(int m=0 ; m<coordinates.length ; m++) {									
								xPoints[m] = (int)Math.floor((coordinates[m][0] - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
								yPoints[m] = (int)Math.floor(mercatorRatio[(int)Math.floor((boundLonLat.getTop() - coordinates[m][1]) * this.imageExpandFactor * this.imageResizeFactor)]);
							}
							
							ig2.setColor(c);
							ig2.fillPolygon(xPoints, yPoints, coordinates.length);
						}
							
						ImageIO.write(bi, "PNG", imageFile);
												
					}					
				}
				
				Calendar cal = new GregorianCalendar();
				cal.setTime(issuedDt);
				cal.add(Calendar.HOUR_OF_DAY, startIdx+i);
				
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
	
	private class LdpsLegend {
		
		private Color[] rhColors = new Color[]{
			new Color(238,238,238),
			new Color(253,172,171),
			new Color(251,135,133),
			new Color(249,99,95),
			new Color(247,68,61),
			new Color(244,43,30),
			new Color(239,28,4),
			new Color(228,21,0),
			new Color(214,19,0),
			new Color(201,17,0),
			new Color(192,16,0),
			new Color(253,242,154),
			new Color(252,236,111),
			new Color(252,230,69),
			new Color(252,223,33),
			new Color(252,217,8),
			new Color(247,208,3),
			new Color(235,198,3),
			new Color(222,188,2),
			new Color(210,179,2),
			new Color(202,172,2),
			new Color(143,255,151),
			new Color(90,254,107),
			new Color(23,251,67),
			new Color(0,245,36),
			new Color(0,235,19),
			new Color(0,215,11),
			new Color(0,190,9),
			new Color(0,165,6),
			new Color(0,143,5),
			new Color(0,129,4),
			new Color(173,228,255),
			new Color(137,215,255),
			new Color(101,203,255),
			new Color(71,190,255),
			new Color(49,178,255),
			new Color(41,167,255),
			new Color(39,153,246),
			new Color(34,137,222),
			new Color(28,125,196),
			new Color(24,116,179),
			new Color(204,203,232),
			new Color(181,179,222),
			new Color(157,153,211),
			new Color(132,126,199),
			new Color(106,99,188),
			new Color(83,73,177),
			new Color(62,46,167),
			new Color(45,19,157),
			new Color(34,0,150),
			new Color(27,0,144),
			new Color(51,51,51)
		};
		
		public Color getTempColor(double value) {
			
			if(value > -100 && value <= 0) {
				return new Color(255,0,0);
			}
			
			return null;
		}
		
		public Color getWsColor(double value) {
			
			if(value >= 10 && value < 13) {
				return new Color(0,255,0);
			} else if(value >= 13 && value < 15) {
				return new Color(255,255,0);
			} else if(value >= 15 && value < 18) {
				return new Color(255,127,0);
			} else if(value >= 18 && value < 100) {
				return new Color(255,0,0);
			}
			
			return null;
		}
		
		public Color getVisColor(double value) {
			
			if(value >= 0 && value < 1600) {
				return new Color(255,0,0);
			} else if(value >= 1600 && value < 3200) {
				return new Color(255,255,0);
			} else if(value >= 3200 && value <= 4800) {
				return new Color(0,255,0);
			}
			
			return null;
		}
		
		public Color getCloudHeightColor(double value) {
			
			if(value >= 0 && value < 300) {
				return new Color(255,0,0);
			} else if(value >= 300 && value < 450) {
				return new Color(255,127,0);
			} else if(value >= 450 && value < 610) {
				return new Color(255,255,0);
			} else if(value >= 610 && value <= 760) {
				return new Color(0,255,0);
			} 
			
			return null;
		}
		
		public Color getRhColor(double value) {
			
			if(value <= 0) {
				return this.rhColors[0];
			} else if(value >= 100) {
				return this.rhColors[51];
			} else {				
				return this.rhColors[(int)(Math.floor(value/2))+1];
			}
		}
	}
	
	public static void main(String[] args) {

		new MakeUmLdpsImage().process();
	}
}