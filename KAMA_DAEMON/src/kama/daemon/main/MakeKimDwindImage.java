package kama.daemon.main;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

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
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class MakeKimDwindImage {
	
	private String[] stnCdList = new String[] {
		"RKNY",
		"RKJB",
		"RKJY",
		"RKJJ",
		"RKPC",
		"RKPK",
		"RKPS",
		"RKPU",
		"RKSI",
		"RKSS",
		"RKTH",
		"RKTN",
		"RKTU"
	};
	
	
	private static BufferedImage[][] wtailImageList;
	
	private final int imageExpandFactor = 2500;
	private final int imageResizeFactor = 1;
	
	private float wTailRatio = 0.025f;
	private int wTailWidth = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int wTailHeight = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	
	private String uVarName = "U";
	private String vVarName = "V";
	
	private Map<String, ModelGridUtil> modelGridUtilMap = null;
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private final String insertFileProcInfo = 
			
			" INSERT INTO AAMI.STORED_FILE_PROC_H(FILE_DT, FILE_NAME, FILE_PATH, PROC_DT, FILE_CD) VALUES " + 
			" (TO_DATE('{fileDt}', 'YYYYMMDDHH24MI'), '{fileName}', '{filePath}', SYSDATE, 'KIM_DWIND') "; 
	
	private final String selectFileProcInfoList = 
			
			" SELECT 												"+
			" 	TO_CHAR((FILE_DT), 'YYYYMMDDHH24') AS FILE_DT,		"+
			" 	FILE_NAME											"+
			" FROM AAMI.STORED_FILE_PROC_H							"+
			" WHERE FILE_DT >= TO_DATE('{targetDt}', 'YYYYMMDD')	"+
			" AND FILE_CD = 'KIM_DWIND'								";
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private boolean initialize() {
		
		this.modelGridUtilMap = new HashMap<String, ModelGridUtil>();
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
				
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : MakeKimDwindImage.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		this.initCoordinates();
		
		return true;
	}
	
	private void initCoordinates() {
		
		System.out.println("MakeKimDwindImage [ Initailize Coordinate Systems ]");
		
		String latPath = config.getString("kim_dwind.coordinates.lat.path");
		String lonPath = config.getString("kim_dwind.coordinates.lon.path");
		
		double[] mapBound = new double[]{50, 20, 110, 140};
		
		for(int i=0 ; i<this.stnCdList.length ; i++) {
			
			String stnCd = this.stnCdList[i];
			
			System.out.println("\t-> Initailize [" + stnCd + "] Coordinate Systems");
			
			String _latPath = latPath.replace("stnCd", stnCd.toLowerCase());
			String _lonPath = lonPath.replace("stnCd", stnCd.toLowerCase());
			
			ModelGridUtil modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.valueOf("KLDW_" + stnCd), ModelGridUtil.Position.MIDDLE_CENTER, _latPath, _lonPath);
			modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
			
			System.out.println(modelGridUtil.getBoundXY());
			System.out.println(modelGridUtil.getBoundLonLat());
			
			this.modelGridUtilMap.put(stnCd, modelGridUtil);
		}
		
		System.out.println("MakeKimDwindImage [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KimDwind Image Grid List");
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");		
	}
	
	private void destroy() {
		this.dbManager.safeClose();
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd/HH"); // 날짜 형식만 필요할 때 사용	
		
		if(!this.initialize()) {
			
			System.out.println("Error : MakeKimDwindImage.process -> initialize failed");
			return;
		}
		
		final String kimDwindFilter = this.config.getString("kim_dwind.filter");
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		//storePath = "\\\\172.26.56.124\\data_store";
		
		try {
						
			Calendar cal = new GregorianCalendar();
						
			cal.setTime(new Date());				
			cal.add(Calendar.HOUR_OF_DAY, -9-12);
			cal.add(Calendar.HOUR_OF_DAY, -cal.get(Calendar.HOUR_OF_DAY)%6); // 6시간 단위로 맞추기 위해 현재 시간을 6시간 단위로 내림 처리
			// 하루전꺼부터 체크
			
			String targetDtStr = sdf.format(cal.getTime());
			
			String query = this.selectFileProcInfoList.replaceAll("\\{targetDt\\}", targetDtStr);	
			
			List<Map<String, Object>> parsedFileInfoList = new ArrayList<Map<String, Object>>();
			
			ResultSet resultSet = dbManager.executeQuery(query);
			
			while(resultSet.next()) {
				
				Map<String, Object> parsedFileInfo = DaemonUtils.getCamelcaseResultSetData(resultSet);				
				parsedFileInfoList.add(parsedFileInfo);
			}
			
			for(int i=0 ; i<3 ; i++) {
				
				Date targetDt = cal.getTime();			
				
				String targetDirStr = storePath + File.separator + "/KIM_DWIND/" + sdf3.format(targetDt);
				
				File targetDir = new File(targetDirStr);
				
				System.out.print("Target Dir : " + targetDir.getAbsolutePath());
			
				if(targetDir.exists()) {
					
					System.out.println(" -> exist, checked");
					
					File[] kimDwindFiles = targetDir.listFiles(new FilenameFilter(){

						@Override
						public boolean accept(File dir, String name) {
														
							if(name.matches(kimDwindFilter)) {								
								return true;	
							} 
							
							return false;
						}
					});
					
					
					List<File> kimDwindFileList = new ArrayList<File>();
					
					for(File kimDwindFile : kimDwindFiles) {
						
						boolean isParsed = false;
						
						for(int j=0 ; j<parsedFileInfoList.size() ; j++) {
							
							Map<String, Object> parsedFileInfo = parsedFileInfoList.get(j);
							
							Date fileDt = sdf2.parse(parsedFileInfo.get("fileDt").toString());
							String fileName = parsedFileInfo.get("fileName").toString();
							
							if(kimDwindFile.getName().equals(fileName)) {
								isParsed = true;
							}
						}
						
						if(!isParsed) {
							kimDwindFileList.add(kimDwindFile);			
						}
					}
					
					kimDwindFileList.sort(new Comparator<File>() {

						@Override
						public int compare(File arg0, File arg1) {							
							return arg0.getName().compareTo(arg1.getName());
						}
					});
					
					this.drawKimDwindImages(kimDwindFileList, storePath);
					
				} else {
					System.out.println(" -> not exist");
				}
				
				cal.add(Calendar.HOUR_OF_DAY, 6);	
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		this.destroy();
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End :::::");
	}
	
	public void drawKimDwindImages(List<File> kimDwindFileList, String storePath) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd/HH"); // 날짜 형식만 필요할 때 사용	
				
		System.out.println("MakeKimDwindImage [ Start Create Images ]");		
		
		for(int i=0 ; i<kimDwindFileList.size() ; i++) {
			
			File kimDwindFile = kimDwindFileList.get(i);
			
			System.out.println("Target File : " + kimDwindFile.getAbsolutePath());
			
			try {
				
				NetcdfDataset ncFile = NetcdfDataset.acquireDataset(kimDwindFile.getAbsolutePath(), null);
				String fileName = kimDwindFile.getName();
				
				String stnCd = fileName.split("_")[3];
				
				Date fileDt = sdf2.parse(fileName.split("_")[5].split("\\.")[0]);
				
		        String savePath = storePath + "/KIM_DWIND_IMG/" + sdf3.format(fileDt) ;
		        //String savePath = "C:/data/datastore//KIM_DWIND_IMG/" + sdf3.format(fileDt) ;
		        
		        File saveDir = new File(savePath);
		        
		        if(!saveDir.exists()) {
		        	saveDir.mkdirs();
		        }		      
		        
		        if(generateImage(ncFile, stnCd, fileName, savePath) != null) {
		        	
		        	String query = this.insertFileProcInfo.replaceAll("\\{fileDt\\}", sdf2.format(fileDt))		        			
														  .replaceAll("\\{fileName\\}", fileName)
														  .replaceAll("\\{filePath\\}", kimDwindFile.getAbsolutePath());

					this.dbManager.executeQuery(query);
					this.dbManager.commit();
					
		        } else {
		        	this.dbManager.rollback();
		        }
				
				ncFile.close();
				
			} catch (Exception e) {
				
			}
		}
		
		System.out.println("MakeKimDwindImage [ End Create Images ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String stnCd, String fileName, String savePath) {

		ModelGridUtil modelGridUtil = this.modelGridUtilMap.get(stnCd);
		
		if(modelGridUtil == null) {
			return null;
		}
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			createWtailImages();
			
			boundLonLat = modelGridUtil.getBoundLonLat();			
			boundXY = modelGridUtil.getBoundXY();
			
			List<Map<String, Object>> polygonDataList = modelGridUtil.getPolygonDataList();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = modelGridUtil.getRows();
			int cols = modelGridUtil.getCols();
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
	
			System.out.println("\t-> Process Attribute ["+uVarName+"," +vVarName+"]");
			
			int[] levels = new int[]{0,1,2,3,4,5,6,7,8};
			
			for(int j=0 ; j<levels.length ; j++) {
				
				System.out.println("\t\t-> Start Read Variable ["+uVarName+"," +vVarName+"]");
				
				Variable varU = ncFile.findVariable("U");
				Variable varV = ncFile.findVariable("V");
				
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(0, 0));
				rangeList.add(new Range(levels[j], levels[j]));						
				rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
				rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
				
				Float[][] valuesU = GridCalcUtil.convertStorageToValues(varU.read(rangeList).getStorage(), rows, cols);
				Float[][] valuesV = GridCalcUtil.convertStorageToValues(varV.read(rangeList).getStorage(), rows, cols);
				
				System.out.println("\t\t-> End Read Variable ["+uVarName+"," +vVarName+"]");
				
				String imgFileName = fileName.replace(".nc", "") + "_" + String.format("%03d", j + 1) + ".png";
				
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
				for(int k=0 ; k<polygonDataList.size() ; k++) {
					
					Map<String, Object> polygonData = polygonDataList.get(k);
					
					int x = Integer.valueOf(polygonData.get("x").toString());
					int y = Integer.valueOf(polygonData.get("y").toString());
						
					float u = valuesU[y-boundXY.getBottom()][x-boundXY.getLeft()];
					float v = valuesV[y-boundXY.getBottom()][x-boundXY.getLeft()];
					
					if(u == -999 || v == -999) {
						continue;
					}
						
					double wd = Math.atan2(u, v) * 180 / Math.PI + 180;				
					double ws = Math.sqrt(u*u + v*v) * 1.943844;
					
					Color c = this.getColor(ws);
					
					if(c == null) {						
						continue;
					}
					
					double[][] coordinates = (double[][])polygonData.get("coordinates");
						
					int[] xPoints = new int[coordinates.length];
					int[] yPoints = new int[coordinates.length];
					
					for(int m=0 ; m<coordinates.length ; m++) {			
						
						int _y = (int)Math.floor((boundLonLat.getTop() - coordinates[m][1]) * this.imageExpandFactor * this.imageResizeFactor);
						
						if(_y >= mercatorRatio.length) {
							_y = mercatorRatio.length-1;
						}
						
						xPoints[m] = (int)Math.floor((coordinates[m][0] - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
						yPoints[m] = (int)Math.floor(mercatorRatio[_y]);
					}
					
					ig2.setColor(c);
					ig2.fillPolygon(xPoints, yPoints, coordinates.length);
				}
				
				for(int k=0 ; k<polygonDataList.size() ; k++) {
					
					Map<String, Object> polygonData = polygonDataList.get(k);
					
					int x = Integer.valueOf(polygonData.get("x").toString());
					int y = Integer.valueOf(polygonData.get("y").toString());
					
					if(x % 2 != 0 || y % 2 != 0) {
						continue;
					}
						
					float u = valuesU[y-boundXY.getBottom()][x-boundXY.getLeft()];
					float v = valuesV[y-boundXY.getBottom()][x-boundXY.getLeft()];
					
					if(u == -999 || v == -999) {
						continue;
					}
						
					double wd = Math.atan2(u, v) * 180 / Math.PI + 180;				
					double ws = Math.sqrt(u*u + v*v) * 1.943844;
						
					double[][] coordinates = (double[][])polygonData.get("coordinates");
						
					BufferedImage windTailImage = getWindTailImage(ws, wd);
						
					if(windTailImage != null) {
						
						int _y = (int)Math.floor((boundLonLat.getTop() - coordinates[0][1]) * this.imageExpandFactor * this.imageResizeFactor);
						
						if(_y >= mercatorRatio.length) {
							_y = mercatorRatio.length-1;
						}
						
						ig2.drawImage(windTailImage, 
								(int)Math.ceil((coordinates[0][0] - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor) - wTailWidth/4, 
								(int)Math.floor(mercatorRatio[_y] - wTailHeight/4), 
								null);
					}
				}
				
				ImageIO.write(bi, "PNG", imageFile);
				
				System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}

        return boundLonLat;
    }
	
	private void createWtailImages() throws IOException {
		
		if(wtailImageList != null) {
			System.out.println("\t-> Already Arrow Images Created");
			return;
		} else {
			System.out.println("\t-> Create Arrow Image List");	
		}
		
		String wtailImageDir = String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/wtail/");
		
		String wtailImagePath = wtailImageDir + "/windTail_black_{ws}/windTail_black_{ws}_{wd}.png";
		
		wtailImageList = new BufferedImage[20][];
		
		for(int i=0 ; i<wtailImageList.length ; i++) {
			
			wtailImageList[i] = new BufferedImage[36];
			
			for(int j=0 ; j<wtailImageList[i].length ; j++) {
				
				File originImageFile = new File(wtailImagePath.replaceAll("\\{ws\\}", String.valueOf(i+1))
															  .replaceAll("\\{wd\\}", String.valueOf(j*10)));
				
				wtailImageList[i][j] = Thumbnails.of(originImageFile)
						.imageType(BufferedImage.TYPE_INT_ARGB)
						.size(this.wTailWidth, this.wTailHeight).outputQuality(1).asBufferedImage();
			}
		}
	}
    
    private Color getColor(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		if(v < 0) {
			return new Color(0,46,255);
		} else if(v >= 0 && v < 2) {
			return new Color(0,116,255);
		} else if(v >= 2 && v < 4) {
			return new Color(0,185,255);
		} else if(v >= 4 && v < 6) {
			return new Color(0,255,255);
		} else if(v >= 6 && v < 8) {
			return new Color(2,234,190);
		} else if(v >= 8 && v < 10) {
			return new Color(4,213,123);
		} else if(v >= 10 && v < 12) {
			return new Color(6,194,66);
		} else if(v >= 12 && v < 14) {
			return new Color(23,183,14);
		} else if(v >= 14 && v < 16) {
			return new Color(87,203,10);
		} else if(v >= 16 && v < 18) {
			return new Color(152,223,6);
		} else if(v >= 18 && v < 20) {
			return new Color(218,244,2);
		} else if(v >= 20 && v < 22) {
			return new Color(255,243,0);
		} else if(v >= 22 && v < 24) {
			return new Color(255,220,0);
		} else if(v >= 24 && v < 26) {
			return new Color(255,195,0);
		} else if(v >= 26 && v < 28) {
			return new Color(255,170,0);
		} else if(v >= 28 && v < 30) {
			return new Color(255,130,0);
		} else if(v >= 30 && v < 32) {
			return new Color(255,90,0);
		} else if(v >= 32 && v < 34) {
			return new Color(255,45,0);
		} else if(v >= 34 && v < 36) {
			return new Color(255,0,0);
		} else if(v >= 36 && v < 38) {
			return new Color(209,0,37);
		} else if(v >= 38 && v < 40) {
			return new Color(163,0,74);
		} else {
			return new Color(118,0,111);
		}
	}
    
    private BufferedImage getWindTailImage(double ws, double wd) {
		
		int wdIndex = (int)(wd/10) > 35 ? 0 : (int)(wd/10);
		int wsIndex = (int)(ws/5) > 19 ? 19 : (int)(ws/5);
		
		try {
		
			return wtailImageList[wsIndex][wdIndex];
			
		} catch (Exception e) {
			
			return null;
		}
	}
	
	public static void main(String[] args) {

		new MakeKimDwindImage().process();
	}
}