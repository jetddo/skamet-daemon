package kama.daemon.common.util.model.image;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class DwindImageGenerator {
		
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private DatabaseManager dbManager;
	
	private ModelGridUtil modelGridUtil;
	
	private static BufferedImage[][] wtailImageList;
	
	private final int imageExpandFactor = 10000;
	private final int imageResizeFactor = 1;
	
	private String staCode = null;
	
	private float wTailRatio = 0.008f;
	private int wTailWidth = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int wTailHeight = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
		
	public DwindImageGenerator(DatabaseManager dbManager, ProcessorInfo processorInfo, String staCode) {
		
		this.daemonSettings = dbManager.Settings;
		this.processorInfo = processorInfo;
		this.dbManager = dbManager;
		this.staCode = staCode;
			
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("DwindImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.daemonSettings.getCoordinatesLatPath().replaceAll("staCode", staCode.toLowerCase());
    	String coordinatesLonPath = this.daemonSettings.getCoordinatesLonPath().replaceAll("staCode", staCode.toLowerCase());
    	
    	switch(staCode.toLowerCase()) {
    	case "jeju": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_JEJU, coordinatesLatPath, coordinatesLonPath);
    		
    		break;    	
    	case "gimpo": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_GIMPO, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 
    	case "incheon": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_INCHEON, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 
    	case "muan": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_MUAN, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 
    	case "ulsan": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_ULSAN, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 
    	
    	case "chungju": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_CHUNGJU, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 		
    		
    	case "daegu": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_DAEGU, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 	
    		
    	case "gyangju": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_GYANGJU, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 	
    		
    	case "kimhae": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_KIMHAE, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 	
    		
    	case "pohang": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_POHANG, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 	
    		
    	case "sacheon": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_SACHEON, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 	
    		
    	case "yangyang": 
    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_YANGYANG, coordinatesLatPath, coordinatesLonPath);
    		
    		break;
    		
    	case "yeosu": 
    		    		
    		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KMAPP_YEOSU, coordinatesLatPath, coordinatesLonPath);
    		
    		break; 	
    	}
    	
    	this.modelGridUtil.setNumberFix(6);
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

		System.out.println("DwindImageGenerator [ Start Create Image ]");
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("DwindImageGenerator [ End Create Image ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			this.createWtailImages();
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();			
			boundXY = this.modelGridUtil.getBoundXY();
			
			List<Map<String, Object>> polygonDataList = this.modelGridUtil.getPolygonDataList();
			
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			System.out.println(mercatorRatio.length);
			
			System.out.println("\t-> Process Attribute [u, v]");
			
			int[] levels = new int[]{0, 7, 11, 16, 20, 23, 26, 28};
				
			for(int j=0 ; j<levels.length ; j++) {
				
				System.out.println("\t\t-> Start Read Variable [U_Comp, V_Comp]");
				
				Variable varU = ncFile.findVariable("U_Comp");
				Variable varV = ncFile.findVariable("V_Comp");
				
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(levels[j], levels[j]));						
				rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
				rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
				
				Float[][] valuesU = GridCalcUtil.convertStorageToValues(varU.read(rangeList).getStorage(), rows, cols);
				Float[][] valuesV = GridCalcUtil.convertStorageToValues(varV.read(rangeList).getStorage(), rows, cols);
				
				System.out.println("\t\t-> End Read Variable [U_Comp, V_Comp]");
				
				String imgFileName = fileName + "_" + j + ".png";
				
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
					
					if(x % 5 != 0 || y % 5 != 0) {
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
								(int)Math.floor(mercatorRatio[_y]), 
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
}
