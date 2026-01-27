package kama.daemon.common.util.model.image;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Polygon;
import java.awt.RenderingHints;
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
import kama.daemon.common.util.model.legendfilter.HobsLegendFilter;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class HobsImageGenerator {
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private DatabaseManager dbManager;
	
	private String stnCd = "RKSI"; // RKSI, RKNY, RKPC 중 하나를 선택할 수 있음
		
	private ModelGridUtil modelGridUtil;
	
	private HobsLegendFilter hobsLegendFilter = null;;
	
	private final int imageExpandFactor = 10000;
	private final int imageResizeFactor = 1;
	
	public HobsImageGenerator(DatabaseManager dbManager, ProcessorInfo processorInfo, String stnCd) {
		
		this.daemonSettings = dbManager.Settings;
		this.processorInfo = processorInfo;
		this.dbManager = dbManager;
		this.stnCd = stnCd;
		
		this.hobsLegendFilter = new HobsLegendFilter();
			
		this.initCoordinates();
	}
	
	private void initCoordinates() {
		
		System.out.println("HobsImageGenerator [ Initailize Coordinate Systems - " + stnCd + " ]");
		
		String coordinatesLatPath = this.daemonSettings.getCoordinatesLatPath().replaceAll("stnCd",  this.stnCd.toLowerCase());
    	String coordinatesLonPath = this.daemonSettings.getCoordinatesLonPath().replaceAll("stnCd",  this.stnCd.toLowerCase());
     	
		switch(this.stnCd) {
		
		case "RKSI":
			this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.HOBS_RKSI, coordinatesLatPath, coordinatesLonPath);
			break;		
		case "RKNY":
			this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.HOBS_RKNY, coordinatesLatPath, coordinatesLonPath);
			break;
		case "RKPC":
			this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.HOBS_RKPC, coordinatesLatPath, coordinatesLonPath);
			break;
		}
    	
    	this.modelGridUtil.setNumberFix(6);
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

		System.out.println("HobsImageGenerator [ Start Create Image ]");
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("HobsImageGenerator [ End Create Image ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();			
			boundXY = this.modelGridUtil.getBoundXY();
			
			System.out.println("\t-> " + boundLonLat);
			System.out.println("\t-> " + boundXY);
			
			List<Map<String, Object>> polygonDataList = this.modelGridUtil.getPolygonDataList();
			
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			// 기온 이미지 생산
			System.out.println("\t-> Process Attribute [Temp]");						
			BufferedImage tempImage = this.generateTempImage(ncFile, polygonDataList, mercatorRatio, imgWidth, imgHeight);
			this.saveImageFile(tempImage, fileName, savePath, "temp");
			
			// 바람 이미지 생산
			System.out.println("\t-> Process Attribute [Wind]");			
			BufferedImage windImage = this.generateWindImage(ncFile, polygonDataList, mercatorRatio, imgWidth, imgHeight);
			this.saveImageFile(windImage, fileName, savePath, "wind");
			
//			// 강수 이미지 생산
//			System.out.println("\t-> Process Attribute [Rain]");
//			BufferedImage rainImage = this.generateRainImage(ncFile, polygonDataList, mercatorRatio, imgWidth, imgHeight);
//			this.saveImageFile(rainImage, fileName, savePath, "rain");
			
			// 습도 이미지 생산
			System.out.println("\t-> Process Attribute [Humidity]");
			BufferedImage humImage = this.generateHumImage(ncFile, polygonDataList, mercatorRatio, imgWidth, imgHeight);
			this.saveImageFile(humImage, fileName, savePath, "humidity");
			
			// 기압 이미지 생산
			System.out.println("\t-> Process Attribute [Pressure]");
			BufferedImage pressureImage = this.generatePressureImage(ncFile, polygonDataList, mercatorRatio, imgWidth, imgHeight);
			this.saveImageFile(pressureImage, fileName, savePath, "pressure");
			
			
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
	
	private void saveImageFile(BufferedImage bi, String fileName, String savePath, String attr) throws IOException {
			
		String imgFileName = fileName.split("\\.")[0] + "_" + attr.toUpperCase() + ".png";
		
		File imageFile = new File(savePath + File.separator + imgFileName);		
		
		System.out.println("\t\t-> Start Write Image");
		ImageIO.write(bi, "PNG", imageFile);
		System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");			
	}
	
	private BufferedImage generateTempImage(NetcdfDataset ncFile, List<Map<String, Object>> polygonDataList, double[] mercatorRatio, int imgWidth, int imgHeight) {
		
		int rows = this.modelGridUtil.getRows();
		int cols = this.modelGridUtil.getCols();

		BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
		BoundXY boundXY = this.modelGridUtil.getBoundXY();
		BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
		
		try {
			
			System.out.println("\t\t-> Start Read Variable [TEMP]");
			
			Variable varTemp = ncFile.findVariable("TEMP");
			
			List<Range> rangeList = new ArrayList<Range>();				
			rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
			rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
			Float[][] valuesTemp = GridCalcUtil.convertStorageToValues(varTemp.read(rangeList).getStorage(), rows, cols);
			
			Graphics2D ig2 = bi.createGraphics();
			
			for(int i=0 ; i<polygonDataList.size() ; i++) {
				
				Map<String, Object> polygonData = polygonDataList.get(i);
				
				int x = Integer.valueOf(polygonData.get("x").toString());
				int y = Integer.valueOf(polygonData.get("y").toString());
				
				float temp = valuesTemp[y-boundXY.getBottom()][x-boundXY.getLeft()];
				
				if(temp == -999) {
					continue;
				}
				
				Color c = this.hobsLegendFilter.getTempColor(temp/10);
				
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
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return bi;
	}
	
	private BufferedImage generateWindImage(NetcdfDataset ncFile, List<Map<String, Object>> polygonDataList, double[] mercatorRatio, int imgWidth, int imgHeight) {
		
		int rows = this.modelGridUtil.getRows();
		int cols = this.modelGridUtil.getCols();

		BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
		BoundXY boundXY = this.modelGridUtil.getBoundXY();
		BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
		
		try {
			
			System.out.println("\t\t-> Start Read Variable [U,V]");
			
			Variable varU = ncFile.findVariable("U");
			Variable varV = ncFile.findVariable("V");
			
			List<Range> rangeList = new ArrayList<Range>();				
			rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
			rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
			
			Float[][] valuesU = GridCalcUtil.convertStorageToValues(varU.read(rangeList).getStorage(), rows, cols);
			Float[][] valuesV = GridCalcUtil.convertStorageToValues(varV.read(rangeList).getStorage(), rows, cols);
			
			Graphics2D ig2 = bi.createGraphics();
			
			for(int i=0 ; i<polygonDataList.size() ; i++) {
				
				Map<String, Object> polygonData = polygonDataList.get(i);
				
				int x = Integer.valueOf(polygonData.get("x").toString());
				int y = Integer.valueOf(polygonData.get("y").toString());
				
				float u = valuesU[y-boundXY.getBottom()][x-boundXY.getLeft()] / 10;
				float v = valuesV[y-boundXY.getBottom()][x-boundXY.getLeft()] / 10;
				
				if(u == -999 || v == -999) {
					continue;
				}
				
				double ws = Math.sqrt(u * u + v * v); // 바람 속도				
				
				Color c = this.hobsLegendFilter.getWsColor(ws);
				
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
					yPoints[m] = (int)Math.floor((boundLonLat.getTop() - coordinates[m][1]) * this.imageExpandFactor * this.imageResizeFactor);
				}
				
				ig2.setColor(c);
				
				ig2.fillPolygon(xPoints, yPoints, coordinates.length);
			}
			
			int skipCount = 14;
			
			for(int i=0 ; i<polygonDataList.size() ; i++) {
				
				Map<String, Object> polygonData = polygonDataList.get(i);
				
				int x = Integer.valueOf(polygonData.get("x").toString());
				int y = Integer.valueOf(polygonData.get("y").toString());
				
				if(y % skipCount == 0) {
					
					if (y % (skipCount*2) == 0) {
					
						if(x % skipCount != 0) {
							continue;
						}
						
					} else {
						
						if((x-(skipCount/2))%skipCount != 0) {
							continue;
						}
					}
				} else {
					continue;
				}
				
				float u = valuesU[y-boundXY.getBottom()][x-boundXY.getLeft()] / 10;
				float v = valuesV[y-boundXY.getBottom()][x-boundXY.getLeft()] / 10;
				
				if(u == -999 || v == -999) {
					continue;
				}
				
				double ws = Math.sqrt(u * u + v * v); // 바람 속도
				double wd = Math.atan2(u, v) * 180 / Math.PI; // 바람 방향 (degree)
				
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
				
				// 화살표의 기본 길이는 기본 격자의 한변의 길이로 설정한다
				// 기변 격자는 xPoints, yPoints 로 구한다
						
				double arrowLength = Math.sqrt(Math.pow(xPoints[2] - xPoints[0], 2) + Math.pow(yPoints[2] - yPoints[0], 2));
								
				this.drawArrow(ig2, xPoints[0], yPoints[0], Math.sqrt(ws*arrowLength)*10, wd-90, 4, 12);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return bi;
	}
	
	private BufferedImage generateRainImage(NetcdfDataset ncFile, List<Map<String, Object>> polygonDataList, double[] mercatorRatio, int imgWidth, int imgHeight) {
		
		int rows = this.modelGridUtil.getRows();
		int cols = this.modelGridUtil.getCols();

		BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
		BoundXY boundXY = this.modelGridUtil.getBoundXY();
		BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
		
		try {
			
			System.out.println("\t\t-> Start Read Variable [Rain_rate]");
			
			Variable varRain = ncFile.findVariable("Rain_rate");
			
			List<Range> rangeList = new ArrayList<Range>();				
			rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
			rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
			
			Float[][] valuesRain = GridCalcUtil.convertStorageToValues(varRain.read(rangeList).getStorage(), rows, cols);
			
			Graphics2D ig2 = bi.createGraphics();
			
			for(int i=0 ; i<polygonDataList.size() ; i++) {
				
				Map<String, Object> polygonData = polygonDataList.get(i);
				
				int x = Integer.valueOf(polygonData.get("x").toString());
				int y = Integer.valueOf(polygonData.get("y").toString());
				
				float rain = valuesRain[y-boundXY.getBottom()][x-boundXY.getLeft()];
				
				if(rain == -999) {
					continue;
				}
				
				Color c = this.hobsLegendFilter.getRainColor(rain/10);
				
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
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return bi;
	}
	
	private BufferedImage generateHumImage(NetcdfDataset ncFile, List<Map<String, Object>> polygonDataList, double[] mercatorRatio, int imgWidth, int imgHeight) {
		
		int rows = this.modelGridUtil.getRows();
		int cols = this.modelGridUtil.getCols();

		BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
		BoundXY boundXY = this.modelGridUtil.getBoundXY();
		BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
		
		try {
			
			System.out.println("\t\t-> Start Read Variable [RH]");
			
			Variable varHum = ncFile.findVariable("RH");
			
			List<Range> rangeList = new ArrayList<Range>();				
			rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
			rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
			
			Float[][] valuesHum = GridCalcUtil.convertStorageToValues(varHum.read(rangeList).getStorage(), rows, cols);
			
			Graphics2D ig2 = bi.createGraphics();
			
			for(int i=0 ; i<polygonDataList.size() ; i++) {
				
				Map<String, Object> polygonData = polygonDataList.get(i);
				
				int x = Integer.valueOf(polygonData.get("x").toString());
				int y = Integer.valueOf(polygonData.get("y").toString());
				
				float hum = valuesHum[y-boundXY.getBottom()][x-boundXY.getLeft()];
				
				if(hum == -999) {
					continue;
				}
				
				Color c = this.hobsLegendFilter.getHumColor(hum/10);
				
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
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return bi;
	}
	
	private BufferedImage generatePressureImage(NetcdfDataset ncFile, List<Map<String, Object>> polygonDataList, double[] mercatorRatio, int imgWidth, int imgHeight) {
		
		int rows = this.modelGridUtil.getRows();
		int cols = this.modelGridUtil.getCols();

		BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
		BoundXY boundXY = this.modelGridUtil.getBoundXY();
		BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
		
		try {
			
			System.out.println("\t\t-> Start Read Variable [SLP]");
			
			Variable varPressure = ncFile.findVariable("SLP");
			
			List<Range> rangeList = new ArrayList<Range>();				
			rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
			rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
			
			Float[][] valuesPressure = GridCalcUtil.convertStorageToValues(varPressure.read(rangeList).getStorage(), rows, cols);
			
			Graphics2D ig2 = bi.createGraphics();
			
			for(int i=0 ; i<polygonDataList.size() ; i++) {
				
				Map<String, Object> polygonData = polygonDataList.get(i);
				
				int x = Integer.valueOf(polygonData.get("x").toString());
				int y = Integer.valueOf(polygonData.get("y").toString());
				
				float pressure = valuesPressure[y-boundXY.getBottom()][x-boundXY.getLeft()];
				
				if(pressure == -999) {
					continue;
				}
				
				Color c = this.hobsLegendFilter.getPressureColor(pressure/10);
				
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
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return bi;
	}
    
	 /**
     * 화살표를 그리는 메서드 (몸통과 헤드를 모두 포함)
     * @param g2d Graphics2D 객체
     * @param startX 시작점 X 좌표
     * @param startY 시작점 Y 좌표
     * @param length 화살표 길이
     * @param angle 화살표 각도 (라디안)
     * @param thickness 화살표 두께
     * @param headSize 헤드 크기
     * @param color 화살표 색상
     */
    public void drawArrow(Graphics2D g2d, double startX, double startY, 
                               double length, double angle, double thickness, 
                               double headSize) {
    	
    	double arrowPosRatio = (length-headSize)/length; // 화살표 몸통과 헤드의 비율
    	
    	// head 사이즈와 length 조건에 따라 최소 크기와 최대 크기를 설정한다
    	
    	
    	angle = angle * Math.PI / 180; // 각도를 라디안으로 변환
    	
        // 안티앨리어싱 설정
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        
        // 색상 설정
        g2d.setColor(new Color(0, 0, 0)); // 검정색;
        
        // 끝점 계산
        double endX = startX + length * Math.cos(angle);
        double endY = startY + length * Math.sin(angle);
        
        // 화살표 몸통 그리기
        // 화살표 방향에 수직인 벡터 계산
        double dx = endX - startX;
        double dy = endY - startY;
        double arrowLength = Math.sqrt(dx * dx + dy * dy);
        
        if (arrowLength > 0) {
            // 단위 벡터 계산
            double unitX = dx / arrowLength;
            double unitY = dy / arrowLength;
            
            // 수직 벡터 계산
            double perpX = -unitY;
            double perpY = unitX;
            
            // 두께의 절반
            double halfThickness = thickness / 2.0;
            
            // 사각형의 네 꼭지점 계산
            double[] xPoints = {
                startX + perpX * halfThickness,
                startX - perpX * halfThickness,
                endX - perpX * halfThickness,
                endX + perpX * halfThickness
            };
            
            double[] yPoints = {
                startY + perpY * halfThickness,
                startY - perpY * halfThickness,
                endY - perpY * halfThickness,
                endY + perpY * halfThickness
            };
            
            // 사각형 그리기 (몸통)
            g2d.fill(new Polygon(
                new int[]{(int)xPoints[0], (int)xPoints[1], (int)xPoints[2], (int)xPoints[3]},
                new int[]{(int)yPoints[0], (int)yPoints[1], (int)yPoints[2], (int)yPoints[3]},
                4
            ));
        }
        
        // 화살표 기준점 계산
        double arrowX = startX + length * arrowPosRatio * Math.cos(angle);
        double arrowY = startY + length * arrowPosRatio * Math.sin(angle);
        
        // 화살표 헤드 그리기 (삼각형 모양으로 변경)
        // 헤드의 각도 (라디안)
        double headAngle = Math.PI / 2; // 30도
        
        // 헤드의 두 점 계산
        double headAngle1 = angle + headAngle;
        double headAngle2 = angle - headAngle;
        
        // 헤드의 두 점 좌표
        double headX1 = arrowX - headSize * Math.cos(headAngle1);
        double headY1 = arrowY - headSize * Math.sin(headAngle1);
        double headX2 = arrowX - headSize * Math.cos(headAngle2);
        double headY2 = arrowY - headSize * Math.sin(headAngle2);
        
        double headEndX = arrowX + headSize * 2 * Math.cos(angle);
        double headEndY = arrowY + headSize * 2 * Math.sin(angle);    
        
        double headEndX2 = arrowX + headSize / 2 * Math.cos(angle);
        double headEndY2 = arrowY + headSize / 2 * Math.sin(angle);
        
        // 헤드좌표 전체를 화살표 몸통 각도의 반대로 옴겨야함
        // 헤드 끝점 좌표
        
        // 헤드 그리
        int[] xPoints = {(int)headEndX, (int)headX1, (int)headEndX2, (int)headX2};
        int[] yPoints = {(int)headEndY, (int)headY1, (int)headEndY2, (int)headY2};
        
        g2d.fill(new Polygon(xPoints, yPoints, 4));
    }
}
