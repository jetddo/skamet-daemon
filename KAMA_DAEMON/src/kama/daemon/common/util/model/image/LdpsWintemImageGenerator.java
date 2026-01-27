package kama.daemon.common.util.model.image;

import java.awt.Color;
import java.awt.Font;
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

public class LdpsWintemImageGenerator {
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private DatabaseManager dbManager;
	
	private ModelGridUtil modelGridUtil;
	
	private static BufferedImage[][] wtailImageList;
	
	private final int imageExpandFactor = 100;
	private final int imageResizeFactor = 1;
	
	double cropTop = 43.1;
	double cropBottom = 28.9;
	double cropLeft = 118.1;
	double cropRight = 135;
	
	private float wTailRatio = 1.2f;
	private int wTailWidth = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int wTailHeight = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int fontSize = (int)(this.imageExpandFactor * this.imageResizeFactor / 4);
	
	public LdpsWintemImageGenerator(DatabaseManager dbManager, ProcessorInfo processorInfo) {
		
		this.daemonSettings = dbManager.Settings;
		this.processorInfo = processorInfo;
		this.dbManager = dbManager;
			
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("LdpsWintemImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.daemonSettings.getCoordinatesLatPath();
		String coordinatesLonPath = this.daemonSettings.getCoordinatesLonPath();
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS, null, coordinatesLatPath, coordinatesLonPath);
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

		System.out.println("LdpsWintemImageGenerator");
		
		System.out.println("\t-> Create Ldps Image Grid List");
		
//		double[] mapBound = new double[]{40, 30, 0, 360};
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("LdpsWintemImageGenerator [ End Create Image ]");
	}
	
	private boolean isDisplayGrid(int[] displayGridList, int pos) {
		
		for(int i=0 ; i<displayGridList.length ; i++) {
		
			if(displayGridList[i] == pos) {
				return true;
			}
		}
		
		return false;
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
			
			System.out.println("\t-> Process Attribute [Wintem]");
			
			int timeIndex = Integer.valueOf(fileName.replaceAll("qwumloa_pb", "").replaceAll(".nc", ""));
			
			int timeLength = fileName.contains("pb000") ? 1 : 2;
			
			for(int i=0 ; i<timeLength ; i++) {
				
				for(int j=0 ; j<1 ; j++) {
					
					System.out.println("\t\t-> Start Read Variable [x-wind, y-wind]");
					
					Variable varXWind = ncFile.findVariable("x-wind");
					Variable varYWind = ncFile.findVariable("y-wind");
					Variable varTemp = ncFile.findVariable("temp");
					
					List<Range> rangeList = new ArrayList<Range>();
					rangeList.add(new Range(i, i));	
					rangeList.add(new Range(j, j));						
					rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
					rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
					
					Float[][] valuesXWind = GridCalcUtil.convertStorageToValues(varXWind.read(rangeList).getStorage(), rows, cols);
					Float[][] valuesYWind = GridCalcUtil.convertStorageToValues(varYWind.read(rangeList).getStorage(), rows, cols);
					Float[][] valuesTemp = GridCalcUtil.convertStorageToValues(varTemp.read(rangeList).getStorage(), rows, cols);
					
					System.out.println("\t\t-> End Read Variable [x-wind, y-wind]");
					
					String imgFileName = fileName.replaceAll("pb[0-9]{3}.nc", "") + "pb" + String.format("%03d", timeIndex - (timeLength-1) + i) + "_wintem_" + String.format("%02d", j) +".png";
					
					File imageFile = new File(savePath + File.separator + imgFileName);
					
					BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
					
					Graphics2D ig2 = bi.createGraphics();
					
					ig2.setFont(new Font("consolas", Font.BOLD, this.fontSize));
					ig2.setColor(new Color(0, 0, 0));
					
					System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
								
					int[] displayGridXList = new int[]{
						160, 180, 200, 220, 240, 260, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 930, 950, 970, 990
					};
					
					int[] displayGridYList = new int[]{
						95, 120, 145, 190, 260, 330, 400, 470, 540, 610, 675, 740, 805, 870, 930, 990
					};
					
					for(int k=0 ; k<polygonDataList.size() ; k++) {
						
						Map<String, Object> polygonData = polygonDataList.get(k);
						
						int x = Integer.valueOf(polygonData.get("x").toString());
						int y = Integer.valueOf(polygonData.get("y").toString());
						
						if(!this.isDisplayGrid(displayGridXList, x)) {
							continue;
						}
						
						if(!this.isDisplayGrid(displayGridYList, y)) {
							continue;
						}
						
						float u = valuesXWind[y-boundXY.getBottom()][x-boundXY.getLeft()];
						float v = valuesYWind[y-boundXY.getBottom()][x-boundXY.getLeft()];
						
						if(u == -999 || v == -999) {
							continue;
						}
							
						double wd = Math.atan2(u, v) * 180 / Math.PI;		
						double ws = Math.sqrt(u*u + v*v);
						int temp = (int)(valuesTemp[y-boundXY.getBottom()][x-boundXY.getLeft()] - 273.15);						
						
						int tailIndex = (int)Math.floor(ws * 1.943844);
						
						if(tailIndex > 19) {
							tailIndex = 19;
						}
						
						double[][] coordinates = (double[][])polygonData.get("coordinates");
						
						double[] firstCoordinate = coordinates[0];
							
						int pixelX = (int)Math.floor((firstCoordinate[0] - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
						int pixelY = (int)Math.floor(mercatorRatio[(int)Math.floor((boundLonLat.getTop() - firstCoordinate[1]) * this.imageExpandFactor * this.imageResizeFactor)]);
						
//						ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], pixelX, pixelY, null);
						ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], pixelX - this.wTailWidth / 2, pixelY - this.wTailHeight / 2, null);
						
						if((int)((wd + 360) % 360) > 90 && (int)((wd + 360) % 360) < 270) {
							ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), pixelX - 30, pixelY - 5);	
						} else {
							ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), pixelX - 30, pixelY - 5 + this.fontSize);	
						}
					}
					
					bi = this.cropImage(bi, boundLonLat, mercatorRatio, cropTop, cropBottom, cropLeft, cropRight);
						
					BufferedImage wintemBaseImg = ImageIO.read(new File(String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/wintem_base.png")));
					
					wintemBaseImg.getGraphics().drawImage(bi, 0, 0, wintemBaseImg.getWidth(), wintemBaseImg.getHeight(), null);
					
					ImageIO.write(wintemBaseImg, "PNG", imageFile);
					System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
				}
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
	
	private BufferedImage cropImage(BufferedImage bi, BoundLonLat boundLonLat, double[] mercatorRatio, double top, double bottom, double left, double right) {
				
		int y1 = (int)Math.floor(mercatorRatio[(int)Math.floor((boundLonLat.getTop() - top) * this.imageExpandFactor * this.imageResizeFactor)]);
		int y2 = (int)Math.floor(mercatorRatio[(int)Math.floor((boundLonLat.getTop() - bottom) * this.imageExpandFactor * this.imageResizeFactor)]);
		int x1 = (int)Math.floor((left - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		int x2 = (int)Math.floor((right - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
		return bi.getSubimage(x1, y1, x2 - x1, y2 - y1);
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
						.size(this.wTailWidth, this.wTailHeight).asBufferedImage();
			}
		}
	}
}
