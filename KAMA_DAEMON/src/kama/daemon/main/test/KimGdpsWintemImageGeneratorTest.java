package kama.daemon.main.test;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.legendfilter.KimGdpsLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KimGdpsWintemImageGeneratorTest {	
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 50;
	private final int imageResizeFactor = 2;
	
	private static BufferedImage[][] wtailImageList;
	
	private Map<String, String> paramMap = null;
	
	private float wTailRatio = 1f;
	private int wTailWidth = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int wTailHeight = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int fontSize = (int)(this.imageExpandFactor * this.imageResizeFactor / 4);
	
	private final String fontName = "Times New Roman";
	
	public KimGdpsWintemImageGeneratorTest() {
		
			
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("KimGdpsImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = "F:/data/datastore/grid/kim_gdps_lat.bin";
		String coordinatesLonPath = "F:/data/datastore/grid/kim_gdps_lon.bin";
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_GDPS, ModelGridUtil.Position.MIDDLE_CENTER, coordinatesLatPath, coordinatesLonPath);
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String savePath, Map<String, String> paramMap) {

		System.out.println("KimGdpsImageGenerator [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KimGdps Image Grid List");
		
		this.paramMap = paramMap;
		
		double[] mapBound = new double[]{50, 20, 110, 150};
		
		this.modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(mapBound);
		
		System.out.println(this.modelGridUtil.getBoundXY());
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("KimGdpsImageGenerator [ End Create Images ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		double cropTop = Double.parseDouble(this.paramMap.get("cropTop"));
		double cropBottom = Double.parseDouble(this.paramMap.get("cropBottom"));
		double cropLeft = Double.parseDouble(this.paramMap.get("cropLeft"));
		double cropRight = Double.parseDouble(this.paramMap.get("cropRight"));
		int resizeImgWidth = Integer.parseInt(this.paramMap.get("imgWidth"));
		int resizeImgHeight = Integer.parseInt(this.paramMap.get("imgHeight"));
		int domainType = Integer.parseInt(this.paramMap.get("domainType"));
		String wintemBaseImgFileName = this.paramMap.get("wintemBaseImg");
		
		try {
			
			this.createWtailImages();
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();
			boundXY = this.modelGridUtil.getBoundXY();
			
			System.out.println(boundLonLat);
				
			KimGdpsLegendFilter kimGdpsLegendFilter = new KimGdpsLegendFilter();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] latInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLatInterval());
			double[] lonInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLonInterval());
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			System.out.println("\t-> Process Attribute [wintem]");
			
			for(int j=0 ; j<1 ; j++) {
					
				System.out.println("\t\t-> Start Read Variable [u-component_of_wind_isobaric, v-component_of_wind_isobaric, Temperature_isobaric]");
				
				Variable varUWind = ncFile.findVariable("u-component_of_wind_isobaric");
				Variable varVWind = ncFile.findVariable("v-component_of_wind_isobaric");
				Variable varTemp = ncFile.findVariable("Temperature_isobaric");
					
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(0, 0));		
				rangeList.add(new Range(23-j, 23-j));						
				rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
				rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
				Float[][] valuesUWind = GridCalcUtil.convertStorageToValuesReverse(varVWind.read(rangeList).getStorage(), rows, cols);
				Float[][] valuesVWind = GridCalcUtil.convertStorageToValuesReverse(varUWind.read(rangeList).getStorage(), rows, cols);
				Float[][] valuesTemp = GridCalcUtil.convertStorageToValuesReverse(varTemp.read(rangeList).getStorage(), rows, cols);
				
				System.out.println("\t\t-> End Read Variable [u-component_of_wind_isobaric, v-component_of_wind_isobaric, Temperature_isobaric]");
				
				String imgFileName = fileName.replace(".gb2", "") + "_wintem_" + domainType + "_" + String.format("%03d", j+1) + ".png";
					
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				ig2.setFont(new Font(this.fontName, Font.BOLD, this.fontSize));
				ig2.setColor(new Color(0, 0, 255));	
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
				for(int k=0 ; k<rows ; k++) {
					
					for(int l=0 ; l<cols ; l++) {
						
						if(k % 7 != 0 || l % 7 != 0) {
							continue;
						}
						
						float u = valuesUWind[k][l];
						float v = valuesVWind[k][l];
						
						if(u == -999 || v == -999) {
							continue;
						}
							
						double wd = Math.atan2(u, v) * 180 / Math.PI + 180;		
						double ws = Math.sqrt(u*u + v*v);
						int temp = (int)(valuesTemp[k][l] - 273.15);		
							
						int tailIndex = (int)Math.floor(ws * 1.943844 / 5);
						
						if(tailIndex > 19) {
							tailIndex = 19;
						}
						
						double xCoord = boundLonLat.getLeft() + lonInterval[l];							
						double yCoord = boundLonLat.getTop() - latInterval[k];
						
						int xPoint = (int)Math.floor((xCoord - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
						int yPoint = (int)Math.floor(mercatorRatio[(int)Math.floor((yCoord - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor)]);
						
//						ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], xPoint, yPoint, null);
						ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], xPoint - this.wTailWidth / 2, yPoint - this.wTailHeight / 2, null);
						
						if((int)((wd + 360) % 360) > 90 && (int)((wd + 360) % 360) < 270) {
							ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), temp < 0 ? xPoint - 15 : xPoint - 30, yPoint - 5);	
						} else {
							ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), temp < 0 ? xPoint - 15 : xPoint - 30, yPoint - 5 + this.fontSize);	
						}
					}
				}
				
				bi = this.cropImage(bi, boundLonLat, mercatorRatio, cropTop, cropBottom, cropLeft, cropRight);
				
				BufferedImage wintemBaseImg = ImageIO.read(new File("C:/DEV/KAMA_AAMI/workspace/KAMA_DAEMON/res/"+wintemBaseImgFileName));
				
				wintemBaseImg.getGraphics().drawImage(bi, 0, 0, wintemBaseImg.getWidth(), wintemBaseImg.getHeight(), null);
				
				wintemBaseImg = Thumbnails.of(wintemBaseImg).imageType(BufferedImage.TYPE_INT_ARGB).forceSize(resizeImgWidth, resizeImgHeight).asBufferedImage();
				
				ImageIO.write(wintemBaseImg, "PNG", imageFile);

			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
	
	private void appendWintemLabel(BufferedImage wintemBaseImg, Date issuedDt, Date fcstDt, String feet, int fcstHour) {

		final int labelWidth = 350;
		final int labelHeight = 160;
		final int lineHeight = 20;
		final int labelTopMargin = 10;		
		final int fontLeftMargin = 10;
		
		SimpleDateFormat labelDateFormat = new SimpleDateFormat("HH \'UTC\' dd MM yyyy"); 
				
		Graphics2D ig2 = wintemBaseImg.createGraphics();
		ig2.setColor(new Color(255, 255, 255));
		ig2.fillRect(wintemBaseImg.getWidth() - labelWidth, wintemBaseImg.getHeight() - labelHeight, labelWidth, labelHeight);
		
		ig2.setStroke(new BasicStroke(3));
		ig2.setColor(new Color(0, 0, 0));
		ig2.drawRect(wintemBaseImg.getWidth() - labelWidth, wintemBaseImg.getHeight() - labelHeight, labelWidth, labelHeight);
		
		ig2.setFont(new Font(fontName, Font.PLAIN, 20));
		ig2.setColor(new Color(0, 0, 0));	
							
		ig2.drawString("ISSUED BY AMO", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*1);
		ig2.drawString("WIND/TEMPERATURE FL " + feet, wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*2);
		ig2.drawString("FIXED TIME PROGNOSTIC CHART", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*3);
		
		ig2.drawString("BASED ON " + labelDateFormat.format(issuedDt), wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*5);
		
		ig2.drawString("VALID " + labelDateFormat.format(fcstDt), wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*4);
		
		ig2.setFont(new Font(fontName, Font.PLAIN, 15));
		ig2.drawString("Units: Wind(knots), Temperature(℃)", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*6);		
		ig2.drawString("Temperatures are negative unless prefixed by PS", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin/2 + lineHeight*7);
		
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
		
		String wtailImageDir = "C:/DEV/KAMA_AAMI/workspace/KAMA_DAEMON/res/wtail";
		
		String wtailImagePath = wtailImageDir + "/windTail_blue_{ws}/windTail_blue_{ws}_{wd}.png";
		
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
	
	public static void main(String[] args) {
        
		KimGdpsWintemImageGeneratorTest kimGdpsImageGeneratorTest = new KimGdpsWintemImageGeneratorTest();
                  
		try {
						
			NetcdfDataset ncFile = NetcdfDataset.acquireDataset("F:/KAMA_AAMI/2025/항기청_수신/항기청_수신_20250602/kim_g128_ne36_pres_h000.2025060200.gb2", null);
			String fileName = "kim_g128_ne36_pres_h000.2025060200.gb2";
	        String savePath = "F:/data/kim_gdps_img";
	        
	        // 평시상태 이미지 생성
	        Map<String, String> paramMap1 = new HashMap<String, String>();
	        paramMap1.put("cropTop", "39");
	        paramMap1.put("cropBottom", "27.5");
	        paramMap1.put("cropLeft", "121");
	        paramMap1.put("cropRight", "135");
	        paramMap1.put("imgWidth", "720");
	        paramMap1.put("imgHeight", "720");
	        paramMap1.put("domainType", "1");
	        paramMap1.put("wintemBaseImg", "wintem_base2_upscaled.png");
	        
	        kimGdpsImageGeneratorTest.generateImages(ncFile, fileName, savePath, paramMap1);
	        
	        // 위기상태 이미지 생성
	        Map<String, String> paramMap2 = new HashMap<String, String>();
	        paramMap2.put("cropTop", "44");
	        paramMap2.put("cropBottom", "27.5");
	        paramMap2.put("cropLeft", "119");
	        paramMap2.put("cropRight", "135");
	        paramMap2.put("imgWidth", "720");
	        paramMap2.put("imgHeight", "900");	  
	        paramMap2.put("domainType", "2");
	        paramMap2.put("wintemBaseImg", "wintem_base3_upscaled.png");
	        
	        kimGdpsImageGeneratorTest.generateImages(ncFile, fileName, savePath, paramMap2);
			
		} catch (Exception e) {
			
		}
		
		
	}
	
}
