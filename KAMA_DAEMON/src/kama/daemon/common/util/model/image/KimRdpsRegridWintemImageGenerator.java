package kama.daemon.common.util.model.image;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.FloatBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

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

public class KimRdpsRegridWintemImageGenerator {
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private DatabaseManager dbManager;
	
	private XMLOutputFactory factory = XMLOutputFactory.newFactory();
	
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
	
	private final String wintemImgFilePattern = "WINTEM_KIM_RDPS_{weatherType}_FL{heightText}_{fcstHour}H_{issuedDt}.jpg";
	
	private FloatBuffer latitudeBuffer;
	private FloatBuffer longitudeBuffer;
	
	private List<Map<String, Object>> fileInfoList = new ArrayList<Map<String, Object>>();
	
	private Map<String, String> regridInfo = null;
	
	public KimRdpsRegridWintemImageGenerator(DatabaseManager dbManager, ProcessorInfo processorInfo) {
		
		this.daemonSettings = dbManager.Settings;
		this.processorInfo = processorInfo;
		this.dbManager = dbManager;
			
		this.initCoordinates();
		
		this.readRegridInfo();
	}

	private void initCoordinates() {
		
		System.out.println("KimRdpsImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.daemonSettings.getCoordinatesLatPath();
		String coordinatesLonPath = this.daemonSettings.getCoordinatesLonPath();
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_RDPS, null, coordinatesLatPath, coordinatesLonPath);
				
		this.latitudeBuffer = modelGridUtil.getLatBuffer();
		this.longitudeBuffer = modelGridUtil.getLonBuffer();
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
	
	public List<Map<String, Object>> generateImages(NetcdfDataset ncFile, String fileName, String savePath, Map<String, String> paramMap) {

		System.out.println("KimRdpsImageGenerator [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KimRdps Image Grid List");
		
		this.paramMap = paramMap;
		
		double[] mapBound = new double[]{50, 20, 110, 150};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		System.out.println(this.modelGridUtil.getBoundXY());
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("KimRdpsImageGenerator [ End Create Images ]");
		
		return this.fileInfoList;
	}
	
	private Map<String, Object> getRegridData(Float[][] values, BoundLonLat maxBoundLonLat, int rows, int cols) {
		
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
		
		return regridData;
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmm");
		
		this.fileInfoList.clear();
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		double cropTop = Double.parseDouble(this.paramMap.get("cropTop"));
		double cropBottom = Double.parseDouble(this.paramMap.get("cropBottom"));
		double cropLeft = Double.parseDouble(this.paramMap.get("cropLeft"));
		double cropRight = Double.parseDouble(this.paramMap.get("cropRight"));
		int resizeImgWidth = Integer.parseInt(this.paramMap.get("imgWidth"));
		int resizeImgHeight = Integer.parseInt(this.paramMap.get("imgHeight"));
		String weatherType = this.paramMap.get("weatherType");
		String wintemBaseImgFileName = this.paramMap.get("wintemBaseImg");
		
		try {
			
			this.createWtailImages();
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();
			boundXY = this.modelGridUtil.getBoundXY();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
				
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
				
			double latTerm = 0d;
			double lonTerm = 0d;
			
			String[] savePaths = savePath.split("\\/");
			
			Calendar cal = new GregorianCalendar();
			cal.set(Calendar.YEAR, Integer.valueOf(savePaths[savePaths.length-4]));
			cal.set(Calendar.MONTH, Integer.valueOf(savePaths[savePaths.length-3])-1);
			cal.set(Calendar.DAY_OF_MONTH, Integer.valueOf(savePaths[savePaths.length-2]));
			cal.set(Calendar.HOUR_OF_DAY, Integer.valueOf(savePaths[savePaths.length-1]));
			
			Date issuedDt = sdf.parse(fileName.split("\\.")[1]);
			
			int fcstHour = Integer.valueOf(fileName.split("\\.")[0].split("_")[4].replaceAll("h", ""));
			
			System.out.println("\t-> Model Fcst Hour: +" + fcstHour + "H");
			
			if(fcstHour > 36) {
				return boundLonLat;
			}
			
			cal.add(Calendar.HOUR_OF_DAY, fcstHour);
			
			Date fcstDt = cal.getTime();
			
			System.out.println("\t-> Process Create Image");
			
			List<Map<String, Object>> wintemUseDataInfoList =  this.getWintemUseDataInfoList();
			
			for(int i=0 ; i<wintemUseDataInfoList.size() ; i++) {
				
				Map<String, Object> wintemUseDataInfo = wintemUseDataInfoList.get(i);
				
				String heightIndexes = wintemUseDataInfo.get("heightIndexes").toString();
				String heightText = wintemUseDataInfo.get("heightText").toString();
				
				Map<String, Float[][]> wintemDataSet = this.getWintemDataSet(ncFile, rows, cols, boundXY, heightIndexes);
				
				Float[][] valuesUWind = wintemDataSet.get("valuesUWind");
				Float[][] valuesVWind = wintemDataSet.get("valuesVWind");
				Float[][] valuesTemp = wintemDataSet.get("valuesTemp");
				
				System.out.print("\t\t\t-> Start Regriding valuesUWind");
			
				Map<String, Object> regridDataXWind = getRegridData(valuesUWind, boundLonLat, rows, cols);
				
				System.out.println(" End");
				
				System.out.print("\t\t\t-> Start Regriding valuesVWind");
				
				Map<String, Object> regridDataYWind = getRegridData(valuesVWind, boundLonLat, rows, cols);
				
				System.out.println(" End");
				
				System.out.print("\t\t\t-> Start Regriding valuesTemp");
				
				Map<String, Object> regridDataTemp = getRegridData(valuesTemp, boundLonLat, rows, cols);
				
				System.out.println(" End");
				
				latTerm = (double)regridDataTemp.get("latTerm");
				lonTerm = (double)regridDataTemp.get("lonTerm");
				
				Float[][] regridValuesXWind = (Float[][])regridDataXWind.get("regridValues");
				Float[][] regridValuesYWind = (Float[][])regridDataYWind.get("regridValues");
				Float[][] regridValuesTemp = (Float[][])regridDataTemp.get("regridValues");
					
				String imgFileName = this.wintemImgFilePattern.toString();
				imgFileName = imgFileName.replaceAll("\\{weatherType\\}", weatherType);
				imgFileName = imgFileName.replaceAll("\\{heightText\\}", heightText);
				imgFileName = imgFileName.replaceAll("\\{fcstHour\\}", String.format("%02d", fcstHour));
				imgFileName = imgFileName.replaceAll("\\{issuedDt\\}", new SimpleDateFormat("yyyyMMddHHmm").format(issuedDt));
					
				Map<String, Object> fileInfo = new HashMap<String, Object>();
				
				this.fileInfoList.add(fileInfo);
				
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				fileInfo.put("imageFile", imageFile);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				ig2.setFont(new Font(this.fontName, Font.BOLD, this.fontSize));
				ig2.setColor(new Color(0, 0, 255));		
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
				for(int k=0 ; k<rows-1 ; k++) {
					
					for(int l=0 ; l<cols-1 ; l++) {
						
						if(k % 30 != 0 || l % 20 != 0) {
							continue;
						}
						
						float u = regridValuesXWind[k][l];
						float v = regridValuesYWind[k][l];
						
						if(u == -999 || v == -999) {
							continue;
						}
							
						double wd = Math.atan2(u, v) * 180 / Math.PI + 180;		
						double ws = Math.sqrt(u*u + v*v);
						int temp = (int)(regridValuesTemp[k][l] - 273.15);		
							
						int tailIndex = (int)Math.floor(ws * 1.943844 / 5);
						
						if(tailIndex > 19) {
							tailIndex = 19;
						}
						
						double xCoord = boundLonLat.getLeft() + l * lonTerm;							
						double yCoord = boundLonLat.getTop() - k * latTerm;
						
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
				
				BufferedImage wintemBaseImg = ImageIO.read(new File(String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/"+wintemBaseImgFileName)));
				
				wintemBaseImg.getGraphics().drawImage(bi, 0, 0, wintemBaseImg.getWidth(), wintemBaseImg.getHeight(), null);
				
				this.appendWintemLabel(wintemBaseImg, issuedDt, fcstDt, heightText, fcstHour);
				
				wintemBaseImg = Thumbnails.of(wintemBaseImg).imageType(BufferedImage.TYPE_INT_ARGB).forceSize(resizeImgWidth, resizeImgHeight).asBufferedImage();
				
				ImageIO.write(wintemBaseImg, "PNG", imageFile);
				
				System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
				
				///////////////////////////////////////////////////////////////////////////////////
				
				System.out.println();
				
				String xmlFileName = imgFileName.replace("jpg", "xml");
				
				File xmlFile = new File(savePath + File.separator + xmlFileName);
				
				System.out.println("\t\t-> Start Write Xml [" + xmlFile.getAbsolutePath() + "]");
				
				fileInfo.put("xmlFile", xmlFile);
				
				FileWriter writer = new FileWriter(xmlFile);
				
				XMLStreamWriter xmlwriter = factory.createXMLStreamWriter(writer);
				
				xmlwriter.writeStartDocument();
			    
			    xmlwriter.writeStartElement("wintem");
			    
			    xmlwriter.writeAttribute("model", "kim_rdps");
			    xmlwriter.writeAttribute("timezone", "utc");
			    xmlwriter.writeAttribute("issued_dt", new SimpleDateFormat("yyyyMMddHH").format(issuedDt));
			    xmlwriter.writeAttribute("fcst_dt", new SimpleDateFormat("yyyyMMddHH").format(fcstDt));
			    xmlwriter.writeAttribute("height", "FL"+heightText);
			    xmlwriter.writeAttribute("ws_unit", "knot");
			    xmlwriter.writeAttribute("wd_unit", "degree");
			    xmlwriter.writeAttribute("temp_unit", "℃");
			    xmlwriter.writeAttribute("projection", "epsg:4326");
			    xmlwriter.writeAttribute("boundary", cropTop + " " + cropBottom + " " + cropLeft + " " + cropRight);
			    				    
				for(int k=0 ; k<rows-1 ; k++) {
					
					for(int l=0 ; l<cols-1 ; l++) {
						
						if(k % 35 != 0 || l % 25 != 0) {
							continue;
						}
						
						float u = regridValuesXWind[k][l];
						float v = regridValuesYWind[k][l];
						
						if(u == -999 || v == -999) {
							continue;
						}
							
						double wd = Math.atan2(u, v) * 180 / Math.PI + 180;		
						double ws = Math.sqrt(u*u + v*v) * 1.943844;
						int temp = (int)(regridValuesTemp[k][l] - 273.15);		
												
						double xCoord = boundLonLat.getLeft() + l * lonTerm;							
						double yCoord = boundLonLat.getTop() - k * latTerm;
						
						if(xCoord < cropLeft || xCoord > cropRight || yCoord < cropBottom || yCoord > cropTop) {
							continue;
						}
					
						xmlwriter.writeStartElement("grid");
						xmlwriter.writeAttribute("lat", yCoord+"");
					    xmlwriter.writeAttribute("lon", xCoord+"");
					    
						xmlwriter.writeStartElement("wd");
						xmlwriter.writeCharacters(wd+"");
						xmlwriter.writeEndElement();
						xmlwriter.writeStartElement("ws");
						xmlwriter.writeCharacters(ws+"");
						xmlwriter.writeEndElement();
						xmlwriter.writeStartElement("temp");
						xmlwriter.writeCharacters(temp+"");
						xmlwriter.writeEndElement();						   
					    
					    xmlwriter.writeEndElement();
					}
				}
				
				xmlwriter.writeEndElement();					
				xmlwriter.writeEndDocument();
				
				writer.flush();
				writer.close();
				
				System.out.println("\t\t-> End Write Xml [" + xmlFile.getAbsolutePath() + "]");
				System.out.println();
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
	
	private void appendWintemLabel(BufferedImage wintemBaseImg, Date issuedDt, Date fcstDt, String feet, int fcstHour) {

		final int labelWidth = 700;
		final int labelHeight = 320;
		final int lineHeight = 40;
		final int labelTopMargin = 10;		
		final int fontLeftMargin = 10;
		
		SimpleDateFormat labelDateFormat = new SimpleDateFormat("HH \'UTC\' dd MM yyyy"); 
				
		Graphics2D ig2 = wintemBaseImg.createGraphics();
		ig2.setColor(new Color(255, 255, 255));
		ig2.fillRect(wintemBaseImg.getWidth() - labelWidth, wintemBaseImg.getHeight() - labelHeight, labelWidth, labelHeight);
		
		ig2.setStroke(new BasicStroke(3));
		ig2.setColor(new Color(0, 0, 0));
		ig2.drawRect(wintemBaseImg.getWidth() - labelWidth, wintemBaseImg.getHeight() - labelHeight, labelWidth, labelHeight);
		
		ig2.setFont(new Font(fontName, Font.PLAIN, 40));
		ig2.setColor(new Color(0, 0, 0));	
							
		ig2.drawString("ISSUED BY AMO", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*1);
		ig2.drawString("WIND/TEMPERATURE FL " + feet, wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*2);
		ig2.drawString("FIXED TIME PROGNOSTIC CHART", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*3);
		
		ig2.drawString("BASED ON " + labelDateFormat.format(issuedDt), wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*5);
		
		ig2.drawString("VALID " + labelDateFormat.format(fcstDt), wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*4);
		
		ig2.setFont(new Font(fontName, Font.PLAIN, 30));
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
		
		String wtailImageDir = String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/wtail/");
		
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
	
	private Map<String, Float[][]> getWintemDataSet(NetcdfDataset ncFile, int rows, int cols, BoundXY boundXY, String heightIndexes) throws Exception {
		
		String[] heightIndexList = heightIndexes.split(",");
		
		System.out.println("\t\t-> Start Read Variable [u-component_of_wind_isobaric, v-component_of_wind_isobaric, Temperature_isobaric]");
		System.out.println("\t\t-> heightIndexes: " + heightIndexes);
		
		List<Float[][]> valuesUWindList = new ArrayList<Float[][]>();
		List<Float[][]> valuesVWindList = new ArrayList<Float[][]>();
		List<Float[][]> valuesTempList = new ArrayList<Float[][]>();
		
		for(int i=0 ; i<heightIndexList.length ; i++) {
			
			String heightIndex = heightIndexList[i];
			
			Variable varUWind = ncFile.findVariable("u-component_of_wind_isobaric");
			Variable varVWind = ncFile.findVariable("v-component_of_wind_isobaric");
			Variable varTemp = ncFile.findVariable("Temperature_isobaric");
			
			List<Range> rangeList = new ArrayList<Range>();
			rangeList.add(new Range(0, 0));		
			rangeList.add(new Range(Integer.valueOf(heightIndex), Integer.valueOf(heightIndex)));						
			rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
			rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
		
			valuesUWindList.add(GridCalcUtil.convertStorageToValues(varUWind.read(rangeList).getStorage(), rows, cols));
			valuesVWindList.add(GridCalcUtil.convertStorageToValues(varVWind.read(rangeList).getStorage(), rows, cols));
			valuesTempList.add(GridCalcUtil.convertStorageToValues(varTemp.read(rangeList).getStorage(), rows, cols));
		}
		
		Map<String, Float[][]> wintemDataSet = new HashMap<String, Float[][]>();
		
		if(heightIndexList.length > 1) {
	
			Float[][] valuesUWind = new Float[rows][cols];
			Float[][] valuesVWind = new Float[rows][cols];
			Float[][] valuesTemp = new Float[rows][cols];

			for(int i=0 ; i<rows ; i++) {			
				for(int j=0 ; j<cols ; j++) {				
					
					float u = 0;
					float v = 0;
					float temp = 0;
					
					for(int k=0 ; k<heightIndexList.length ; k++) {
						u += valuesUWindList.get(k)[i][j];
						v += valuesVWindList.get(k)[i][j];
						temp += valuesTempList.get(k)[i][j];
					}
					
					valuesUWind[i][j] = u / (float)(heightIndexList.length);
					valuesVWind[i][j] = v / (float)(heightIndexList.length);
					valuesTemp[i][j] = temp / (float)(heightIndexList.length);
				}
			}
			
			wintemDataSet.put("valuesUWind", valuesUWind);
			wintemDataSet.put("valuesVWind", valuesVWind);
			wintemDataSet.put("valuesTemp", valuesTemp);
			
			
		} else {
			
			wintemDataSet.put("valuesUWind", valuesUWindList.get(0));
			wintemDataSet.put("valuesVWind", valuesVWindList.get(0));
			wintemDataSet.put("valuesTemp", valuesTempList.get(0));
		}
		
		System.out.println("\t\t-> End Read Variable [u-component_of_wind_isobaric, v-component_of_wind_isobaric, Temperature_isobaric]");
		
		return wintemDataSet;
	}
	
	private List<Map<String, Object>> getWintemUseDataInfoList() {
		
		// 각 고도별 데이터 활용 정보
		List<Map<String, Object>> wintemUseDataInfoList = new ArrayList<Map<String, Object>>();
		
		Map<String, Object> wintemUseDataInfo = null;
		
		// FT1000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "22");
		wintemUseDataInfo.put("heightText", "010");
		wintemUseDataInfoList.add(wintemUseDataInfo);

		// FT2000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "21,20");
		wintemUseDataInfo.put("heightText", "020");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT2500		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "20");
		wintemUseDataInfo.put("heightText", "025");
		wintemUseDataInfoList.add(wintemUseDataInfo);

		// FT3000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "19");
		wintemUseDataInfo.put("heightText", "030");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT4000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "18");
		wintemUseDataInfo.put("heightText", "040");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT5000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "17");
		wintemUseDataInfo.put("heightText", "050");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT6000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "16");
		wintemUseDataInfo.put("heightText", "060");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT7000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "16,15");
		wintemUseDataInfo.put("heightText", "070");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT8000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "15");
		wintemUseDataInfo.put("heightText", "080");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT9000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "15,14");
		wintemUseDataInfo.put("heightText", "090");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT10000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "14");
		wintemUseDataInfo.put("heightText", "100");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT12000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "13");
		wintemUseDataInfo.put("heightText", "120");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT14000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "12");
		wintemUseDataInfo.put("heightText", "140");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT16000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "11");
		wintemUseDataInfo.put("heightText", "160");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT18000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "10");
		wintemUseDataInfo.put("heightText", "180");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT21000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "9");
		wintemUseDataInfo.put("heightText", "210");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT24000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "8");
		wintemUseDataInfo.put("heightText", "240");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT27000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "7");
		wintemUseDataInfo.put("heightText", "270");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT30000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "6");
		wintemUseDataInfo.put("heightText", "300");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT34000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "5");
		wintemUseDataInfo.put("heightText", "340");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT39000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "4");
		wintemUseDataInfo.put("heightText", "390");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT44000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "3");
		wintemUseDataInfo.put("heightText", "440");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		// FT52000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "2");
		wintemUseDataInfo.put("heightText", "520");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		return wintemUseDataInfoList;
	}
}
