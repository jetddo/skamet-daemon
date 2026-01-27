package kama.daemon.model.observation.adopt.RDR.proc;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.imageio.ImageIO;

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.common.util.EndianDataInputStream;
import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.Lambert.GRID_PARAM_TYPE;
import kama.daemon.common.util.converter.PointF;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

import com.google.common.io.ByteSource;

public class RadarKmaWdConveter2 {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	
	private Lambert lambert = new Lambert(GRID_PARAM_TYPE.RDR_KMA_WD);
	private double maxLat = -1;
	private double maxLon = -1;
	private double minLat = 91;
	private double minLon = 181;
	private double latTerm, lonTerm;
	
	private int nz = 56;
	private int ny = 960;
	private int nx = 960;
	private int data_out = -30000;
	private int data_scale = 100;
	
	private final int reshapeScaleX = 30;
	private final int reshapeScaleY = 30;
	
	private final int defaultArrowWidth = 30;
	private final int defaultArrowHeight = 30;
	
	private static BufferedImage[][] arrowImageList;
	
	private double[] windLegendList = new double[]{0, 0.04, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 8.0, 9.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 90.0};
	
	public RadarKmaWdConveter2(DaemonSettings daemonSettings, ProcessorInfo processorInfo) {	
		
		this.daemonSettings = daemonSettings;
		this.processorInfo = processorInfo;
	}
	
	private short[][] readRadarLayerWindRawData(Variable var, int index) throws IOException, InvalidRangeException {
		
		List<Range> rangeList = new ArrayList<Range>();
		rangeList.add(new Range(index, index));
		rangeList.add(new Range(0, ny-1));
		rangeList.add(new Range(0, nx-1));
		
		short[] storage = (short[])var.read(rangeList).getStorage();
		
		short[][] radarLayerRawData = new short[this.ny][]; // x-wind
		
		for(int i=0 ; i<this.ny ; i++) {
			
			radarLayerRawData[i] = new short[this.nx];			
			
			for(int j=0 ; j<this.nx ; j++) {
				radarLayerRawData[i][j] = storage[ny * i + j];
			}
		}
		
		return radarLayerRawData;
	}
	
	private void setLambertEnv() {
		
        for(int i=0 ; i<this.ny ; i++) {
        	
        	for(int j=0 ; j<this.nx ; j++) {
        	
        		PointF coord = this.lambert.lambertToWgs84(j, i);
        		
        		this.maxLat = Math.max(this.maxLat, coord.y);
        		this.maxLon = Math.max(this.maxLon, coord.x);
        		this.minLat = Math.min(this.minLat, coord.y);
        		this.minLon = Math.min(this.minLon, coord.x);        		
        	}
        }
        
        this.latTerm = (this.maxLat - this.minLat) / (this.ny-1);
        this.lonTerm = (this.maxLon - this.minLon) / (this.nx-1);
        
        System.out.println("maxLat : " + this.maxLat);
        System.out.println("maxLon : " + this.maxLon);
        System.out.println("minLat : " + this.minLat);
        System.out.println("minLon : " + this.minLon);
	}
	
	private short[][] regridRadarLayer(short[][] radarLayerRawData) {
		
		short[][] radarLayerRegridData = new short[this.ny][];
		
		for(int i=0 ; i<this.ny ; i++) {
			
			radarLayerRegridData[i] = new short[this.nx];
			
			for(int j=0 ; j<this.nx ; j++) {
				radarLayerRegridData[i][j] = -999;
			}
		}
		
		for(int i=0 ; i<this.ny ; i++) {
			
			for(int j=0 ; j<this.nx ; j++) {
				
				PointF coord = lambert.lambertToWgs84(j, i);
                int y = (int) ((coord.y - minLat) / latTerm);
                int x = (int) ((coord.x - minLon) / lonTerm);

                radarLayerRegridData[y][x] = radarLayerRawData[i][j];
			}
		}
		
		return radarLayerRegridData;
	}
	
	private short[][] reshapeRadarLayer(short[][] radarLayerRegridData) {
		
		short[][] radarLayerReshapeData = new short[this.ny / this.reshapeScaleY][];
		
		for(int i=0 ; i<this.ny / this.reshapeScaleY ; i++) {
			
			radarLayerReshapeData[i] = new short[this.nx / this.reshapeScaleX];
			
			for(int j=0 ; j<this.nx / this.reshapeScaleX ; j++) {			
				radarLayerReshapeData[i][j] = this.reshapeGridData(radarLayerRegridData, i, j);
			}
		}
		
		return radarLayerReshapeData;
	}
	
	private short reshapeGridData(short[][] radarLayerRegridData, int y, int x) {
		
		for(int i=y*this.reshapeScaleY ; i<y*this.reshapeScaleY + this.reshapeScaleY ; i++) {
			
			for(int j=x*this.reshapeScaleX ; j<x*this.reshapeScaleX + this.reshapeScaleX ; j++) {
				
			}
		}
		
		return radarLayerRegridData[y*this.reshapeScaleY][x*this.reshapeScaleX];
	}
	
	public void parseRadarFile(File radarNcFile) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start KMA WD Radar Image Converter :::::");
		
		try {
			
			this.createArrowImages();
			
			this.setLambertEnv();
		
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA WD Radar Layer Raw Data :::::");
			
			for(int i=0 ; i<this.nz ; i++) {	
				
				if(i == 6) {

					NetcdfDataset ncFile = NetcdfDataset.acquireDataset(radarNcFile.getAbsolutePath(), null);
		        	
					Variable uVar = ncFile.findVariable("u_component");
					Variable vVar = ncFile.findVariable("v_component");
					
					short[][] radarLayerXWindReshapeData = this.reshapeRadarLayer(this.regridRadarLayer(this.readRadarLayerWindRawData(uVar, i)));		
					short[][] radarLayerYWindReshapeData = this.reshapeRadarLayer(this.regridRadarLayer(this.readRadarLayerWindRawData(vVar, i)));
					
					this.createImage(radarLayerXWindReshapeData, radarLayerYWindReshapeData, radarNcFile.getName(), i);

		            ncFile.close();	
				}
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End KMA WD Radar Image Converter :::::");
	}
	
	private void createImage(short[][] radarLayerXWindReshapeData, short[][] radarLayerYWindReshapeData, String fileName, int index) {
		
		File imageFileDir = new File(String.format("%s/RDR_IMG/%s/%s", this.daemonSettings.OutputRootPath(), this.processorInfo.ClassPrefix, DateFormatter.formatDate(processorInfo.FileDateFromNameOriginal, "yyyy/MM/dd")));
		
		if(!imageFileDir.exists() && !imageFileDir.mkdirs()) {
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Error : Cannot Create Directory ( " + imageFileDir.getAbsolutePath() + " )" );
		}
		
		File imageFile = new File(imageFileDir.getAbsolutePath() + File.separator + String.format("%s_%03d.png", fileName, index + 1));
		
		try {
						
			BufferedImage bi = new BufferedImage(this.nx, this.ny, BufferedImage.TYPE_INT_ARGB);	
			
			Graphics2D graphics2D = bi.createGraphics();
			
			for(int i=0 ; i<this.ny / this.reshapeScaleY ; i++) {
				
				for(int j=0 ; j<this.nx / this.reshapeScaleX ; j++) {
				
					short u = radarLayerXWindReshapeData[i][j];
					short v = radarLayerYWindReshapeData[i][j];
					
					if(u == this.data_out || v == this.data_out || u == -999 || v == -999) {
						continue;
					}
					
					u /= this.data_scale;
					v /= this.data_scale;
					
					double wd = Math.atan2(u, v) * 180 / Math.PI;		
					double ws = Math.sqrt(u*u + v*v);
						
					double arrowWidth = defaultArrowWidth * Math.pow(ws/50, 1.0/10.0);
					double arrowHeight = defaultArrowHeight * Math.pow(ws/50, 1.0/10.0);
					
					if(arrowWidth < 1 || arrowHeight < 1) {
						continue;
					}
					
					BufferedImage bii = Thumbnails.of(arrowImageList[this.getArrowIndex(ws)][(int)((wd + 360) % 360) / 10]).imageType(BufferedImage.TYPE_INT_ARGB).forceSize((int)arrowWidth, (int)arrowHeight).asBufferedImage();
					
//					graphics2D.drawImage(arrowImageList[this.getArrowIndex(ws)][(int)((wd + 360) % 360) / 10], j * this.reshapeScaleX, (this.ny / this.reshapeScaleY - i - 1) * this.reshapeScaleY, defaultArrowWidth, defaultArrowHeight, null);					
					graphics2D.drawImage(bii, j * this.reshapeScaleX + (int)((this.reshapeScaleX - arrowWidth) / 2)
							, (this.ny / this.reshapeScaleY - i - 1) * this.reshapeScaleY + (int)((this.reshapeScaleY - arrowHeight) / 2)
							, null);
				}
			}
			
			ImageIO.write(bi, "PNG", imageFile);		
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Create Image : " + imageFile.getAbsolutePath());
	}
	
	private int getArrowIndex(double data) {
		
		for(int i=0 ; i<this.windLegendList.length ; i++) {
			
			if(i == this.windLegendList.length - 1) {
			
				if(data >= this.windLegendList[this.windLegendList.length - 1]) {
					return i;
				}
				
			} else {
				
				if(data >= this.windLegendList[i] && data < this.windLegendList[i+1]) {
					return i;
				}
			}
		}
		
		return 0;
	}
	
	private void createArrowImages() throws IOException {
		
		if(arrowImageList != null) {
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Already Arrow Images Created :::::");
			return;
		} else {
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Create Arrow Image List :::::");	
		}
		
		String arrowImageDir = String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/arrows/");
		
		arrowImageList = new BufferedImage[this.windLegendList.length][];
		
		for(int i=0 ; i<arrowImageList.length ; i++) {
			
			arrowImageList[i] = new BufferedImage[36];
			
			for(int j=0 ; j<arrowImageList[i].length ; j++) {
				
				File originImageFile = new File(arrowImageDir + "/arrow_" + String.format("%02d", i) + ".png");
					
				arrowImageList[i][j] = Thumbnails.of(originImageFile)
						.imageType(BufferedImage.TYPE_INT_ARGB)
						.rotate(j * 10).scale(1).asBufferedImage();
			}
		}
	}
}
