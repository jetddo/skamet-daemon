package kama.daemon.main.test;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.imageio.ImageIO;

import com.google.common.io.ByteSource;

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

public class RadarKmaWd2Test {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
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
	
	private final int reshapeScaleX = 15;
	private final int reshapeScaleY = 15;
	 
	private BufferedImage[][] arrowImageList;
	
	private double[] windLegendList = new double[]{0, 0.04, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 8.0, 9.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 90.0};
	
	private float[][] readRadarLayerWindRawData(Variable var, int index) throws IOException, InvalidRangeException {
		
		List<Range> rangeList = new ArrayList<Range>();
		rangeList.add(new Range(index, index));
		rangeList.add(new Range(0, ny-1));
		rangeList.add(new Range(0, nx-1));
		
		short[] storage = (short[])var.read(rangeList).getStorage();
		
		float[][] radarLayerRawData = new float[this.ny][]; // x-wind
		
		for(int i=0 ; i<this.ny ; i++) {
			
			radarLayerRawData[i] = new float[this.nx];			
			
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
	
	private float[][] regridRadarLayer(float[][] radarLayerRawData) {
		
		float[][] radarLayerRegridData = new float[this.ny][];
		
		for(int i=0 ; i<this.ny ; i++) {
			
			radarLayerRegridData[i] = new float[this.nx];
			
			for(int j=0 ; j<this.nx ; j++) {
				radarLayerRegridData[i][j] = -999f;
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
	
	private float[][] reshapeRadarLayer(float[][] radarLayerRegridData) {
		
		float[][] radarLayerReshapeData = new float[this.ny / this.reshapeScaleY][];
		
		for(int i=0 ; i<this.ny / this.reshapeScaleY ; i++) {
			
			radarLayerReshapeData[i] = new float[this.nx / this.reshapeScaleX];
			
			for(int j=0 ; j<this.nx / this.reshapeScaleX ; j++) {			
				radarLayerReshapeData[i][j] = this.reshapeGridData(radarLayerRegridData, i, j);
			}
		}
		
		return radarLayerReshapeData;
	}
	
	private float reshapeGridData(float[][] radarLayerRegridData, int y, int x) {
		
		for(int i=y*this.reshapeScaleY ; i<y*this.reshapeScaleY + this.reshapeScaleY ; i++) {
			
			for(int j=x*this.reshapeScaleX ; j<x*this.reshapeScaleX + this.reshapeScaleX ; j++) {
				
			}
		}
		
		return radarLayerRegridData[y*this.reshapeScaleY][x*this.reshapeScaleX];
	}
	
	public void parseRadarFile(String radarFilePath) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start KMA WD Radar Image Converter :::::");
		
		try {
			
			this.createArrowImages();
					
			this.setLambertEnv();
	
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA WD Radar Layer Raw Data :::::");
			
			for(int i=0 ; i<this.nz ; i++) {	
				
				if(true) {
					
					NetcdfDataset ncFile = NetcdfDataset.acquireDataset(radarFilePath, null);
		        	
					Variable uVar = ncFile.findVariable("u_component");
					Variable vVar = ncFile.findVariable("v_component");
					
					float[][] radarLayerXWindReshapeData = this.reshapeRadarLayer(this.regridRadarLayer(this.readRadarLayerWindRawData(uVar, i)));		
					float[][] radarLayerYWindReshapeData = this.reshapeRadarLayer(this.regridRadarLayer(this.readRadarLayerWindRawData(vVar, i)));
					
					this.createImage(radarLayerXWindReshapeData, radarLayerYWindReshapeData, i);

		            ncFile.close();				
				}
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End KMA WD Radar Image Converter :::::");
	}
	
	private void createImage(float[][] radarLayerXWindReshapeData, float[][] radarLayerYWindReshapeData, int index) {
		
		String imageFilePath = "F:/data/radar_kma_wd/radar_kma_wd_" + String.format("%03d", index)  + ".png";
		
		try {
						
			BufferedImage bi = new BufferedImage(this.nx, this.ny, BufferedImage.TYPE_INT_ARGB);	
			
			Graphics2D graphics2D = bi.createGraphics();
			
			for(int i=0 ; i<this.ny / this.reshapeScaleY ; i++) {
				
				for(int j=0 ; j<this.nx / this.reshapeScaleX ; j++) {
				
					float u = radarLayerXWindReshapeData[i][j];
					float v = radarLayerYWindReshapeData[i][j];
					
					if((int)u == this.data_out || (int)v == this.data_out || u == -999f || v == -999f) {
						continue;
					}
					
					u /= this.data_scale;
					v /= this.data_scale;
					
					double wd = Math.atan2(u, v) * 180 / Math.PI;		
					double ws = Math.sqrt(u*u + v*v);
						
					graphics2D.drawImage(arrowImageList[this.getArrowIndex(ws)][(int)((wd + 360) % 360) / 10], j * this.reshapeScaleX, (this.ny / this.reshapeScaleY - i - 1) * this.reshapeScaleY, null);					
				}
			}
			
			ImageIO.write(bi, "PNG", new File(imageFilePath));		
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Create Image : " + imageFilePath);
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
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Create Arrow Image List :::::");
		
		this.arrowImageList = new BufferedImage[this.windLegendList.length][];
		
		for(int i=0 ; i<this.arrowImageList.length ; i++) {
			
			this.arrowImageList[i] = new BufferedImage[36];
			
			for(int j=0 ; j<this.arrowImageList[i].length ; j++) {
				
				File originImageFile = new File("F:/data/arrows/arrow_" + i + ".png");
					
				this.arrowImageList[i][j] = Thumbnails.of(originImageFile)
						.imageType(BufferedImage.TYPE_INT_ARGB)
						.rotate(j * 10)
						.size(this.reshapeScaleX, this.reshapeScaleY).asBufferedImage();
			}
		}
	}

	public static void main(String[] args) {
		
		RadarKmaWd2Test radarTest = new RadarKmaWd2Test();
		
		radarTest.parseRadarFile("F:/data/ftp/RDR_R3D_KMA_WD_202005131550.nc");
	}
}
