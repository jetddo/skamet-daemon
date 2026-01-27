package kama.daemon.main;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.imageio.ImageIO;

import com.google.common.io.ByteSource;

import kama.daemon.common.util.EndianDataInputStream;
import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.Lambert.GRID_PARAM_TYPE;
import kama.daemon.common.util.converter.PointF;
import net.coobird.thumbnailator.Thumbnails;

public class RadarKmaWdTest {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private RadarHeader radarHeader;
	
	private Lambert lambert = new Lambert(GRID_PARAM_TYPE.RDR_KMA_WD);
	private double maxLat = -1;
	private double maxLon = -1;
	private double minLat = 91;
	private double minLon = 181;
	private double latTerm, lonTerm;
	
	private byte[] uBuffer;
	private byte[] vBuffer;
	
	private final int reshapeScaleX = 15;
	private final int reshapeScaleY = 15;
	 
	private BufferedImage[][] arrowImageList;
	
	private double[] windLegendList = new double[]{0, 0.04, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 8.0, 9.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 90.0};
	
	private float[][] readRadarLayerWindRawData(byte[] buffer, int index) throws IOException {
		
		float[][] radarLayerRawData = new float[this.radarHeader.ny][]; // x-wind
		
		for(int i=0 ; i<this.radarHeader.ny ; i++) {
			
			radarLayerRawData[i] = new float[this.radarHeader.nx];			
			
			for(int j=0 ; j<this.radarHeader.nx ; j++) {
				radarLayerRawData[i][j] = ByteBuffer.wrap(buffer, radarHeader.nx * radarHeader.ny * 2 * index + i * this.radarHeader.nx * 2 + j * 2, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
			}
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Read Radar Layer Raw Data Size : " + (this.radarHeader.ny * this.radarHeader.nx) + " bytes");
		
		return radarLayerRawData;
	}
	
	private void setLambertEnv() {
		
        for(int i=0 ; i<this.radarHeader.ny ; i++) {
        	
        	for(int j=0 ; j<this.radarHeader.nx ; j++) {
        	
        		PointF coord = this.lambert.lambertToWgs84(j, i);
        		
        		this.maxLat = Math.max(this.maxLat, coord.y);
        		this.maxLon = Math.max(this.maxLon, coord.x);
        		this.minLat = Math.min(this.minLat, coord.y);
        		this.minLon = Math.min(this.minLon, coord.x);        		
        	}
        }
        
        this.latTerm = (this.maxLat - this.minLat) / (this.radarHeader.ny-1);
        this.lonTerm = (this.maxLon - this.minLon) / (this.radarHeader.nx-1);
        
        System.out.println("maxLat : " + this.maxLat);
        System.out.println("maxLon : " + this.maxLon);
        System.out.println("minLat : " + this.minLat);
        System.out.println("minLon : " + this.minLon);
	}
	
	private float[][] regridRadarLayer(float[][] radarLayerRawData) {
		
		float[][] radarLayerRegridData = new float[this.radarHeader.ny][];
		
		for(int i=0 ; i<this.radarHeader.ny ; i++) {
			
			radarLayerRegridData[i] = new float[this.radarHeader.nx];
			
			for(int j=0 ; j<this.radarHeader.nx ; j++) {
				radarLayerRegridData[i][j] = -999f;
			}
		}
		
		for(int i=0 ; i<this.radarHeader.ny ; i++) {
			
			for(int j=0 ; j<this.radarHeader.nx ; j++) {
				
				PointF coord = lambert.lambertToWgs84(j, i);
                int y = (int) ((coord.y - minLat) / latTerm);
                int x = (int) ((coord.x - minLon) / lonTerm);

                radarLayerRegridData[y][x] = radarLayerRawData[i][j];
			}
		}
		
		return radarLayerRegridData;
	}
	
	private float[][] reshapeRadarLayer(float[][] radarLayerRegridData) {
		
		float[][] radarLayerReshapeData = new float[this.radarHeader.ny / this.reshapeScaleY][];
		
		for(int i=0 ; i<this.radarHeader.ny / this.reshapeScaleY ; i++) {
			
			radarLayerReshapeData[i] = new float[this.radarHeader.nx / this.reshapeScaleX];
			
			for(int j=0 ; j<this.radarHeader.nx / this.reshapeScaleX ; j++) {			
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
			
			byte[] gZipBuffer = new GZipTgzReader(radarFilePath).readAllBytes();
		
			EndianDataInputStream dis = new EndianDataInputStream(ByteSource.wrap(gZipBuffer).openStream());
			dis.order(ByteOrder.LITTLE_ENDIAN);			
			
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA WD Radar Header Info :::::");
			this.parseRadarHeaderInfo(dis);
			
			this.setLambertEnv();
			
			System.out.println(this.radarHeader);
//
//			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA WD Radar Layer Raw Data :::::");
//			
//			for(int i=0 ; i<this.radarHeader.nz ; i++) {	
//				
//				if(true) {
//					
//					float[][] radarLayerXWindReshapeData = this.reshapeRadarLayer(this.regridRadarLayer(this.readRadarLayerWindRawData(this.uBuffer, i)));		
//					float[][] radarLayerYWindReshapeData = this.reshapeRadarLayer(this.regridRadarLayer(this.readRadarLayerWindRawData(this.vBuffer, i)));
//					
//					this.createImage(radarLayerXWindReshapeData, radarLayerYWindReshapeData, i);	
//				}
//			}
		
			dis.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End KMA WD Radar Image Converter :::::");
	}
	
	private void createImage(float[][] radarLayerXWindReshapeData, float[][] radarLayerYWindReshapeData, int index) {
		
		String imageFilePath = "F:/data/radar_kma_wd/radar_kma_wd_" + String.format("%03d", index)  + ".png";
		
		try {
						
			BufferedImage bi = new BufferedImage(this.radarHeader.nx, this.radarHeader.ny, BufferedImage.TYPE_INT_ARGB);	
			
			Graphics2D graphics2D = bi.createGraphics();
			
			for(int i=0 ; i<this.radarHeader.ny / this.reshapeScaleY ; i++) {
				
				for(int j=0 ; j<this.radarHeader.nx / this.reshapeScaleX ; j++) {
				
					float u = radarLayerXWindReshapeData[i][j];
					float v = radarLayerYWindReshapeData[i][j];
					
					if((short)u == this.radarHeader.data_out || (short)v == this.radarHeader.data_out || u == -999f || v == -999f) {
						continue;
					}
					
					u /= this.radarHeader.data_scale;
					v /= this.radarHeader.data_scale;
					
					double wd = Math.atan2(u, v) * 180 / Math.PI;		
					double ws = Math.sqrt(u*u + v*v);
						
					graphics2D.drawImage(arrowImageList[this.getArrowIndex(ws)][(int)((wd + 360) % 360) / 10], j * this.reshapeScaleX, (this.radarHeader.ny / this.reshapeScaleY - i - 1) * this.reshapeScaleY, null);					
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
		
		RadarKmaWdTest radarTest = new RadarKmaWdTest();
		
		radarTest.parseRadarFile("F:/data/ftp/RDR_R3D_KMA_WD_201912021300.bin.gz");
	}

	private void parseRadarHeaderInfo(EndianDataInputStream dis) throws IOException {
		
		radarHeader = new RadarHeader();
		
		radarHeader.version =  dis.readByte();
		radarHeader.ptype = dis.readShort();
		dis.readFully(radarHeader.tm);
		dis.readFully(radarHeader.tm_in);
		this.parseHeaderDate();
		radarHeader.num_stn = dis.readByte();
		radarHeader.map_code = dis.readByte();
		radarHeader.map_etc = dis.readByte();
		radarHeader.nx = dis.readShort();
		radarHeader.ny = dis.readShort();
		radarHeader.nz = dis.readShort();
		radarHeader.dxy = dis.readShort();
		radarHeader.dz = dis.readShort();
		radarHeader.z_min = dis.readShort();
		radarHeader.num_data = dis.readByte();
		radarHeader.dz2 = dis.readShort();
		radarHeader.z_min2 = dis.readShort();
		radarHeader.data_out = dis.readShort();
		radarHeader.data_in = dis.readShort();
		radarHeader.data_min = dis.readShort();
		radarHeader.data_minus = dis.readShort();
		radarHeader.data_scale = dis.readShort();
		radarHeader.data_unit = dis.readByte();
		radarHeader.num_obs = dis.readShort();
		
		dis.skipBytes(14); // skip etc
	
		this.uBuffer = new byte[radarHeader.nx * radarHeader.ny * radarHeader.nz * 2];
		this.vBuffer = new byte[radarHeader.nx * radarHeader.ny * radarHeader.nz * 2];
		
		dis.readFully(this.uBuffer);
		dis.readFully(this.vBuffer);
	}
	
	private void parseHeaderDate() {
		
		int yy, mm, dd, hh, mi, ss;
		
		yy = radarHeader.tm[1] << 8 | radarHeader.tm[0] & 0xFF;
		mm = radarHeader.tm[2];
		dd = radarHeader.tm[3];
		hh = radarHeader.tm[4];
		mi = radarHeader.tm[5];
		ss = radarHeader.tm[6];
		
		radarHeader.tm_str = String.format("%d%02d%02d%02d%02d%02d", yy, mm, dd, hh, mi, ss);
		
		yy = radarHeader.tm_in[1] << 8 | radarHeader.tm_in[0] & 0xFF;
		mm = radarHeader.tm_in[2];
		dd = radarHeader.tm_in[3];
		hh = radarHeader.tm_in[4];
		mi = radarHeader.tm_in[5];
		ss = radarHeader.tm_in[6];
		
		radarHeader.tm_in_str = String.format("%d%02d%02d%02d%02d%02d", yy, mm, dd, hh, mi, ss);
	}
	
	@SuppressWarnings("unused")
	private class RadarHeader {
		
		public byte version; //포멧버전
		public short ptype; //생산Product
		public byte[] tm = new byte[7]; //레이더 관측시각
		public String tm_str; //레이더 관측시각 yyyyMMddHHmmss
		public byte[] tm_in = new byte[7]; //합성자료 생성시각
		public String tm_in_str; //합성자료 생성시각 yyyyMMddHHmmss
		public byte num_stn; //합성에 사용된 레이더 지점수 
		public byte map_code; //지도정보 코드
		public byte map_etc; //기타 지도 정보코드(예비)
		public short nx; //X축 격자점수
		public short ny; //Y축 격자점수
		public short nz; //Z축 격자점수
		public short dxy; //격자점간의 수평거리(m)
		public short dz; //격자점간의 수직거리(m) (nz=1이면 dz=0)
		public short z_min; //nz > 1인 경우, 최저고도값(m) (nz <= 1이면 0)		
		public byte num_data; //(nx*ny*nz)를 1개 자료블럭으로 했을때, 저장된 자료블럭수
		public short dz2; //2번째 단계의 수직격자점간 거리(m)
		public short z_min2; //2번째 단계의 시작 고도값(m)
		public short data_out; //레이더 관측영역밖 NULL값
		public short data_in; //레이더 영역내 비관측영역 NULL값
		public short data_min; //관측영역내 표시를 위한 최소값
		public short data_minus; //저장된 값에서 이 값을 먼저 뺌
		public short data_scale; //빼고 난 다음에 이 값으로 나누어서 실값을 찾음		
		public byte data_unit; //위 방식으로 해독한 후에 단위 코드		
		public short num_obs; 
		short[] etc = new short[7]; //예비
	
		@Override
		public String toString() {
			
			String str = "---------- HEADER INFO ----------\n";
			
			Field[] fields = this.getClass().getFields();
			
			for(Field field : fields) {
				
				Class<?> clazz = field.getType();
				
				if(clazz.isArray()) {
					continue;
				}
				
				try {
					
					str += field.getName() + " -> " + field.get(this) + "\n";
					
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			str += "---------- HEADER INFO ----------";
			
			return str;			
		}
	}
}
