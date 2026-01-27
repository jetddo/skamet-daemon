package kama.daemon.model.observation.adopt.RDR.proc;

import java.awt.Color;
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

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.common.util.EndianDataInputStream;
import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.Lambert.GRID_PARAM_TYPE;
import kama.daemon.common.util.converter.PointF;

public class RadarKmaHsrConveter {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private RadarHeader radarHeader;
	
	private Lambert lambert = new Lambert(GRID_PARAM_TYPE.RDR_KMA_HSR);
	private double maxLat = -1;
	private double maxLon = -1;
	private double minLat = 91;
	private double minLon = 181;
	private double latTerm, lonTerm;
	
	public RadarKmaHsrConveter(DaemonSettings daemonSettings, ProcessorInfo processorInfo) {		
		this.daemonSettings = daemonSettings;
		this.processorInfo = processorInfo;
	}
	
	private short[][] readRadarLayerRawData(EndianDataInputStream dis) throws IOException {
		
		short[][] radarLayerRawData = new short[this.radarHeader.ny][];
		
		byte[] buffer = new byte[this.radarHeader.nx * 2];
		
		for(int i=0 ; i<this.radarHeader.ny ; i++) {
			
			radarLayerRawData[i] = new short[this.radarHeader.nx];			
			
			dis.readFully(buffer);
			
			for(int j=0 ; j<this.radarHeader.nx ; j++) {
				radarLayerRawData[i][j] = ByteBuffer.wrap(buffer, j * 2, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
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
	
	private short[][] regridRadarLayer(short[][] radarLayerRawData) {
		
		short[][] radarLayerRegridData = new short[this.radarHeader.ny][];
		
		for(int i=0 ; i<this.radarHeader.ny ; i++) {
			
			radarLayerRegridData[i] = new short[this.radarHeader.nx];
			
			for(int j=0 ; j<this.radarHeader.nx ; j++) {
				radarLayerRegridData[i][j] = -999;
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
	
	@SuppressWarnings("unused")
	public void parseRadarFile(File radarGZipFile) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start KMA HSR Radar Image Converter :::::");
		
		try {
			
			byte[] gZipBuffer = new GZipTgzReader(radarGZipFile.getAbsolutePath()).readAllBytes();
		
			EndianDataInputStream dis = new EndianDataInputStream(ByteSource.wrap(gZipBuffer).openStream());
			dis.order(ByteOrder.LITTLE_ENDIAN);			
			
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA HSR Radar Header Info :::::");
			this.parseRadarHeaderInfo(dis);
			
			this.setLambertEnv();
			
			System.out.println(this.radarHeader);

			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA HSR Radar Layer Raw Data :::::");
			
			short[][] radarLayerRegridData = this.regridRadarLayer(this.readRadarLayerRawData(dis));
			
			this.createImage(radarLayerRegridData, radarGZipFile.getName(), 0);
		
			dis.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End KMA HSR Radar Image Converter :::::");
	}
	
	public Color getColor(short _data) {
		
		if(_data == -999) {
			return null;
		}
		
		float data = _data/100f;
		
		if(data > 0.0f && data < 0.1f) {
			return new Color(0,200,255);
		} else if(data >= 0.1f && data < 0.5f) {
			return new Color(0,155,245);
		} else if(data >= 0.5f && data < 1.0f) {
			return new Color(0,51,245);
		} else if(data >= 1.0f && data < 2.0f) {
			return new Color(0,255,0);
		} else if(data >= 2.0f && data < 3.0f) {
			return new Color(0,190,0);
		} else if(data >= 3.0f && data < 4.0f) {
			return new Color(0,140,0);
		} else if(data >= 4.0f && data < 5.0f) {
			return new Color(0,90,0);
		} else if(data >= 5.0f && data < 6.0f) {
			return new Color(255,255,0);
		} else if(data >= 6.0f && data < 7.0f) {
			return new Color(255,220,31);
		} else if(data >= 7.0f && data < 8.0f) {
			return new Color(249,205,0);
		} else if(data >= 8.0f && data < 9.0f) {
			return new Color(224,185,0);
		} else if(data >= 9.0f && data < 10.0f) {
			return new Color(204,170,0);
		} else if(data >= 10.0f && data < 15.0f) {
			return new Color(255,102,0);
		} else if(data >= 15.0f && data < 20.0f) {
			return new Color(255,50,0);
		} else if(data >= 20.0f && data < 25.0f) {
			return new Color(210,0,0);
		} else if(data >= 25.0f && data < 30.0f) {
			return new Color(180,0,0);
		} else if(data >= 30.0f && data < 40.0f) {
			return new Color(224,169,255);
		} else if(data >= 40.0f && data < 50.0f) {
			return new Color(201,105,255);
		} else if(data >= 50.0f && data < 60.0f) {
			return new Color(179,41,255);
		} else if(data >= 60.0f && data < 70.0f) {
			return new Color(147,0,228);
		} else if(data >= 70.0f && data < 80.0f) {
			return new Color(179,180,222);
		} else if(data >= 80.0f && data < 90.0f) {
			return new Color(76,78,177);
		} else if(data >= 90.0f && data < 110.0f) {
			return new Color(0,3,144);
		} else if(data >= 110.0f) {
			return new Color(51,51,51);	
		} else {
			return null;
		}
	}
	
	private void createImage(short[][] radarLayerRegridData, String fileName, int index) {
		
		File imageFileDir = new File(String.format("%s/RDR_IMG/%s/%s", this.daemonSettings.OutputRootPath(), this.processorInfo.ClassPrefix, DateFormatter.formatDate(processorInfo.FileDateFromNameOriginal, "yyyy/MM/dd")));
		
		if(!imageFileDir.exists() && !imageFileDir.mkdirs()) {
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Error : Cannot Create Directory ( " + imageFileDir.getAbsolutePath() + " )" );
		}
		
		File imageFile = new File(imageFileDir.getAbsolutePath() + File.separator + String.format("%s_%03d.png", fileName.replaceFirst("[.][^.]+$", ""), 0));
        	
		try {
						
			BufferedImage bi = new BufferedImage(this.radarHeader.nx, this.radarHeader.ny, BufferedImage.TYPE_INT_ARGB);	
			
			Graphics2D graphics2D = bi.createGraphics();
			
			for(int i=0 ; i<this.radarHeader.ny ; i++) {
				
				for(int j=0 ; j<this.radarHeader.nx ; j++) {
					
	               Color color = this.getColor(radarLayerRegridData[i][j]);
	               
	               if(color != null) {
	            	
	            	   graphics2D.setColor(color);
	            	   graphics2D.drawLine(j, this.radarHeader.ny - i - 1, j, this.radarHeader.ny - i - 1);	
	               }
				}
			}
			
			ImageIO.write(bi, "PNG", imageFile);		
			
		} catch (IOException e) {
			
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Create Image : " + imageFile.getAbsolutePath());
	}

	private void parseRadarHeaderInfo(EndianDataInputStream dis) throws IOException {
		
		radarHeader = new RadarHeader();
		
		radarHeader.version = dis.readByte();
		radarHeader.ptype = dis.readShort();
		
		radarHeader.tm_yy = dis.readShort();
		radarHeader.tm_mm = dis.readByte();
		radarHeader.tm_dd = dis.readByte();
		radarHeader.tm_hh = dis.readByte();
		radarHeader.tm_mi = dis.readByte();
		radarHeader.tm_ss = dis.readByte();
		
		radarHeader.tm_str = String.format("%d%02d%02d%02d%02d%02d", radarHeader.tm_yy, radarHeader.tm_mm, radarHeader.tm_dd, radarHeader.tm_hh, radarHeader.tm_mi, radarHeader.tm_ss);
		
		radarHeader.tm_in_yy = dis.readShort();
		radarHeader.tm_in_mm = dis.readByte();
		radarHeader.tm_in_dd = dis.readByte();
		radarHeader.tm_in_hh = dis.readByte();
		radarHeader.tm_in_mi = dis.readByte();
		radarHeader.tm_in_ss = dis.readByte();
		
		radarHeader.tm_in_str = String.format("%d%02d%02d%02d%02d%02d", radarHeader.tm_in_yy, radarHeader.tm_in_mm, radarHeader.tm_in_dd, radarHeader.tm_in_hh, radarHeader.tm_in_mi, radarHeader.tm_in_ss);
		
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
		
		dis.read(radarHeader.data_code);
		dis.read(radarHeader.etc);
		
		// 레이더 지점 정보는 읽지 말자
		dis.skip(20*48);
	}
	
	@SuppressWarnings("unused")
	private class RadarHeader {
		
		// 포맷버전
		public int version;
		
		// 생산Product
		public int ptype;
		
		// 레이더 관측시각
		public String tm_str; 
		
		public int tm_yy;
		public int tm_mm;
		public int tm_dd;
		public int tm_hh;
		public int tm_mi;
		public int tm_ss;
		
		// 합성자료 생성시각
		public String tm_in_str; 
		
		public int tm_in_yy;
		public int tm_in_mm;
		public int tm_in_dd;
		public int tm_in_hh;
		public int tm_in_mi;
		public int tm_in_ss;
		
		// 합성에 사용된 레이더 지점수		
		public int num_stn;
		
		// 지도정보 코드
		public int map_code;
		
		//기타 지도 정보코드
		public int map_etc;
		
		// X축 격자점수
		public int nx;
		
		// Y축 격자점수
		public int ny;
		
		// Z축 격자점수
		public int nz;
		
		// 격자점의 수평거리(m)
		public int dxy;
		
		// 격자점의 수직거리(m) (nz=1 이면 dz=0)
		public int dz;
		
		// nz>1 인 경우, 최저고도값(m) (nz <= 1이면 0)
		public int z_min;
		
		// (nx*ny*nz)를 1개 자료블럭으로 했을때, 저장된 자료블럭수
		public int num_data;
		
		// 저장된 자료블럭별 특성 코드
		public byte[] data_code = new byte[16];
		
		// 예비
		public byte[] etc = new byte[15];
		
		//////////////////////////////////////////////////////////////////
			
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
