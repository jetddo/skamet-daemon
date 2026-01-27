package kama.daemon.main.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.Lambert.GRID_PARAM_TYPE;
import kama.daemon.common.util.converter.PointF;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class FetchRadarKmaWdTest {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private Lambert lambert = new Lambert(GRID_PARAM_TYPE.RDR_KMA_WD);
	
	private int nz = 56;
	private int ny = 960;
	private int nx = 960;
	private int data_out = -30000;
	private int data_scale = 100;
	
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
	
	public void fetchRadarFile(String radarFilePath, float lat, float lon) {
		
		PointF coord = this.lambert.wgs84ToLambert(lat, lon);
		
		try {
	
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA WD Radar Layer Raw Data :::::");
			
			NetcdfDataset ncFile = NetcdfDataset.acquireDataset(radarFilePath, null);
        	
			Variable uVar = ncFile.findVariable("u_component");
			Variable vVar = ncFile.findVariable("v_component");
			
			for(int i=0 ; i<this.nz ; i++) {	
				
				if(true) {
					
					float[][] radarLayerXWindRawData = this.readRadarLayerWindRawData(uVar, i);
					float[][] radarLayerYWindRawData = this.readRadarLayerWindRawData(vVar, i);
					
					float u = radarLayerXWindRawData[(int)coord.getY()][(int)coord.getX()];
					float v = radarLayerYWindRawData[(int)coord.getY()][(int)coord.getX()];
					
					u /= this.data_scale;
					v /= this.data_scale;
					
					System.out.println("Layer: " + i);
					System.out.println("u_component: " + u);
					System.out.println("v_component: " + v);
				}
			}
			
			ncFile.close();		
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		
		FetchRadarKmaWdTest radarTest = new FetchRadarKmaWdTest();
		
		radarTest.fetchRadarFile("F:/data/datastore/RDR/2020/05/13/RDR_R3D_KMA_WD_202005131550.nc", 37.46282f, 126.43881f);
	}
}
