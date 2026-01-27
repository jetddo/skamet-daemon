package kama.daemon.model.prediction.proc;

import java.awt.Color;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DataFileStore;
import kama.daemon.common.util.Log;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.ModelGridUtil;

import org.apache.commons.lang3.NotImplementedException;

import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

/**
 * Created by chlee on 2017-02-15.
 */
public class ICING_EA_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "icing_ea";
    private static final int DB_COLUMN_COUNT = 2;
    private static final int FILE_DATE_INDEX_POS = 2; // 20170101/visibility.nc
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // COL
    private final int INSERT_QUERY_1 = 1;
    private final int INSERT_QUERY_2 = 2;
    private final int DELETE_QUERY = 3;
    
	private FloatBuffer latitudeBuffer;
	private FloatBuffer longitudeBuffer;
	
	private ModelGridUtil modelGridUtil;
	
    public ICING_EA_DataProcess(DaemonSettings settings) throws IOException {
        super(settings, DATAFILE_PREFIX);
        
        try {
			initCoordinates(settings);
		} catch (final IOException e) {
            Log.print(String.format("Can not read ICING_EA coordinates."));
            throw e;
		}
    }

	private void initCoordinates(final DaemonSettings settings) throws IOException {
		
		modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.ICING_EA, settings.getCoordinatesLatPath(), settings.getCoordinatesLonPath());
		
		final RandomAccessFile latitudeFile = new RandomAccessFile(settings.getCoordinatesLatPath(), "r");
		final FileChannel latitudeFileChannel = latitudeFile.getChannel();

		final ByteBuffer latitudeByteBuffer = ByteBuffer.allocateDirect((int) latitudeFile.length()).order(ByteOrder.BIG_ENDIAN);
		latitudeFileChannel.read(latitudeByteBuffer);
		latitudeByteBuffer.clear();

		this.latitudeBuffer = latitudeByteBuffer.asFloatBuffer();

		latitudeFileChannel.close();
		latitudeFile.close();

		final RandomAccessFile longitudeFile = new RandomAccessFile(settings.getCoordinatesLonPath(), "r");
		final FileChannel longitudeFileChannel = longitudeFile.getChannel();

		final ByteBuffer longitudeByteBuffer = ByteBuffer.allocateDirect((int) longitudeFile.length()).order(ByteOrder.BIG_ENDIAN);
		longitudeFileChannel.read(longitudeByteBuffer);
		longitudeByteBuffer.clear();

		this.longitudeBuffer = longitudeByteBuffer.asFloatBuffer();

		longitudeFileChannel.close();
		longitudeFile.close();
	}
    
    private void makeRegridDataFile(File modelFile, ProcessorInfo processorInfo) {
    	
        File saveFolder = new File(processorInfo.FileSavePath);

        if (!saveFolder.exists() || saveFolder.isFile())
        {
            saveFolder.mkdirs();
        }
    	
    	int latSize = 418;
		int lonSize = 490;
		
		float minLat = 12.287582f;
		float maxLat = 61.078056f;
		float minLon = 78.32236f;
		float maxLon = 173.67761f;
		
		float latTerm = (maxLat - minLat) / (latSize-1);
		float lonTerm = (maxLon - minLon) / (lonSize-1);
		
		try {
				
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(processorInfo.FileSavePath + File.separator + modelFile.getName() + ".regrid.bin")));
			
			NetcdfDataset ncFile = NetcdfDataset.acquireDataset(modelFile.getAbsolutePath(), null);
			
			Variable var = ncFile.findVariable("RAP");
			
			for(int i=0 ; i<41 ; i++) {
				
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(i, i));
				rangeList.add(new Range(0, latSize-1));
				rangeList.add(new Range(0, lonSize-1));
				
				float[] rawData = (float[])var.read(rangeList).getStorage();
				
				float[][] regridValues = new float[latSize][];
				boolean[][] regridChecks = new boolean[latSize][];
				
				for(int j=0 ; j<latSize ; j++) {
					
					regridValues[j] = new float[lonSize];
					regridChecks[j] = new boolean[lonSize];
					
					for(int k=0 ; k<lonSize ; k++) {
						regridValues[j][k] = -999f;
						regridChecks[j][k] = false;
					}
				}
				
				for(int j=0 ; j<latSize ; j++) {
					
					for(int k=0 ; k<lonSize ; k++) {
						
						float originLat = this.latitudeBuffer.get(j * lonSize + k);
						float originLon = this.longitudeBuffer.get(j * lonSize + k);
						
						int y = (int)((originLat - minLat) / latTerm);
						int x = (int)((originLon - minLon) / lonTerm);
						
						regridValues[y][x] = rawData[j * lonSize + k];
						regridChecks[y][x] = true;
					}
				}
					
				for(int j=0 ; j<latSize ; j++) {
					
					for(int k=0 ; k<lonSize ; k++) {
						
						if(regridChecks[j][k] == false) {
							
							float regridLat = minLat + latTerm * j;
							float regridLon = minLon + lonTerm * k;
							
							this.modelGridUtil.setSingleGridBoundInfoforDistanceGrid(regridLon, regridLat);
							
							BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
							BoundXY boundXY = this.modelGridUtil.getBoundXY();
							
							float originLat = (float)boundLonLat.getTop();
							float originLon = (float)boundLonLat.getLeft();
							
							if(Math.abs(originLat - regridLat) < latTerm*2 && Math.abs(originLon - regridLon) < lonTerm*2) {
								regridValues[j][k] = rawData[boundXY.getTop() * lonSize + boundXY.getLeft()];
							}
						}
					}
				}
				
				for(int j=0 ; j<latSize ; j++) {					
					for(int k=0 ; k<lonSize ; k++) {
						out.writeFloat(regridValues[j][k]);						
					}
				}
//				
//				BufferedImage bi = new BufferedImage(lonSize, latSize, BufferedImage.TYPE_INT_ARGB);
//				
//				Graphics2D ig2 = bi.createGraphics();
//				
//				for(int j=0 ; j<latSize ; j++) {
//					
//					for(int k=0 ; k<lonSize ; k++) {
//						
//						Color c = this.getColor(regridValues[j][k]);
//						
//						if(c != null) {
//							ig2.setColor(c);
//							ig2.drawLine(k, j, k, j);	
//						}	
//					}
//				}
//				
//				ImageIO.write(bi, "PNG", new File(processorInfo.FileSavePath + File.separator + modelFile.getName() + ".regrid.png"));
				
			}
			
			ncFile.close();
	
			out.close();
		
		} catch (IOException | InvalidRangeException e) {
			e.printStackTrace();
		}
    	
    }
	
	private Color getColor(float value) {
		
		switch((int)value) {
		
		case 1: return new Color(0,255,47); 
		case 2: return new Color(198,255,12);
		case 3: return new Color(251,255,9);
		case 4: return new Color(253,207,2);
		case 5: return new Color(255,131,0);
		case 6: return new Color(255,50,0);
		}
		
		return null;
	}

    @Override@SuppressWarnings("Duplicates")
    protected void processDataByFileGroup(DatabaseManager dbManager, File[] dataFiles, ProcessorInfo processorInfo) throws Exception
    {
        String query = null;
        List<String> queriesList;

        queriesList = new ArrayList<>();

        // 파일 전부 extract
        for (File file : dataFiles)
        {
            // 처리할 파일명 로그 print
        	Log.print("INFO : File NAME -> {0}", file.getAbsoluteFile());
            
            makeRegridDataFile(file, processorInfo);
                 
            if (DataFileStore.storeDateFile(file, processorInfo.FileSavePath))
            {
                query = buildFileInfoQuery(processorInfo, new File(processorInfo.FileSavePath, file.getName()));
                queriesList.add(query);
            }
        }

        for (File file : dataFiles)
        {
            if (file.exists())
            {
                file.delete();
            }
        }

        // 쿼리 한꺼번에 처리
        for (String savedQuery : queriesList)
        {
            dbManager.executeUpdate(savedQuery);
        }

        dbManager.commit();
    }

    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        throw new NotImplementedException("Not implemented");
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
    }
    //</editor-fold>

    //<editor-fold desc="Auto-generated getters (No need to modify)">
    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
    //</editor-fold>
}
