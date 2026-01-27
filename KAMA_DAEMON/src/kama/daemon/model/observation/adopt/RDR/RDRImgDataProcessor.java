package kama.daemon.model.observation.adopt.RDR;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DataFileStore;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.common.util.Log;
import kama.daemon.model.observation.adopt.RDR.proc.RDR3dLayer;
import kama.daemon.model.observation.adopt.RDR.proc.RDR3dReader;

import java.io.File;
import java.io.IOException;

/**
 * @author chlee
 * Created on 2016-12-19.
 */
public abstract class RDRImgDataProcessor extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "rdr";
    private static final int DB_COLUMN_COUNT = 71;
    private static final int FILE_DATE_INDEX_POS = 3; // RDR_CNQCZ_3D80_201609010020.bin.gz
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2 }; // TM, STN_ID, RWY_DIR
    private final int INSERT_QUERY = 1;
    private final int TRUNCATE_QUERY = 2;
    private final int DELETE_QUERY = 3;

    public RDRImgDataProcessor(DaemonSettings settings, String prefix)
    {
        super(settings, prefix);
    }

    protected void processRDRImage(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        try
        {
            RDR3dReader reader;
            String sFileSavePath;
            File outputFile;
            String outputFileName;

            // png 파일 저장 경로를 독립적으로 사용 (e.g., %BASE_PATH%/RDR_IMG/CNQCZ/2017/01/01/~~~~.png)
            sFileSavePath = String.format("%s/RDR_IMG/%s/%s", dbManager.Settings.OutputRootPath(), processorInfo.ClassPrefix, DateFormatter.formatDate(processorInfo.FileDateFromNameOriginal, "yyyy/MM/dd"));

            reader = new RDR3dReader(file.getAbsolutePath());
            reader.regridAndSmoothAllLayers();

            String[] routeIDs = { "G597", "G585", "A582", "A593", "Y722", "Y711", "RKPKtoZMUB", "RKPKtoVVDN", "RKPKtoRJAA", "RKPKtoZLXY", "RKPKtoZYYJ", "RKSItoRJAA", "RKSItoZABB", "RKSItoVHHH" };

            for (int i = 0; i < reader.layerCount(); i++)
            {
                outputFileName = String.format("%s/%s_%03d.png", dbManager.Settings.FTPLocalPath(), file.getName().replaceFirst("[.][^.]+$", ""), i + 1);
                outputFile = new File(outputFileName);

                try
                {
                    RDR3dLayer layer;
                    layer = reader.getLayer(i);
                    layer.saveAs(outputFile);
                    DataFileStore.storeDateFile(outputFile, sFileSavePath);

//                    // 항로에 따른 연직 단면도 같이 처리
//                    for (int j = 0; j < routeIDs.length; j++)
//                    {
//                        File outputFile2 = new File(String.format("%s/%s_%03d_%s.png", dbManager.Settings.FTPLocalPath(), file.getName().replaceFirst("[.][^.]+$", ""), i + 1, routeIDs[j]));
//
//                        RDR3dLayer verticalSlice = reader.getVerticalSlice(routeIDs[j], 500);
//                        verticalSlice.saveAs(outputFile2);
//                        DataFileStore.storeDateFile(outputFile2, sFileSavePath);
//                    }
                }
                catch (Exception ex)
                {
                    Log.print(String.format("Unable to create image file: %s/%s", processorInfo.FileSavePath, outputFile));
                    throw ex;
                }
            }
        }
        catch (IOException ex)
        {
            // rethrow exception
            throw ex;
        }
    }
}