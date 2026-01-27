package kama.daemon.model.prediction.proc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DataFileStore;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.common.util.Log;
import kama.daemon.model.prediction.adopt.KTG.proc.KTGReader;

public final class KTG_DataProcess extends DataProcessor {

	private final int INSERT_QUERY = 1;

	private FloatBuffer latitudeBuffer;
	private FloatBuffer longitudeBuffer;

	public KTG_DataProcess(final DaemonSettings settings) throws IOException {
		super(settings, "ktg");

		// KTG ���浵 ��ǥ�� �ʱ�ȭ�Ѵ�.
		try {
			initCoordinates(settings);
		} catch (final IOException e) {
            Log.print(String.format("Can not read KTG coordinates."));
            throw e;
		}
	}

	private void initCoordinates(final DaemonSettings settings) throws IOException {
		/**
		 * ���� ��ǥ�� ���Ͽ��� �о���δ�.
		 */
		final RandomAccessFile latitudeFile = new RandomAccessFile(settings.getCoordinatesLatPath(), "r");
		final FileChannel latitudeFileChannel = latitudeFile.getChannel();

		final ByteBuffer latitudeByteBuffer = ByteBuffer.allocateDirect((int) latitudeFile.length()).order(ByteOrder.LITTLE_ENDIAN);
		latitudeFileChannel.read(latitudeByteBuffer);
		latitudeByteBuffer.clear();

		this.latitudeBuffer = latitudeByteBuffer.asFloatBuffer();

		latitudeFileChannel.close();
		latitudeFile.close();

		/**
		 * �浵 ��ǥ�� ���Ͽ��� �о���δ�.
		 */
		final RandomAccessFile longitudeFile = new RandomAccessFile(settings.getCoordinatesLonPath(), "r");
		final FileChannel longitudeFileChannel = longitudeFile.getChannel();

		final ByteBuffer longitudeByteBuffer = ByteBuffer.allocateDirect((int) longitudeFile.length()).order(ByteOrder.LITTLE_ENDIAN);
		longitudeFileChannel.read(longitudeByteBuffer);
		longitudeByteBuffer.clear();

		this.longitudeBuffer = longitudeByteBuffer.asFloatBuffer();

		longitudeFileChannel.close();
		longitudeFile.close();
	}

    @Override
	protected void processDataInternal(final DatabaseManager dbManager, final File file, final ProcessorInfo processorInfo) throws Exception {
    	final String outputFileMovePath = String.format("%s/KTG_IMG/%s", dbManager.Settings.OutputRootPath(), DateFormatter.formatDate(processorInfo.FileDateFromNameOriginal, "yyyy/MM/dd/HH"));

		// �ߺ��� ���� �ٽ� ������� �ʵ��� �Ѵ�.
		// tub1 ���� �� : 0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000, 20000, 21000
		// tub2 ���� �� : 19000, 20000, 21000, 22000, 23000, 24000, 25000, 26000, 27000, 28000, 29000, 30000, 31000, 32000, 33000, 34000, 35000, 36000, 37000, 38000, 39000, 40000, 41000
		int levIndex = 0;
		if (file.getName().indexOf("tub1") == -1) {
			levIndex = 3;
		}

		final KTGReader reader = new KTGReader(file.getAbsolutePath());
		reader.regridingAllLayers(this.latitudeBuffer, this.longitudeBuffer);

		for (int index = levIndex; index < reader.layerCount(); ++index) {
			// ������ �̹��� ������ ��θ� �����Ѵ�.
			// �� ��δ� �ӽ÷� ������ ����̸� �̹��� ���� ������ �Ϸ�� �Ŀ� ���� �̹��� ���� ��η� �����Ѵ�.
            final File outputFile = new File(String.format("%s/%s_%03d.png", dbManager.Settings.FTPLocalPath(), file.getName().replaceFirst("[.][^.]+$", ""), index + 1));

            saveLayer2ImageFileAndMove(reader, index, outputFile, outputFileMovePath);
            executeDbInsert(dbManager, file, processorInfo, reader, index, outputFile, outputFileMovePath);
        }
    }

    @Override
    protected void defineQueries() {
		defineQueryFormat(1, "INSERT INTO %%WORKSPACE%%.KTG_IMG_L (DH_SEQ, ATTD, FILE_STR_LOC, FILE_NM, ANNC_DT, FCST_DT) VALUES (''{0}'', ''{1}'', ''{2}'', ''{3}'', TO_DATE(''{4}'', \''YYYY-MM-DD HH24:mi:ss\''), TO_DATE(''{5}'', \''YYYY-MM-DD HH24:mi:ss\''))");
    }

	@Override
	protected int getDateFileIndex() {
		return 2;
	}

	@Override
	protected Date parseDateFromFileName(final File file) throws ParseException {
		return DateFormatter.parseDate(file.getName().split("\\.")[1], "yyyyMMddHH");
	}

	private void saveLayer2ImageFileAndMove(final KTGReader reader, final int layerIndex, final File outputFile, final String outputFileMovePath) throws Exception {
		reader.getLayer(layerIndex).saveAs(outputFile);

		DataFileStore.storeDateFile(outputFile, outputFileMovePath);
	}

	private void executeDbInsert(final DatabaseManager dbManager, final File file, final ProcessorInfo processorInfo, final KTGReader reader, final int layerIndex, final File outputFile, final String outputFileMovePath) throws Exception {
		final String fileName = file.getName().trim();
		if (fileName.equals("") == true) {
			return;
		}

		String[] fileNameSplit = fileName.split("\\.");

		final int altitude = (int) reader.getAltitude(layerIndex);
		final String vldtyHour = fileNameSplit[0].substring(fileNameSplit[0].length() - 2);
		final String anncDateString = fileNameSplit[1] + "0000";
		final String forecastDateString = getForecastDateString(fileNameSplit[1], Integer.parseInt(vldtyHour)) + "0000";
		final String fileWriteLocation = outputFileMovePath.substring(outputFileMovePath.indexOf("KTG_IMG") + 8);

        Object[] bindArray = new Object[6];

		bindArray[0] = convertToDBText(String.format("%s%s%05d", fileNameSplit[1], vldtyHour, altitude));		// ���� ó�� �̷� ��ȣ(��ǥ����(13�ڸ�) + ��ȿ�ð�(2�ڸ�) + ��)
		bindArray[1] = convertToDBText(String.format("%d", altitude));											// ��
		bindArray[2] = convertToDBText(fileWriteLocation);														// ���� ���� ��ġ
		bindArray[3] = convertToDBText(outputFile.getName());													// ���ϸ�
		bindArray[4] = convertToDBText(anncDateString);															// ��ǥ����
		bindArray[5] = convertToDBText(forecastDateString);														// ��������

        dbManager.executeUpdate(MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray));
    }

	private String getForecastDateString(final String anncDate, final int forecastHour) throws Exception {
		final DateFormat df = new SimpleDateFormat("yyyyMMddHH");

		final Calendar cal = Calendar.getInstance();
		cal.setTime(df.parse(anncDate));
		cal.add(Calendar.HOUR, forecastHour);

		return df.format(cal.getTime());
	}

}
