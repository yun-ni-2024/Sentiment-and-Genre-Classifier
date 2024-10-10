package com.PrepSongs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.DecimalFormat;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

public class PrepSongsMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String filePath = value.toString();
        String[] parts = filePath.split("/");

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path(filePath);

        // Check if the file exists and is readable in HDFS
        if (!fs.exists(hdfsPath)) {
            System.err.println("File does not exist: " + filePath);
            return;
        }

        int fileId = -1;

        FSDataInputStream inputStream = null;

        try {
            inputStream = fs.open(hdfsPath);
            Path localTempPath = new Path("/tmp/" + parts[parts.length - 1]);
            fs.copyToLocalFile(hdfsPath, localTempPath);

            fileId = H5.H5Fopen(localTempPath.toString(), HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);

            String song_id = readField(fileId, "/metadata/songs", "song_id");
            String track_id = readField(fileId, "/analysis/songs", "track_id");
            String title = readField(fileId, "/metadata/songs", "title");
            String release = readField(fileId, "/metadata/songs", "release");
            String artist_id = readField(fileId, "/metadata/songs", "artist_id");
            String artist_name = readField(fileId, "/metadata/songs", "artist_name");
            String mode = readField(fileId, "/analysis/songs", "mode");
            String energy = readField(fileId, "/analysis/songs", "energy");
            String tempo = readField(fileId, "/analysis/songs", "tempo");
            String loudness = readField(fileId, "/analysis/songs", "loudness");
            String duration = readField(fileId, "/analysis/songs", "duration");
            String danceability = readField(fileId, "/analysis/songs", "danceability");
            String year = readField(fileId, "/musicbrainz/songs", "year");

            // Format the output value
            StringBuilder outputValue = new StringBuilder();
            outputValue.append(song_id).append(",")
                    .append(track_id).append(",")
                    .append(title).append(",")
                    .append(release).append(",")
                    .append(artist_id).append(",")
                    .append(artist_name).append(",")
                    .append(mode).append(",")
                    .append(energy).append(",")
                    .append(tempo).append(",")
                    .append(loudness).append(",")
                    .append(duration).append(",")
                    .append(danceability).append(",")
                    .append(year);

            context.write(new Text(song_id), new Text(outputValue.toString()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }

            if (fileId >= 0) {
                try {
                    H5.H5Fclose(fileId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            fs.delete(new Path("/tmp/" + parts[parts.length - 1]), false);
        }
    }

    private String readField(int fileId, String datasetPath, String fieldName) {
        int datasetId = -1;
        int datatypeId = -1;
        int numMembers = -1;
        int dataspaceId = -1;
        int memberDatatypeId = -1;

        try {
            datasetId = H5.H5Dopen(fileId, datasetPath);
            datatypeId = H5.H5Dget_type(datasetId);
            numMembers = H5.H5Tget_nmembers(datatypeId);

            // 查找字段的索引
            int fieldIndex = -1;
            for (int i = 0; i < numMembers; i++) {
                String memberName = H5.H5Tget_member_name(datatypeId, i);
                if (fieldName.equals(memberName)) {
                    fieldIndex = i;
                    break;
                }
            }

            // 获取数据集的数据空间
            dataspaceId = H5.H5Dget_space(datasetId);

            // 获取数据集的维度
            long[] dims = new long[1];
            H5.H5Sget_simple_extent_dims(dataspaceId, dims, null);
            int dataSize = (int) dims[0];

            // 获取字段的数据类型和偏移量
            memberDatatypeId = H5.H5Tget_member_type(datatypeId, fieldIndex);
            long memberOffset = H5.H5Tget_member_offset(datatypeId, fieldIndex);

            // 创建缓冲区来存储数据
            byte[] buffer = new byte[H5.H5Tget_size(datatypeId) * dataSize];

            // 读取数据集中的数据
            H5.H5Dread(datasetId, datatypeId, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, buffer);

            // 读取特定字段的值
            for (int i = 0; i < dataSize; i++) {
                int offset = (int) (i * H5.H5Tget_size(datatypeId) + memberOffset);
                String fieldValue = null;

                int fieldType = H5.H5Tget_class(memberDatatypeId);
                if (fieldType == HDF5Constants.H5T_INTEGER) {
                    fieldValue = Integer.toString(readInt(buffer, offset, H5.H5Tget_size(memberDatatypeId)));
                } else if (fieldType == HDF5Constants.H5T_FLOAT) {
                    fieldValue = new DecimalFormat("0.######").format(readDouble(buffer, offset, H5.H5Tget_size(memberDatatypeId)));
//                    fieldValue = String.format("%.3f", readDouble(buffer, offset, H5.H5Tget_size(memberDatatypeId)));
                } else if (fieldType == HDF5Constants.H5T_STRING) {
                    fieldValue = new String(buffer, offset, H5.H5Tget_size(memberDatatypeId)).trim();
                } else {
                    fieldValue = "Unknown type";
                }

                return fieldValue;
            }
        } catch (HDF5LibraryException e) {
            e.printStackTrace();
        } finally {
            // 关闭所有打开的HDF5资源
            try {
                if (memberDatatypeId >= 0) H5.H5Tclose(memberDatatypeId);
                if (dataspaceId >= 0) H5.H5Sclose(dataspaceId);
                if (datatypeId >= 0) H5.H5Tclose(datatypeId);
                if (datasetId >= 0) H5.H5Dclose(datasetId);
            } catch (HDF5LibraryException e) {
                e.printStackTrace();
            }
        }

        return "Unknown field";
    }

    private int readInt(byte[] buffer, int offset, int size) {
        int value = 0;
        for (int i = 0; i < size; i++) {
            value |= (buffer[offset + i] & 0xFF) << (8 * i);
        }
        return value;
    }

    private static double readDouble(byte[] buffer, int offset, int size) {
        // 确保字节数组长度为8
        if (size != 8) {
            throw new IllegalArgumentException("Size must be 8 bytes for a double");
        }

        // 将字节数组转换为long，假设为小端序
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, size).order(ByteOrder.LITTLE_ENDIAN);
        long longBits = byteBuffer.getLong();
        return Double.longBitsToDouble(longBits);
    }
}