package com.test;

import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.HObject;
import ncsa.hdf.object.h5.H5File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PrepSongsMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        Path filePath = fileSplit.getPath();

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path(filePath.toString());

        // Check if the file exists and is readable in HDFS
        if (!fs.exists(hdfsPath)) {
            System.err.println("File does not exist: " + filePath);
            return;
        }

        context.write(new Text("1"), new Text(filePath.toString()));

        String path = "proj/SongDataset/songs/A/A/A/TRAAAMQ128F1460CD3.h5";

        int fileId = -1;
        int datasetId = -1;
        int datatypeId = -1;
        int numMembers = -1;
        int dataspaceId = -1;
        int memberDatatypeId = -1;

        FSDataInputStream inputStream = null;

        try {
            inputStream = fs.open(hdfsPath);
            Path localTempPath = new Path("/tmp/temp.h5");
            fs.copyToLocalFile(hdfsPath, localTempPath);

            context.write(new Text("2"), new Text(filePath.toString()));
            fileId = H5.H5Fopen(localTempPath.toString(), HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);
            context.write(new Text("3"), new Text(filePath.toString()));
            datasetId = H5.H5Dopen(fileId, "/metadata/songs");
            context.write(new Text("4"), new Text(filePath.toString()));
            datatypeId = H5.H5Dget_type(datasetId);
            numMembers = H5.H5Tget_nmembers(datatypeId);

            String fieldName = "song_id";
            context.write(new Text("5"), new Text(filePath.toString()));

            // 查找字段的索引
            int fieldIndex = -1;
            for (int i = 0; i < numMembers; i++) {
                String memberName = H5.H5Tget_member_name(datatypeId, i);
                if (fieldName.equals(memberName)) {
                    fieldIndex = i;
                    break;
                }
            }
            context.write(new Text("6"), new Text(filePath.toString()));

            // 获取数据集的数据空间
            dataspaceId = H5.H5Dget_space(datasetId);

            // 获取数据集的维度
            long[] dims = new long[1];
            H5.H5Sget_simple_extent_dims(dataspaceId, dims, null);
            int dataSize = (int) dims[0];
            context.write(new Text("7"), new Text(filePath.toString()));

            // 获取字段的数据类型和偏移量
            memberDatatypeId = H5.H5Tget_member_type(datatypeId, fieldIndex);
            long memberOffset = H5.H5Tget_member_offset(datatypeId, fieldIndex);

            // 创建缓冲区来存储数据
            byte[] buffer = new byte[H5.H5Tget_size(datatypeId) * dataSize];

            // 读取数据集中的数据
            H5.H5Dread(datasetId, datatypeId, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, buffer);

            context.write(new Text("8"), new Text(filePath.toString()));
            // 读取特定字段的值
            for (int i = 0; i < dataSize; i++) {
                int offset = (int) (i * H5.H5Tget_size(datatypeId) + memberOffset);
                String fieldValue = new String(buffer, offset, H5.H5Tget_size(memberDatatypeId)).trim();
                context.write(new Text(filePath.toString()), new Text(fieldValue));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }

            if (fileId >= 0) {
                try {
                    H5.H5Tclose(memberDatatypeId);
                    H5.H5Sclose(dataspaceId);
                    H5.H5Tclose(datatypeId);
                    H5.H5Dclose(datasetId);
                    H5.H5Fclose(fileId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            fs.delete(new Path("/tmp/temp.h5"), false);
        }
    }
}