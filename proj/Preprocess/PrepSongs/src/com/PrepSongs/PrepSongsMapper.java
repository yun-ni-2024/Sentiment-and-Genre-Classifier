package com.PrepSongs;

import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.HObject;
import ncsa.hdf.object.h5.H5File;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

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

        int fileId = -1;
        int datasetId = -1;
        int datatypeId = -1;
        int numMembers = -1;
        int dataspaceId = -1;
        int memberDatatypeId = -1;

        try {
            fileId = H5.H5Fopen(filePath.toString(), HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);
            datasetId = H5.H5Dopen(fileId, "/metadata/songs");
            datatypeId = H5.H5Dget_type(datasetId);
            numMembers = H5.H5Tget_nmembers(datatypeId);

            String fieldName = "song_id";

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
                String fieldValue = new String(buffer, offset, H5.H5Tget_size(memberDatatypeId)).trim();
                System.out.println(": " + fieldValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
        }
    }
}