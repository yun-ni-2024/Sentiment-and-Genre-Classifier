package com.fuck;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;

public class fuck {
    public static void main(String[] args) throws Exception {
        int fileId = H5.H5Fopen("1.h5", HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);
        System.out.println("fileId=" + fileId);

        int datasetId = H5.H5Dopen(fileId, "/metadata/songs");
        System.out.println("datasetId=" + datasetId);

        int datatypeId = H5.H5Dget_type(datasetId);
        System.out.println("datatypeId=" + datatypeId);

        int numMembers = H5.H5Tget_nmembers(datatypeId);
        System.out.println("numMembers=" + numMembers);

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
        int dataspaceId = H5.H5Dget_space(datasetId);

        // 获取数据集的维度
        long[] dims = new long[1];
        H5.H5Sget_simple_extent_dims(dataspaceId, dims, null);
        int dataSize = (int) dims[0];

        // 获取字段的数据类型和偏移量
        int memberDatatypeId = H5.H5Tget_member_type(datatypeId, fieldIndex);
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

        // 关闭资源
        H5.H5Tclose(memberDatatypeId);
        H5.H5Sclose(dataspaceId);
        H5.H5Tclose(datatypeId);
        H5.H5Dclose(datasetId);
        H5.H5Fclose(fileId);
    }
}
