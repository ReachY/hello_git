# !/usr/bin/env python
# -*- coding=utf-8 -*-
#
import pymongo
import os
import shutil
import minio


"""
说明：
    pip3 install pyopenssl==19.0.0 minio==4.0.0 pymongo==3.9.0
    ufw allow 27017 9000
    需要把docker 容器端口暴露出来
"""

USERNAME = "mongo"
PASSWORD = "mongostorage"
DB_NAME = "dncloud"
ENDPOINT = "192.168.253.56:27017"
OLD_DIR = "/var/lib/docker/volumes/minio-storage/_data/dnstore"
data_dicom = "dicom"
data_image = "image"
data_algorithm = "algorithm"
MINO_ENDPOINT = "192.168.253.56:9000"
AK = "minio"
SK = "miniostorage"
BUCKET_NAME = "dnstore"
# 统计完成数量
apple = {"count": 0, "total": 0, "dicom": 0, "image": 0, "algorithm": 0, "shares": 0}


class MvData(object):
    def __init__(self, ):
        """移动数据"""
        self.endpoint = "mongodb://" + ENDPOINT
        kwargs = {
            "connect": True,
            "minPoolSize": 30,
            "maxPoolSize": 60
        }
        self.db_name = DB_NAME
        print(self.endpoint, kwargs)
        self.connection = pymongo.MongoClient(self.endpoint, **kwargs)
        self.connection.admin.authenticate(USERNAME, PASSWORD)

    def get_db(self):
        """datebase"""
        return self.connection[self.db_name]


def db_instance():
    return MvData().get_db()


def datebase():
    DB = db_instance()
    data = DB.store.data
    runtimes = DB.detection.runtimes
    lesions = DB.detection.lesions
    shares = DB.store.shares
    return data, runtimes, lesions, shares


def find(table):
    contents = table.find({}, {"_id": False}, no_cursor_timeout =False)
    return [x for x in contents]


def find_one(table, filename):
    return table.find_one({"filename": filename}, {"_id": False})


def mv_dicom(old_path, new_path):
    """移动dicom文件到指定路径"""
    if os.path.exists(old_path):
        shutil.copy(old_path, new_path)


def mv_dicom_files():
    data, runtimes, lesions, shares = datebase()
    print("---------------------mv files-------------------------")
    print("start mv dicom files ...")
    data_info = find(data)
    apple["dicom"] = len(data_info)
    print("dicom files about {}".format(apple["dicom"]))
    for index, dicom in enumerate(data_info):
        if dicom.get("object_name", ""):
            old_path = os.path.join(OLD_DIR, dicom.get("object_name"))
            object_path = os.path.join(data_dicom, dicom.get("object_name"))
            mv_dicom(old_path, object_path)
        print("success migrate dicom files count: {} , %{} complete".format(index, (index/apple["dicom"]*100)))


def mv_algo_files():
    data, runtimes, lesions, shares = datebase()
    print("---------------------mv files-------------------------")
    print("start mv algorithm files ...")
    runtimes_info = find(runtimes)
    apple["algorithm"] = len(runtimes_info)
    print("algorithm files about {}".format(apple["algorithm"]))
    for index, runtime in enumerate(runtimes_info):
        if runtime.get("output", {}):
            for key, value in runtime["output"].items():
                object = find_one(shares, value)
                if not object:
                    continue
                object_name = object.get("object_name")
                old_path = os.path.join(OLD_DIR, object_name)
                object_path = os.path.join(data_algorithm, object_name)
                mv_dicom(old_path, object_path)
        print("success migrate algorithm files count: {} , %{} complete".format(index, (index / apple["algorithm"] * 100)))


def mv_images_files():
    data, runtimes, lesions, shares = datebase()
    print("---------------------mv files-------------------------")
    print("start mv image files ...")
    lesions_info = find(lesions)
    apple["image"] = len(lesions_info)
    print("image files about {}".format(apple["image"]))
    for index, lesion in enumerate(lesions_info):
        if lesion.get("location", {}):
            for key, value in lesion["location"].items():
                if not value:
                    continue
                object = find_one(shares, value)
                if not object:
                    continue
                object_name = object.get("object_name")
                old_path = os.path.join(OLD_DIR, object_name)
                object_path = os.path.join(data_image, object_name)
                mv_dicom(old_path, object_path)
        print("success migrate image files count: {} , %{} complete".format(index, (index / apple["image"] * 100)))


def mv_shares_files():
    data, runtimes, lesions, shares = datebase()
    print("---------------------mv files-------------------------")
    print("start mv shares files ...")
    shares_info = find(shares)
    apple["shares"] = len(shares_info)
    print("shares files about {}".format(apple["shares"]))
    shres_count = 0
    for index, share in enumerate(shares_info):
        object_name = share.get("object_name")
        if not object_name:
            continue
        old_path = os.path.join(OLD_DIR, object_name)
        if not os.path.exists(old_path):
            continue
        shres_count += 1
        object_path = os.path.join(data_image, object_name)
        mv_dicom(old_path, object_path)
        print("success migrate shares files to images, count: {} , %{} complete".format(index, (index / apple["shares"] * 100)))

    print(shres_count, "shares")
    print("actually mv shares files {}".format(shres_count))
    return True


if __name__ == '__main__':

    mv_dicom_files()
    mv_algo_files()
    mv_images_files()
    mv_shares_files()
