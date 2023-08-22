from pyspark.sql import SparkSession
from pyspark.sql.functions import *



class Spark:
    __previousVersion=""
    __DFMaster=""
    __spark = SparkSession.builder.appName("ETL").config("spark.driver.extraClassPath","./drive/postgresql-42.6.0.jar").getOrCreate()

    def __init__(self,path,format,header,sep):

        df=self.__spark.read.load(path,format=format,inferSchema=True,header=header,sep=sep)
        self.setDataFrame(df)

    def getpreviousVersion(self):
        return self.__previousVersion

    def getDataFrame(self):
        return self.__DFMaster

    def setDataFrame(self,valor):
        self.__previousVersion=self.__DFMaster
        self.__DFMaster=valor


    def deleteNull(cls,column):

        df= cls.__DFMaster.select("*").where(col(column).isNotNull())
        cls.setDataFrame(df)


    def drop_duplicate(cls,column):
        df=cls.__DFMaster.dropDuplicates([column])
        cls.setDataFrame(df)


    def select(cls,*args):
        df=cls.__DFMaster.select([c for c in args])
        cls.setDataFrame(df)

    def writeTableDBPostgres(self,urlDB,nametable,user,password):
        (self.__DFMaster.write.mode("overwrite").format("jdbc")
         .option("url",urlDB)
         .option("dbtable",nametable)
         .option("user",user)
         .option("password",password)
         .save())
