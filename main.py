import Spark

df=Spark.Spark(path="./archive(2)/Founders and Board Members.csv",format="csv",header=True,sep=",")
df.select("Name","Companies")
df.deleteNull("Companies")
df.drop_duplicate("Companies")
df.writeTableDBPostgres(urlDB="jdbc:postgresql://localhost:5432/db",nametable="testePYY",user="postgres",password="******")
df.getDataFrame().show()
