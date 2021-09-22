library(tidycovid19)
library(data.table)
library(readr)
library(lubridate)
library(dplyr)
library(zoo)#rollmean等方法


# d = as.data.table(list(1:6/2, 3:8/4))
# # rollmean of single vector and single window
# cbind(d,c=frollmean(d[, V1], 3))

hifleet_containers=fread("D:/06data/ships_hifleet/hifleetships_brief.csv")
#提取原始数据，该数据以船舶stop的形式与相应的港口和国家进行了关联
filepaths=dir('D:/data/cruiseships/2018_cruiseship_stops_ports_country/',full.names = TRUE)
mmsis=data.table(dir('D:/data/cruiseships/2018_cruiseship_stops_ports_country/'))[,mmsi:=tstrsplit(V1,'.',fixed = TRUE)[1]]$mmsi 
shipstopfiles0=data.table(filepath=filepaths,mmsi=mmsis)
shipstopfiles=left_join(shipstopfiles0,hifleet_containers[,.SD[1,],mmsi][,mmsi:=as.character(mmsi)],by="mmsi")
max_grosston=max(shipstopfiles$gross_tonnage)
shipstopfiles=shipstopfiles[,grosston_factor:=round(gross_tonnage/max_grosston,2)]
shipn=nrow(shipstopfiles)

#下载和处理疫情相关的数据。为便于计算将NA相关的数据假设为0，并将疫情数据按照国家和日期进行排列
#计算同一个国家的疫情增加人数（newcases）前一天数据减去
merged_dta0 <- data.table(download_merged_data(cached = TRUE))
merged_dta0=merged_dta0[,list(country,iso3c,date,confirmed,lockdown,population)]
#百万人口增长率
merged_dta=merged_dta0[order(iso3c,date)][,newadd:=(confirmed-lag(confirmed))/population*10000000][,iso3c1:=lag(iso3c)]
merged_dta=merged_dta[iso3c1!=iso3c,newadd:=0]
merged_dta=merged_dta[newadd<0,newadd:=0]#增加病例少于0，我们认为数据有误，将其调整为0
merged_dta=merged_dta[,newadd1:=lag(newadd)][,iso3c2:=lag(iso3c)]
merged_dta=merged_dta[iso3c!=iso3c2,newadd1:=0]
merged_dta=merged_dta[(newadd+newadd1)>0,newaddfactor:=1+round(growWeight*(newadd-newadd1)/(newadd1+newadd),2)]
merged_dta=merged_dta[(newadd+newadd1)==0,newaddfactor:=1][is.na(newaddfactor),newaddfactor:=1]
merged_dta[is.na(merged_dta)] <- 0 #没有数据的假设为0
setkey(merged_dta,iso3c,date)
growWeight=0.5 #新病例增长率的权重
#计算增量和增速
#重要：有计算出addnew为负数的情况?????????。
#confirmed1 表示前一天累计感染病例。addnew1表示前一天新增病例，addnew表示当前日期病例数
# merged_dta=merged_dta[,list(country,iso3c,date,confirmed,deaths,recovered,lockdown,pop_density,population,timestamp)]
# merged_dta=merged_dta[,confirmed1:=shift(confirmed,n=1)][,iso3c1:=shift(iso3c,n=1)][iso3c1!=iso3c,confirmed1:=0][is.na(iso3c1),confirmed1:=confirmed][is.na(iso3c1),iso3c1:=iso3c][,newadd:=confirmed-confirmed1]
# merged_dta=merged_dta[,newadd1:=shift(newadd,n=1)][,iso3c1:=shift(iso3c,n=1)][iso3c1!=iso3c,newadd1:=0][is.na(iso3c1),newadd1:=newadd][is.na(iso3c1),iso3c1:=iso3c]
# merged_dta=merged_dta[newadd>0,newaddfactor:=1+round(growWeight*(newadd-newadd1)/(abs(newadd1)+abs(newadd)),2)][newadd<0,newaddfactor:=1-round((growWeight*abs(newadd)/(abs(newadd1)+abs(newadd))),2)][newadd==0,newaddfactor:=1]
# merged_dta$iso3c1=NULL；

for(k in seq(1,shipn)){
  if(k%%10==0){print(k)}
  #k=1
  ashipfile=shipstopfiles[k]
  agrosston_rate=ashipfile$grosston_factor
  ammsi=ashipfile$mmsi
  aship_stops=fread(ashipfile$filepath)
  add_days=24*30
  aship_stops=aship_stops[,startday:=as.Date(startday)+days(add_days)][,endday:=as.Date(endday)+days(add_days)]
  
  n=nrow(aship_stops)
  
  if(n>0){
    #设置表头
    astop=aship_stops[1,]
    affect_stops=merged_dta[iso3c==-1][,mmsi:=astop$mmsi][,stopid:=astop$cl][,lon:=astop$lon][,lat:=astop$lat][,starttime:=astop$starttime][,endtime:=astop$endtime][,dist:=astop$dist][,portid:=astop$id][,portname:=astop$portname]
    affect_stops=affect_stops[,grosston_rate:=0][grosston_rate<0]
    #针对每个stop连接数据，再合并
    for(i in 1:n){
      #if(i%%10==0)print(i)#i=3
      astop=aship_stops[i,]
      iso3=astop$iso3;
      days=seq(astop$startday, astop$endday, by="days")
      affected=merged_dta[iso3c==iso3][date%in%days]
      if(nrow(affected)>0){
        affected=affected[,mmsi:=astop$mmsi][,stopid:=astop$cl][,lon:=astop$lon][,lat:=astop$lat][,starttime:=astop$starttime][,endtime:=astop$endtime][,dist:=astop$dist][,portid:=astop$id][,portname:=astop$portname][,grosston_rate:=(1+agrosston_rate)]
        affect_stops=rbind(affect_stops,affected)
      }
    }
    #当个港口每天只能有一个数据
    affect_stops0=affect_stops[,.SD[1],list(mmsi,portid,date)]
    
    res=affect_stops0[,list(addIndex=newadd*newaddfactor*agrosston_rate),
                      list(mmsi,date,stopid,starttime,endtime,portid,portname,iso3c,country,confirmed,newadd,newadd1,newaddfactor,grosston_rate)]
    indexs=data.table(acm_14_addIndex=0)[acm_14_addIndex<0]
    if(nrow(res)>0){
      for(i in 1:nrow(res)){
        #i=5
        endd=res[i]$date
        startd=endd-days(14)
        ds=seq(startd,endd,by='days')
        acm_14_addIndex=sum(res[date%in%ds][,.SD[1],date]$addIndex)#如果一条船同一天经过停靠两个港口，那么只取停靠的第一个港口的数据。
        indexs=rbind(indexs,data.table(acm_14_addIndex))
      }
      res1=cbind(res,indexs)
      #增加船舶相关信息
      res1=res1[,deadweight:=ashipfile$deadweight][,teu:=ashipfile$teu][,grosston:=ashipfile$gross_tonnage]
      
      #针对单船标准化
      min_index=min(res1$acm_14_addIndex)
      max_index=max(res1$acm_14_addIndex)
      res1=res1[,ship_sd_14_index:=round((acm_14_addIndex-min_index)/(max_index-min_index)*100)]
      fwrite(res1,paste('D:/data/cruiseships/shipriskindex/',ammsi,'.csv',sep = ''))
    }
  }
  
}
plot(res1$date,res1$acm_14_addIndex,type = 'l')
plot(res1$date,res1$ship_sd_14_index,type = 'l')


#计算集装箱港口境外疫情输入
#port index statistics

filepaths=dir('D:/data/cruiseships/shipriskindex/',full.names = TRUE)
mmsis=data.table(dir('D:/data/cruiseships/shipriskindex/'))[,mmsi:=tstrsplit(V1,'.',fixed = TRUE)[1]]$mmsi 
shipindex0=data.table(filepath=filepaths,mmsi=mmsis)
shipindex=left_join(shipindex0,hifleet_containers[,.SD[1,],mmsi][,mmsi:=as.character(mmsi)],by="mmsi")
n=length(mmsis)
allshipindex=fread(shipindex[1]$filepath)[mmsi<0]
for (i in seq(1,n)){
  if(i%%10==0){print(i)}
  ashipindex=fread(shipindex[i]$filepath)
  allshipindex=rbind(allshipindex,ashipindex)
}

portIndex=allshipindex[,list(acIndex=sum(acm_14_addIndex)),list(country,iso3c,portid,portname,date)]
portIndex=portIndex[order(portid,date)]
port14Index=portIndex[acIndex<0][,acm_14_Index:=0][,date:=as.Date(date)]
portids=portIndex[,.N,portid]$portid;
for(i in seq(1,length(portids))){
  #i=1
  if(i%%10==0){print(i)}
  aportid=portids[i]
  aportindex=portIndex[portid==aportid][order(date)]
  aportindex=aportindex[,date:=as.Date(date)]
  #添加缺失的日期
  startdate=as.Date('2020-01-01')
  enddate=as.Date('2020-12-31')
  #enddate=Sys.Date()
  dates=seq(startdate,enddate,'days')
  ap=aportindex[1]
  adddates=data.table(date=dates)[,country:=ap$country][,iso3c:=ap$iso3c][,portid:=ap$portid][,portname:=ap$portname][,acIndex:=0]
  adddates=adddates[!(dates%in%aportindex[,.N,date]$date),list(country,iso3c,portid,portname,date,acIndex)]
  aportindex1=rbind(aportindex[,date:=as.Date(date)],adddates)[order(date)]
  aportindex1=aportindex1[,acm_14_Index:=0]
  k=nrow(aportindex1)
  for(j in seq(1,k)){
    #j=93
    endd=as.Date(aportindex1[j]$date)
    startd=as.Date(endd-days(14))
    ds=seq(startd,endd,by='days')
    sumIndex=sum(aportindex1[date%in%ds][,.SD[1],list(date)]$acIndex)
    aportindex1[j,acm_14_Index:=sumIndex]
  }
  port14Index=rbind(port14Index,aportindex1)
}
maxPortIndex=max(port14Index$acm_14_Index)
minPortIndex=min(port14Index$acm_14_Index)
port14Index=port14Index[,sd_14_index:=round(log10(acm_14_Index-minPortIndex+1)/log10(maxPortIndex-minPortIndex)*100)]

fwrite(port14Index,'D:/data/cruiseships/port14index/port14index.csv')

port14Index=fread('D:/data/cruiseships/port14index/port14index.csv')
hist(port14Index[sd_14_index>0]$sd_14_index)
aportindex1=port14Index[portname=='Brisbane']
aportindex1=aportindex1[,rmean_7_sd_index:=rollmean(sd_14_index, 7, na.pad=TRUE, align="right")]
plot(aportindex1$date,log(aportindex1$acm_14_Index/maxPortIndex),type = "l")
plot(aportindex1$date,aportindex1$sd_14_index,type = "l")

