
commodittyDataTablesDB=function(RIC,SDate){
  startDate=SDate
  RICdb=RIC
  rutaBD="/Users/oscardelatorretorres/Dropbox/01 TRABAJO/27basesDatosInvestigacion/02simulacionesMS/01commoditties/"
  rutaBD2="/Users/oscardelatorretorres/Dropbox/01 TRABAJO/27basesDatosInvestigacion/01precios/02futures/01agricultural/"
  rutaBD3="/Users/oscardelatorretorres/Dropbox/01 TRABAJO/27basesDatosInvestigacion/01precios/01equity/"
  connPrecios <- dbConnect(RSQLite::SQLite(), paste0(rutaBD,"wtiSims.db"))
  connComm <- dbConnect(RSQLite::SQLite(), paste0(rutaBD2,"commodityFutures.db"))  
  connIndex <- dbConnect(RSQLite::SQLite(), paste0(rutaBD3,"equityIndexes.db")) 
  
  dailyOutTable="dataSims1D"
  weeklyOutTable="dataSims1W"
  # research Price update====
  
  startSimsTime=Sys.time()
  
  
  # Future prices and returns Refintiv:
  
  RICS=dbGetQuery(connComm,"SELECT * FROM RIC")
  
  datos=dbGetQuery(connComm,
                   paste0("SELECT * FROM priceNative WHERE RIC=='",RICdb,"' AND Date>='",startDate,"'")
  )
  #1994-01-03
  datos=datos[-1,]
  
  datos=data.frame(Date=as.Date(datos$Date,origin="1899-12-30"),
                   Settle=as.numeric(datos$Close),
                   Volume=as.numeric(datos$Volume),
                   OI=as.numeric(datos$OpenInterest))
  datos$Settle=na.locf(datos$Settle)
  
  datos$Return=c(0,log(datos$Settle[2:nrow(datos)])-
                   log(datos$Settle[1:(nrow(datos)-1)]))
  naRows=which(is.na(datos$Return))
  
  if (length(naRows)>0){
    datos$Return[naRows]=0
  }
  
  naRows=which(is.na(datos$Volume))
  if (length(naRows)>0){
    datos$Volume[naRows]=0
  }
  
  # DXY data:
  
  datosIndex=dbGetQuery(connIndex,
                        paste0( "SELECT * FROM indexNative WHERE RIC=='.DXY' AND Date>='",startDate,"'")
  )
  DXY=data.frame(Date=as.Date(datosIndex$Date,origin="1899-12-30"),
                 DXY=as.numeric(datosIndex$Close))
  
  datos$DXY=NA
  
  for (a in 1:nrow(datos)){
    dateRowId=which(DXY$Date==datos$Date[a])
    if (length(dateRowId>0)){
      datos$DXY[a]=DXY$DXY[dateRowId]
    }
  }
  datos$DXY=na.locf(datos$DXY)
  datos$DXY[2:nrow(datos)]=log(datos$DXY[2:nrow(datos)])-log(datos$DXY[1:(nrow(datos)-1)])
  datos$DXY[1]=0
  
  # commodity index data:
  
  commIndex=dbGetQuery(connIndex,
                       paste0("SELECT * FROM indexNative WHERE RIC=='.TRCCRBTR' AND Date>='",startDate,"'")
  )
  
  commIndex=data.frame(Date=as.Date(commIndex$Date),
                       commIndex=as.numeric(commIndex$Close))
  
  datos$commIndex=NA
  for (a in 1:nrow(datos)){
    dateRowId=which(commIndex$Date==datos$Date[a])
    if (length(dateRowId>0)){
      datos$commIndex[a]=commIndex$commIndex[dateRowId]
    }
    
  }
  
  datos$commIndex=na.locf(datos$commIndex)
  
  datos$commIndex[2:nrow(datos)]=log(datos$commIndex[2:nrow(datos)])-log(datos$commIndex[1:(nrow(datos)-1)])
  datos$commIndex[1]=0
  
  # OVX (oil VIX) data:
  
  datosIndex=dbGetQuery(connIndex,
                        paste0( "SELECT * FROM indexNative WHERE RIC=='.OVX' AND Date>='",startDate,"'")
  )
  OVX=data.frame(Date=as.Date(datosIndex$Date,origin="1899-12-30"),
                 OVX=as.numeric(datosIndex$Close))
  
  datos$OVX=NA
  
  for (a in 1:nrow(datos)){
    dateRowId=which(OVX$Date==datos$Date[a])
    if (length(dateRowId>0)){
      datos$OVX[a]=OVX$OVX[dateRowId]
    }
  }
  
  # Datos NA detection (no previous data in TS):
  naRowId=which(datos$Date<="2007-05-10")
  maxRowId=max(naRowId)
  datos$OVX[naRowId]=datos$OVX[maxRowId+1]
  
  datos$OVX=na.locf(datos$OVX)
  
  datos$OVX[2:nrow(datos)]=log(datos$OVX[2:nrow(datos)])-log(datos$OVX[1:(nrow(datos)-1)])
  datos$OVX[1]=0
  
  # Especulation ratio according to Kim (2005):
  
  datos$especRatio=datos$Volume/datos$OI
  
  infColsEspecRatio=which(is.infinite(datos$especRatio))
  
  if (length(infColsEspecRatio)>0){
    datos$especRatio[infColsEspecRatio]=datos$especRatio[infColsEspecRatio-1]
  }
  
  datos$especRatio=na.locf(datos$especRatio)
  # Enefermedades infecciosas:
  
  
  INFECTDISEMVTRACK=fredr(
    series_id = "INFECTDISEMVTRACKD",
    observation_start = as.Date(startDate)
  )
  
  INFECTDISEMVTRACK=data.frame(Fecha=INFECTDISEMVTRACK$date,Value=INFECTDISEMVTRACK$value)
  
  
  datos$INFECTDISEMVTRACK=as.numeric(matrix(NA,nrow(datos),1))
  
  for (a in 1:nrow(datos)){
    
    idFecha=max(which(INFECTDISEMVTRACK$Fecha<=datos$Date[a]))
    datos$INFECTDISEMVTRACK[a]=INFECTDISEMVTRACK$Value[idFecha]
    
  }
  
  datos$INFECTDISEMVTRACK=na.locf(datos$INFECTDISEMVTRACK)
  
  # Equity market-related Economic uncertainty index:
  
  MARKEVVOLUNCERTAINTY=fredr(
    series_id = "WLEMUINDXD",
    observation_start = as.Date(startDate)
  )
  
  MARKEVVOLUNCERTAINTY=data.frame(Fecha=MARKEVVOLUNCERTAINTY$date,Value=MARKEVVOLUNCERTAINTY$value)
  
  
  datos$MARKEVVOLUNCERTAINTY=as.numeric(matrix(NA,nrow(datos),1))
  
  for (a in 1:nrow(datos)){
    
    idFecha=max(which(MARKEVVOLUNCERTAINTY$Fecha<=datos$Date[a]))
    datos$MARKEVVOLUNCERTAINTY[a]=MARKEVVOLUNCERTAINTY$Value[idFecha]
    
  }
  
  datos$MARKEVVOLUNCERTAINTY=na.locf(datos$MARKEVVOLUNCERTAINTY)
  
  # US Economic policy unvertainty index:
  
  USECPOLICYUNCERTAINTY=fredr(
    series_id = "USEPUINDXD",
    observation_start = as.Date(startDate)
  )
  
  USECPOLICYUNCERTAINTY=data.frame(Fecha=USECPOLICYUNCERTAINTY$date,Value=USECPOLICYUNCERTAINTY$value)
  
  
  datos$USECPOLICYUNCERTAINTY=as.numeric(matrix(NA,nrow(datos),1))
  
  for (a in 1:nrow(datos)){
    
    idFecha=max(which(USECPOLICYUNCERTAINTY$Fecha<=datos$Date[a]))
    datos$USECPOLICYUNCERTAINTY[a]=USECPOLICYUNCERTAINTY$Value[idFecha]
    
  }
  
  datos$USECPOLICYUNCERTAINTY=na.locf(datos$USECPOLICYUNCERTAINTY)
  
  
  
  # VIX data:
  
  VIX=fredr(
    series_id = "VIXCLS",
    observation_start = as.Date(startDate)
  )
  
  VIX=data.frame(Fecha=VIX$date,Value=VIX$value)
  
  
  datos$VIX=as.numeric(matrix(NA,nrow(datos),1))
  
  for (a in 1:nrow(datos)){
    
    idFecha=max(which(VIX$Fecha<=datos$Date[a]))
    datos$VIX[a]=VIX$Value[idFecha]
    
  }
  
  datos$VIX=na.locf(datos$VIX)
  
  
  # 3-month t-bill data:
  
  TBILL=fredr(
    series_id = "DTB3",
    observation_start = as.Date(startDate)
  )
  
  TBILL=data.frame(Fecha=TBILL$date,Value=TBILL$value)
  
  
  datos$TBILL=as.numeric(matrix(NA,nrow(datos),1))
  
  for (a in 1:nrow(datos)){
    
    idFecha=max(which(TBILL$Fecha<=datos$Date[a]))
    datos$TBILL[a]=TBILL$Value[idFecha]
    
  }
  
  datos$TBILL=na.locf(datos$TBILL)
  
  # calculates de risk premia for the futures and some indexes:
  
  datos$Return=datos$Return-(datos$TBILL/100)
  datos$DXY=datos$DXY-(datos$TBILL/100)
  datos$commIndex=datos$commIndex-(datos$TBILL/100)
  
  # Daily data table writting====
  
  # Writes the prices in the data base:
  datosDB=datos
  datosDB$Date=as.character(datosDB$Date)
  datosDB$Ticker=RICdb
  
  #datosDBW=to.weekly(datos)
  
  dbWriteTable(connPrecios,dailyOutTable,datosDB, overwrite=TRUE)
  
  # Updates the last date in the data base:
  dbExecute(connPrecios, "update futuresData set lastDateD=? where Id=1",
            as.character(tail(datosDB$Date,1))) 
  
  # Weekly data table writting====  
  # Transform to weekly:
  
  datosW=tabla.semanal(datos)
  
  # Corrige si la fecha de la semana actual no es lunes
  difDaysLastWeek=as.numeric(as.Date(datosW$Date[nrow(datosW)])-as.Date(datosW$Date[nrow(datosW)-1]))
  
  if(difDaysLastWeek<7){
    datosW=datosW[-nrow(datosW),]
  }
  
  datosDBW=datosW
  datosDBW$Date=as.character(datosDBW$Date)
  #s
  dbWriteTable(connPrecios,weeklyOutTable,datosDBW, overwrite=TRUE)
  
  # Updates the last date in the data base:
  dbExecute(connPrecios, "update futuresData set lastDateW=? where Id=1",
            as.character(tail(datosDBW$Date,1))) 
  
  # Writes the excel output tables:
  # Daily data
  write.xlsx(datosDB,paste0(rutaBD,"excelOutputs/wtiDailyData1.xls"))
  #Weekly data:
  write.xlsx(datosDBW,paste0(rutaBD,"excelOutputs/wtiWeeklyData1.xls"))
  
  # DB datatableRecord====
  
  endSimsTime=Sys.time()
  
  endSimsTime-startSimsTime
  
  dbDisconnect(connPrecios)
  dbDisconnect(connComm)
  dbDisconnect(connIndex)
  
  print(paste0("Daily and weekly data updates in "))
  #Function ends here:  
}