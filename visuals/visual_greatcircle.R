library(maps)
library(geosphere)
library(maptools)
library(sp)
setwd("C:/Users/d29905P/Documents/INFORMS2015")
airport_data <- read.csv("C:/Users/d29905P/Documents/INFORMS2015/airports.dat", header=FALSE, stringsAsFactors=FALSE)
network_data <- read.csv("C:/Users/d29905P/Documents/INFORMS2015/results_table_mape18.csv", header=TRUE,  stringsAsFactors=FALSE)
carriers <- c("AS","UA","US","WN")
airportlist <-list()
#get array of markets for each carrier
for(carrier in carriers) {
  airportlist[[carrier]] = as.vector(network_data[network_data$UNIQUE_CARRIER == carrier,]$BI_MARKET)
}

#draw networks for each carrier
for(carrier in carriers) {
  png(filename = sprintf("map%s.png",carrier),
      width = 1024, height = 768, units = "px", type = "cairo")
  if(carrier=='AS') {carrier_col ="#057D31"}
  else if(carrier=='UA') {carrier_col ="blue"}
  else if(carrier=='US') {carrier_col ="#FBB917"}
  else {carrier_col ="red"}
  xlim <- c(-128.738281, -110.601563)
  ylim <- c(30.039321, 52.856229)
  map("state", col="#f2f2f2", fill=TRUE, bg="white", lwd=0.05, xlim=xlim, ylim=ylim, boundary=FALSE)
  
  airports = c('SEA','PDX','SFO','SAN','LAX','LAS','PHX','OAK','ONT','SMF','SJC')
  labcol= "#2B3856" 
  connections = airportlist[[carrier]]
  for(connection in connections) {
    from = as.character(strsplit(connection, "_")[[1]][1])
    to  = as.character(strsplit(connection, "_")[[1]][2])
    p1 = c(airport_data[airport_data$V5 ==from,]$V8,airport_data[airport_data$V5 ==from,]$V7)
    p2 = c(airport_data[airport_data$V5 ==to,]$V8,airport_data[airport_data$V5 ==to,]$V7)
    inter<-gcIntermediate(p1=p1, p2=p2,n=50,addStartEnd=TRUE)
    lines(inter, col=carrier_col,cex=2,lwd=1.5)
  }   
  for(airport in airports) {
    
    lat = as.numeric(airport_data[airport_data$V5 ==as.character(airport),]$V7) 
    lon = as.numeric(airport_data[airport_data$V5 ==as.character(airport),]$V8)
    points(lon,lat,pch=18,cex=1.5, col="#151B54" )
    if (as.character(airport) == "ONT") {text(lon,lat,airport,cex=1,adj=0,pos=3,col=labcol) } 
    else if (as.character(airport) == "LAS") {text(lon,lat,airport,cex=1,adj=0,pos=4,col=labcol) } 
    else if (as.character(airport) == "SJC") {text(lon,lat,airport,cex=1,adj=0,pos=1,col=labcol) } 
    else if (as.character(airport) == "OAK") {text(lon,lat,airport,cex=1,adj=0,pos=4,col=labcol) } 
    else if (as.character(airport) == "SMF") {text(lon,lat,airport,cex=1,adj=0,pos=3,col=labcol) } 
    else  {text(lon,lat,airport,cex=1,adj=0,pos=2,col=labcol)} 
    
  }

  title(carrier)
  dev.off()

}

#visualize competition
png(filename = "competition.png",
    width = 1024, height = 768, units = "px", type = "cairo")

xlim <- c(-128.738281, -110.601563)
ylim <- c(30.039321, 52.856229)
map("state", col="#f2f2f2", fill=TRUE, bg="white", lwd=0.05, xlim=xlim, ylim=ylim, boundary=FALSE)

airports = c('SEA','PDX','SFO','SAN','LAX','LAS','PHX','OAK','ONT','SMF','SJC')
labcol= "black"
market_df = network_data[,c(3,5)]
market_df  = market_df[!duplicated(market_df),]
for(i in 1:nrow(market_df)) {
  row <- market_df[i,]
  connection = as.character(row[1])
  from = as.character(strsplit(connection, "_")[[1]][1])
  to  = as.character(strsplit(connection, "_")[[1]][2])
  p1 = c(airport_data[airport_data$V5 ==from,]$V8,airport_data[airport_data$V5 ==from,]$V7)
  p2 = c(airport_data[airport_data$V5 ==to,]$V8,airport_data[airport_data$V5 ==to,]$V7)
  inter<-gcIntermediate(p1=p1, p2=p2,n=50,addStartEnd=TRUE)
  if (row[2] ==1) {lines(inter, col="red",cex=2,lwd=1,lty=1)}
  else if (row[2] ==2) {lines(inter, col="#057D31",cex=2,lwd=1.5,lty=5)}
  else if (row[2] ==3) {lines(inter, col="blue",cex=2,lwd=2, lty=1)}
}
for(airport in airports) {
  
  lat = as.numeric(airport_data[airport_data$V5 ==as.character(airport),]$V7) 
  lon = as.numeric(airport_data[airport_data$V5 ==as.character(airport),]$V8)
  points(lon,lat,pch=18,cex=1.5, col="#151B54" )
  if (as.character(airport) == "ONT") {text(lon,lat,airport,cex=1,adj=0,pos=3,col=labcol, font=2) } 
  else if (as.character(airport) == "LAS") {text(lon,lat,airport,cex=1,adj=0,pos=4,col=labcol, font=2) } 
  else if (as.character(airport) == "SJC") {text(lon,lat,airport,cex=1,adj=0,pos=1,col=labcol, font=2) } 
  else if (as.character(airport) == "OAK") {text(lon,lat,airport,cex=1,adj=0,pos=4,col=labcol,  font=2) } 
  else if (as.character(airport) == "SMF") {text(lon,lat,airport,cex=1,adj=0,pos=3,col=labcol, font=2) } 
  else  {text(lon,lat,airport,cex=1,adj=0,pos=2,col=labcol, font=2)} 
  
}
legend('right','groups',c("1 player", "2 player", "3 player"),lty=c(1,5,1),lwd=c(1,1.5,2),col=c("red","#057D31","blue"))
dev.off()




#map just by itself
png(filename = "plain_map.png",
    width = 1024, height = 768, units = "px", type = "cairo")
xlim <- c(-128.738281, -110.601563)
ylim <- c(30.039321, 52.856229)
map("state", col="#f2f2f2", fill=TRUE, bg="white", lwd=0.05, xlim=xlim, ylim=ylim, boundary=FALSE)
airports = c('SEA','PDX','SFO','SAN','LAX','LAS','PHX','OAK','ONT','SMF','SJC')
for(airport in airports) {
  
  lat = as.numeric(airport_data[airport_data$V5 ==as.character(airport),]$V7) 
  lon = as.numeric(airport_data[airport_data$V5 ==as.character(airport),]$V8)
  points(lon,lat,pch=18,cex=1.5, col="#151B54" )
  if (as.character(airport) == "ONT") {text(lon,lat,airport,cex=1,adj=0,pos=3,col=labcol) } 
  else if (as.character(airport) == "LAS") {text(lon,lat,airport,cex=1,adj=0,pos=4,col=labcol) } 
  else if (as.character(airport) == "SJC") {text(lon,lat,airport,cex=1,adj=0,pos=1,col=labcol) } 
  else if (as.character(airport) == "OAK") {text(lon,lat,airport,cex=1,adj=0,pos=4,col=labcol) } 
  else if (as.character(airport) == "SMF") {text(lon,lat,airport,cex=1,adj=0,pos=3,col=labcol) } 
  else  {text(lon,lat,airport,cex=1,adj=0,pos=2,col=labcol)} 
  
}
dev.off()

