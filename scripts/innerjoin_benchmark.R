rm(list=ls(all=F))
library(data.table)

merge_string <- function(dt1,dt2, niter=3,fix=FALSE){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       x <- merge(dt1, dt2,by.x='D1',by.y='D2',all = FALSE)
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time
     }
    return(round(mean(time_measures),digits=3))
}
merge_int <- function(dt1,dt2, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       x <- merge(dt1, dt2,by.x='E1',by.y='E2',all = FALSE)
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time
    }
    return(round(mean(time_measures),digits=3))
}

benchmark <- function(file_name, timer_func=default_timeit, fix=FALSE){
    result <- c()
    #print("FILE READ DATA.TABLE")
    dt1 = fread(file_name,header=F, sep=",", col.names = c('A1','B1','C1','D1','E1'))
    dt2 = fread('../data/smaller_file_million_original.csv',header=F, sep=",", col.names = c('A2','B2','C2','D2','E2'))
    result[1] <- merge_string(dt1,dt2)
    result[2] <- merge_int(dt1,dt2)

    #print ("FILE READ DATA.FRAME") 
    df1 <- read.table(file_name, header = FALSE,sep = ",", col.names = c('A1','B1','C1','D1','E1'))
    df2 <- read.table('../data/smaller_file_million_original.csv', header = FALSE,sep = ",", col.names = c('A2','B2','C2','D2','E2')) 
    result[3] <- merge_string(df1,df2)
    result[4] <- merge_int(df1,df2)
    return(result)
}

plot_graph <- function(d,plot_name){
     print("GOING TO PLOT")
     png(paste(plot_name,".png",sep=""))
     plot(as.integer(d$size),d$DF_string,xlab="n",ylab="time taken (secs)", type="b", col="red",log="x", lwd=2)
     lines(as.integer(d$size),d$DF_int,type="o", lty=2,col="red", lwd=2)
     lines(as.integer(d$size),d$DT_string,type="b",col="blue",lwd=2)
     lines(as.integer(d$size),d$DT_int,type="o", lty=2,col="blue",lwd=2)
     lines(as.integer(d$size),d$PD_string,type="b",col="green",lwd=2)
     lines(as.integer(d$size),d$PD_int,type="o", lty=2,col="green",lwd=2)
     legend("topleft",'groups', c("DF","DT","PD"),lty=c(1,1), lwd=c(2.5,2.5),col=c("red","blue","green"))
     dev.off()
     
}

innerjoin_benchmark <- function(){
    d <- data.frame(size=integer(),DT_string=double(),DT_int=double(),DF_string=double(),DF_int=double())
    c <- c("../data/smaller_file_ten.csv","../data/smaller_file_hundred.csv", "../data/smaller_file_thousand.csv", "../data/smaller_file_tenthousand.csv", "../data/smaller_file_hundredthousand.csv","../data/smaller_file_million.csv","../data/smaller_file_tenmillion.csv","../data/smaller_file_hundredmillion.csv")
    n <- 8
    s <- c(10,100,1000,10000,100000, 1000000, 10000000,100000000)
    for (i in 1:n){
         filename <- c[i]
         print(filename)
         r = benchmark(filename)
         new_row = data.frame(size=as.integer(s[i]), DT_string=r[1],DT_int=r[2],DF_string=r[3],DF_int=r[4])
         d <- rbind(d,new_row)
}    
    d_python = read.table("../data/merge.csv",header=TRUE,sep=",")
    print(d_python)
    d <- cbind(d,d_python)
    print(d)
    plot_graph(d,"merge")
}

innerjoin_benchmark()
