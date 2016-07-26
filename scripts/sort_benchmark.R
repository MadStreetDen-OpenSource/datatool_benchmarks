rm(list=ls(all=F))
library(data.table)

sort_dt_string <- function(dt,asc=TRUE, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       dt[order('D')] # - Method 1
       #setorder(dt,D) # Method 2
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time 
    } 
    return(mean(time_measures))
}
sort_dt_int <- function(dt,asc=TRUE, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       dt[order('E')] #- method1
       #setorder(dt,E) # method2
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time
    }
    return(mean(time_measures))
}
sort_df_string <- function(df,asc=TRUE, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       df[order('D'),]
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time
    }
    return(mean(time_measures))
}
sort_df_int <- function(df,asc=TRUE, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       df[order('E'),]
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time
    }
    return(mean(time_measures))
}
benchmark <- function(file_name, timer_func=default_timeit){
    result <- c()
    #print("FILE READ DATA.TABLE")
    dt = fread(file_name,header=F, sep=",", col.names = c('A','B','C','D','E'))
    result[1] <- sort_dt_string(dt)
    result[2] <- sort_dt_int(dt)

    #print ("FILE READ DATA.FRAME") i
    df <- read.table(file_name, header = FALSE,sep = ",", col.names = c('A','B','C','D','E'))
    result[3] <- sort_df_string(df)
    result[4] <- sort_df_int(df)
    return(result)
}

plot_graph <- function(d,plot_name){
     png(paste(plot_name,".png",sep=""))
     plot(as.integer(d$size),d$DF_string,xlab="n",ylab="time taken (secs)", type="b", col="red",log="x", lwd=2)
     lines(as.integer(d$size),d$DF_int,type="o", lty=2,col="red", lwd=2)
     lines(as.integer(d$size),d$DT_string,type="b",col="blue", lwd=2)
     lines(as.integer(d$size),d$DT_int,type="o", lty=2,col="blue", lwd=2)
     lines(as.integer(d$size),d$PD_string,type="b",col="green", lwd=2)
     lines(as.integer(d$size),d$PD_int,type="o", lty=2,col="green", lwd=2)
     lines(as.integer(d$size),d$NP,type="o", lty=2,col="violet", lwd=2)
     legend("topleft",'groups', c("R-data.frame","R-data.table","Pandas","NumPy"),lty=c(1,1), lwd=c(2.5,2.5),col=c("red","blue","green","violet"))
     dev.off()
     
}

sort_benchmark <- function(){
    d <- data.frame(size=integer(),DT_string=double(),DT_int=double(),DF_string=double(),DF_int=double())
    c <- c("../data/smaller_file_ten.csv","../data/smaller_file_hundred.csv", "../data/smaller_file_thousand.csv", "../data/smaller_file_tenthousand.csv", "../data/smaller_file_hundredthousand.csv","../data/smaller_file_million.csv","../data/smaller_file_tenmillion.csv")
    n <- 7 
    s <- c(10,100,1000,10000,100000, 1000000, 10000000)
    for (i in 1:n){
         filename <- c[i]
         print(filename)
         r = benchmark(filename)
         new_row = data.frame(size=as.integer(s[i]), DT_string=r[1],DT_int=r[2],DF_string=r[3],DF_int=r[4])
         d <- rbind(d,new_row)
    }
    d_python = read.table("../data/sort.csv",header=TRUE,sep=",")
    print(d_python)
    d <- cbind(d,d_python)
    print(d)
    plot_graph(d,"sort")
}

sort_benchmark()
