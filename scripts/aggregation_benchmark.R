rm(list=ls(all=F))
library(data.table)

aggregate_dt_int <- function(dt, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       x <- dt[, sum(A), by = E] 
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time 
    } 
    return(mean(time_measures))
}
aggregate_dt_str <- function(dt, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       x <- dt[, sum(A), by = D] 
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time
    }
    return(mean(time_measures))
}
aggregate_df_int <- function(df, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       x <- aggregate(A ~ E, df, sum) 
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time
    }
    return(mean(time_measures))
}
aggregate_df_str <- function(df, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       x <- aggregate(A ~ D, df, sum) 
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time
    }
    return(mean(time_measures))
}

benchmark <- function(file_name, timer_func=default_timeit){
    result <- c()
    #print("FILE READ DATA.TABLE")
    dt = fread(file_name,header=F, sep=",", col.names = c('A','B','C','D','E'))
    result[1] <- aggregate_dt_str(dt)
    result[2] <- aggregate_dt_int(dt)

    #print ("FILE READ DATA.FRAME") 
    df <- read.table(file_name, header = FALSE,sep = ",", col.names = c('A','B','C','D','E'))    
    result[3] <- aggregate_df_str(df)
    result[4] <- aggregate_df_int(df)
    return(result)
}

plot_graph <- function(d,plot_name){
     png(paste(plot_name,".png",sep=""))
     plot(as.integer(d$size),d$DF_string,xlab="n",ylab="time taken (secs)", type="b", col="red",log="x",lwd=2)
     lines(as.integer(d$size),d$DF_num,type="o", lty=2,col="red", lwd=2)
     lines(as.integer(d$size),d$DT_string,type="b",col="blue", lwd=2)
     lines(as.integer(d$size),d$DT_num,type="o", lty=2,col="blue", lwd=2)
     lines(as.integer(d$size),d$PD_string,type="b",col="green", lwd=2)
     lines(as.integer(d$size),d$PD_num,type="o", lty=2,col="green", lwd=2)
     lines(as.integer(d$size),d$NP,type="o", lty=2,col="violet", lwd=2)
     legend("topleft",'groups', c("R-data.frame","R-data.table","Pandas","NumPy"),lty=c(1,1), lwd=c(2.5,2.5),col=c("red","blue","green","violet"))
     dev.off()
     
}

aggregation_benchmark <- function(){
    d <- data.frame(size=integer(),DT_string=double(),DT_num=double(),DF_string=double(),DF_num=double())
    c <- c("../data/smaller_file_ten.csv","../data/smaller_file_hundred.csv", "../data/smaller_file_thousand.csv", "../data/smaller_file_tenthousand.csv", "../data/smaller_file_hundredthousand.csv","../data/smaller_file_million.csv","../data/smaller_file_tenmillion.csv")
    n <- 7
    s <- c(10,100,1000,10000,100000, 1000000, 10000000)
    for (i in 1:n){
         filename <- c[i]
         print(filename)
         r = benchmark(filename)
         new_row = data.frame(size=as.integer(s[i]), DT_string=r[1],DT_num=r[2],DF_string=r[3],DF_num=r[4])
         d <- rbind(d,new_row)
    }
    d_python = read.table("../data/aggregate.csv",header=TRUE,sep=",")
    print(d_python)
    d <- cbind(d,d_python)
    print(d)
    plot_graph(d,"aggregation")
}

aggregation_benchmark()
