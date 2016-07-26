rm(list=ls(all=F))
library(data.table)

default_timeit <- function(func,args, niter=3){
   time_measures = c()
   for (i in 1:niter){
       gc()
       start_time <- Sys.time()
       func(args)
       end_time <- difftime(Sys.time(), start_time, units="secs")
       time_measures[i] <- end_time 
    } 
    return(mean(time_measures))
}

benchmark <- function(file_name){
    result <- c()
    #print("FILE READ DATA.TABLE")
    result[1] <- default_timeit(fread,file_name)

    #print ("FILE READ DATA.FRAME")     
    result[2] <- default_timeit(read.table,file_name)
    return(result)
}

plot_graph <- function(d,plot_name){
     png(paste(plot_name,".png",sep=""))
     plot(as.integer(d$size),d$DF,xlab="n",ylab="time taken (secs)", type="b", col="red", lwd=2)
     lines(as.integer(d$size),d$DT,type="b",col="blue",lwd=2)
     lines(as.integer(d$size),d$PD,type="b",col="green",lwd=2)
     lines(as.integer(d$size),d$NP_CSV,type="b",col="violet",lwd=2)
     lines(as.integer(d$size),d$NPY,type="b",col="orange",lwd=2)
     lines(as.integer(d$size),d$NPZ,type="b",col="black",lwd=2)
     legend("topleft",'groups', c("R-DataFram","R-DataTable","Pandas","NumPy_CSV","NumPy-NPY Files","NumPy-NPZ Files"),lty=c(1,1), lwd=c(2.5,2.5),col=c("red","blue","green","violet","orange","black"))
     title(main=plot_name,outer = TRUE)
     dev.off()
     
}

multicol_file_lod_benchmark <- function(){
    d <- data.frame(size=integer(),DT=double(),DF=double())
    c <- c("../data/smaller_file_ten.csv","../data/smaller_file_hundred.csv", "../data/smaller_file_thousand.csv", "../data/smaller_file_tenthousand.csv", "../data/smaller_file_hundredthousand.csv","../data/smaller_file_million.csv","../data/smaller_file_tenmillion.csv")
    n <- 7
    s <- c(10,100,1000,10000,100000, 1000000, 10000000)
    for (i in 1:n){
         filename <- c[i]
         print(filename)
         r = benchmark(filename)
         new_row = data.frame(size=as.integer(s[i]), DT=r[1],DF=r[2])
         d <- rbind(d,new_row)
    }
    d_python = read.table("../data/multicols.csv",header=TRUE,sep=",")
    d <- cbind(d,d_python)
    print(d)
    plot_graph(d,"multicol")
}

singlecol_file_lod_benchmark <- function(){
    d <- data.frame(size=integer(),DT=double(),DF=double())
    c <- c("../data/single_column_ten.csv","../data/single_column_hundred.csv", "../data/single_column_thousand.csv", "../data/single_column_tenthousand.csv", "../data/single_column_hundredthousand.csv","../data/single_column_million.csv","../data/single_column_tenmillion.csv")
    n <- 7
    s <- c(10,100,1000,10000,100000, 1000000,10000000)
    for (i in 1:n){
         filename <- c[i]
         print(filename)
         r = benchmark(filename)
         new_row = data.frame(size=as.integer(s[i]), DT=r[1],DF=r[2])
         d <- rbind(d,new_row)
    }
    d_python = read.table("../data/singlecols.csv",header=TRUE,sep=",")
    d <- cbind(d,d_python)
    print(d)
    plot_graph(d,"singlecol" )
}
multicol_file_lod_benchmark()
singlecol_file_lod_benchmark()
