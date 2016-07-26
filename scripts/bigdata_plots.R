rm(list=ls(all=F))

plot_load_graph <- function(df,rdd,hive_table,redshift,plot_name){
     png(paste("bigdata_",plot_name,".png",sep=""))
     plot(df$size,df$time1,xlab="n",ylab="time taken (secs)", type="b", col="orange", lwd=2, ylim=c(0,80))
     lines(df$size,rdd$time1,type="b",col="black", lwd=2)
     lines(df$size,hive_table$time1,type="b",col="blue", lwd=2)
     lines(df$size,redshift$table_a_load_time,type="b",col="red", lwd=2)
     legend("topleft",'groups', c("Spark-DF","Spark-RDD","Spark-Hive","Redshift"),lty=c(1,1), lwd=c(5,5),col=c("orange","black","blue","red"))
     dev.off()
}

plot_aggregation_graph <- function(df_c,df_d,rdd_c,rdd_d,hive_table_c,hive_table_d,redshift,plot_name){
     png(paste("bigdata_",plot_name,".png",sep=""))
     plot(df_c$size,df_c$time2,xlab="n",ylab="time taken (secs)", type="b", col="orange", lwd=2, ylim=c(0,300))
     lines(df_c$size,df_d$time2,type="o",col="orange", lty=2, lwd=2)
     lines(df_c$size,rdd_c$time2,type="b",col="black", lwd=2)
     lines(df_c$size,rdd_d$time2,type="o",col="black", lty=2, lwd=2)
     lines(df_c$size,redshift$aggregation_time_C,type="b",col="red", lwd=2)
     lines(df_c$size,redshift$aggregation_time_D,type="o",col="red",lty=2, lwd=2)
     lines(df_c$size,hive_table_c$time2,type="b",col="blue", lwd=2)
     lines(df_c$size,hive_table_d$time2,type="o",col="blue",lty=2, lwd=2)
     legend("topleft",'groups', c("Spark-DF","Spark-RDD","Spark-Hive","Redshift"),lty=c(1,1), lwd=c(5,5),col=c("orange","black","blue","red"))
     dev.off()
}

plot_merge_graph <- function(df_c,df_d,rdd_c,rdd_d,hive_table_c,hive_table_d,redshift,plot_name){
     png(paste("bigdata_",plot_name,".png",sep=""))
     plot(df_c$size,df_c$time2,xlab="n",ylab="time taken (secs)", type="b", col="orange", lwd=2, ylim = c(0,200))
     #plot(df_c$size,redshift$join_time_C,xlab="n",ylab="time taken (secs)", type="b", col="orange",log="x", lwd=2)
     lines(df_c$size,df_d$time2,type="o",col="orange", lty=2, lwd=2)
     lines(df_c$size,rdd_c$time2,type="b",col="black", lwd=2)
     lines(df_c$size,rdd_d$time2,type="o",col="black", lty=2, lwd=2)
     lines(df_c$size,hive_table_c$time2,type="b",col="blue", lwd=2)
     lines(df_c$size,hive_table_d$time2,type="o",col="blue", lty=2, lwd=2)
     lines(df_c$size,redshift$join_time_C,type="b",col="red", lwd=2)
     lines(df_c$size,redshift$join_time_D,type="o",col="red",lty=2, lwd=2)
     legend("topleft",'groups', c("Spark-DF","Spark-RDD","Spark-Hive","Redshift"),lty=c(1,1), lwd=c(5,5),col=c("orange","black","blue","red"))
     dev.off()
}

plot_sort_graph <- function(df_c,df_d,rdd_c,rdd_d,hive_table_c,hive_table_d,redshift,plot_name){
     png(paste("bigdata_",plot_name,".png",sep=""))
     plot(df_c$size,df_c$time2,xlab="n",ylab="time taken (secs)", type="b", col="orange", lwd=2, ylim=c(0,70))
     #plot(df_c$size,redshift$join_time_C,xlab="n",ylab="time taken (secs)", type="b", col="orange",log="x", lwd=2)
     lines(df_c$size,df_d$time2,type="o",col="orange", lty=2, lwd=2)
     lines(df_c$size,rdd_c$time2,type="b",col="black", lwd=2)
     lines(df_c$size,rdd_d$time2,type="o",col="black", lty=2, lwd=2)
     lines(df_c$size,hive_table_c$time2,type="b",col="blue", lwd=2)
     lines(df_c$size,hive_table_d$time2,type="o",col="blue", lty=2, lwd=2)
     lines(df_c$size,redshift$table_a_sort_time_C,type="b",col="red", lwd=2)
     lines(df_c$size,redshift$table_a_sort_time_D,type="o",col="red",lty=2, lwd=2)
     legend("topleft",'groups', c("Spark-DF","Spark-RDD","Spark-Hive","Redshift"),lty=c(1,1), lwd=c(5,5),col=c("orange","black","blue","red"))
     dev.off()
}

split_function <- function(row){
      return(as.numeric(strsplit(strsplit(row[1], "s3://msd-intern/benchmark_billionset_with_header/")[[1]][2], "/")[[1]][1]))
}

main <- function(){
    redshift <- read.table("../data/spark/redshift_numbers.csv",header=TRUE,sep=",")
    #==============================================
    # Load
    #===============================================
    df = read.table("../data/spark/load/df_load.csv",header=TRUE,sep=",")
    rdd = read.table("../data/spark/load/rdd_load.csv",header=TRUE,sep=",")
    hive_table = read.table("../data/spark/load/table_load.csv",header=TRUE,sep=",")
    df$size <- apply(df, 1, split_function)

    plot_load_graph(df,rdd,hive_table,redshift,"load")

    #==============================================
    # Group by and sum 
    #===============================================
    df_c = read.table("../data/spark/groupby/df_groupby_c.csv",header=TRUE,sep=",")
    df_d = read.table("../data/spark/groupby/df_groupby_d.csv",header=TRUE,sep=",")
    rdd_c = read.table("../data/spark/groupby/rdd_groupby_c.csv",header=TRUE,sep=",")
    rdd_d = read.table("../data/spark/groupby/rdd_groupby_d.csv",header=TRUE,sep=",")
    hive_table_c = read.table("../data/spark/groupby/table_groupby_c.csv",header=TRUE,sep=",")
    hive_table_d = read.table("../data/spark/groupby/table_groupby_d.csv",header=TRUE,sep=",")
    df_c$size <- apply(df_c, 1, split_function)
    
    plot_aggregation_graph(df_c,df_d,rdd_c,rdd_d,hive_table_c, hive_table_d,redshift,"group_by")
    #==============================================
    # Merge 
    #===============================================
    df_c = read.table("../data/spark/join/df_joinby_c.csv",header=TRUE,sep=",")
    df_d = read.table("../data/spark/join/df_joinby_d.csv",header=TRUE,sep=",")
    rdd_c = read.table("../data/spark/join/rdd_joinby_c.csv",header=TRUE,sep=",")
    rdd_d = read.table("../data/spark/join/rdd_joinby_d.csv",header=TRUE,sep=",")
    hive_table_c = read.table("../data/spark/join/table_joinby_c.csv",header=TRUE,sep=",")
    hive_table_d = read.table("../data/spark/join/table_joinby_d.csv",header=TRUE,sep=",")
    df_c$size <- apply(df_c, 1, split_function)

    plot_merge_graph(df_c,df_d,rdd_c,rdd_d,hive_table_c,hive_table_d,redshift,"merge")

    #==============================================
    # Sort
    #===============================================
    df_c = read.table("../data/spark/sort/df_sortby_c.csv",header=TRUE,sep=",")
    df_d = read.table("../data/spark/sort/df_sortby_d.csv",header=TRUE,sep=",")
    rdd_c = read.table("../data/spark/sort/rdd_sortby_c.csv",header=TRUE,sep=",")
    rdd_d = read.table("../data/spark/sort/rdd_sortby_d.csv",header=TRUE,sep=",")
    hive_table_c = read.table("../data/spark/sort/table_sortby_c.csv",header=TRUE,sep=",")
    hive_table_d = read.table("../data/spark/sort/table_sortby_d.csv",header=TRUE,sep=",")
    df_c$size <- apply(df_c, 1, split_function)

    plot_sort_graph(df_c,df_d,rdd_c,rdd_d,hive_table_c,hive_table_d,redshift,"sort")
}


main()
