.yarnrEnv <- new.env()
yarnr.init <- function(hadoop_config=NULL) {
  yarnr.CP<- c( list.files(paste(system.file(package="yarnr"),"java", sep=.Platform$file.sep ),pattern="jar$", full.names=TRUE)
               ,hadoop_config
              )
  assign("classpath", yarnr.CP, envir =.yarnrEnv )
  .jinit(classpath=yarnr.CP)
  return (paste("Classpath added successfully: ", yarnr.CP))
}

yarnr.makeCluster <- function(session=NULL
                             ,zkConnectString=NULL
                             ,numNodes=1
                             ,memoryPerNodeInMB=200
                             ,dockerImage=NULL
                             ,port=10170
                             )
{
  yarnrClient <- .jnew("com.hortonworks.datascience.YarnRClient", session, zkConnectString)
  cl <- vector("list",1)
  old <- options(timeout = 10000)
  on.exit(options(old))
  
  .jcall(yarnrClient,"V","start","centos6-r",as.integer(port),as.integer(numNodes))
  con <- socketConnection(port = port, server=TRUE, blocking=TRUE,
                          open="a+b")
  cl[[1]] <- structure(list(con = con, host = "localhost", rank = numNodes), class = "SOCKnode")
  class(cl) <- c("SOCKcluster", "cluster")
  cl
}
