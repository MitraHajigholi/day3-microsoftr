# grab some data!
remoteDir <- "https://packages.revolutionanalytics.com/datasets/AirOnTimeCSV2012"
download.file(file.path(remoteDir, "airOT201201.csv"), "airOT201201.csv")
download.file(file.path(remoteDir, "airOT201202.csv"), "airOT201202.csv")

# install a nifty package
devtools::install_github("revolutionanalytics/dplyrXdf")

# load packages
library(RevoScaleR)
library(mrsdeploy)
library(tidyverse)
#library(sparklyr)
library(dplyrXdf)

# consume a csv
## make it a data.frame
df201201<-rxDataStep("airOT201201.csv",maxRowsByCols = 3000000000)
## make it an xdf
xdf201201<-rxDataStep("airOT201201.csv","airOT201201.xdf", overwrite = TRUE)

# query it
## in mem
df201201 %>% 
  count(DAY_OF_MONTH)

## from disk
xdf201201 %>% 
  count(DAY_OF_MONTH) %>% 
  collect()

xdf201201 %>% 
  count(ORIGIN) %>% 
  persist("counts")

rxGetVarInfo("airOT201201.xdf")

xdf201201 %>% 
  summarise(avg=mean(DEP_DELAY, na.rm=TRUE))%>% 
  collect()

xdf201201 %>% 
  filter(MONTH>6) %>% 
  group_by(DEST, ORIGIN) %>% 
  summarise(n=n(),avg=mean(DEP_DELAY, na.rm=TRUE)) %>% 
  collect()

xdf201201 %>% 
  filter(MONTH!="6") %>% 
  group_by(DEST, ORIGIN) %>% 
  summarise(n=n(),avg=mean(DEP_DELAY, na.rm=TRUE)) ->
  summary_result

summary(summary_result)

## Alt-summaries ----
rxCrossTabs(formula = ~ as.factor(DEST):as.factor(ORIGIN), data = xdf201201)
rxCube( DEP_DELAY ~ as.factor(DEST):as.factor(ORIGIN), 
       data =  xdf201201,
       removeZeroCounts = TRUE)
rxChiSquaredTest(rxCrossTabs(formula = ~ DEST:ORIGIN, data = xdf201201, returnXtabs = TRUE))
rxQuantile("DEP_DELAY", xdf201201,
           probs = c(0,.1,.9,1))




# leveraging remote ML Servers
mrsdeploy::remoteLogin(
  "52.151.29.166:12800",
  username = "admin",
  password = "Pa55w.rd"
)

mtcars
mtlite <- mtcars[1:10,]
ls()
pause()
mtlite
getRemoteObject("mtlite") # Simple fetch
getRemoteObject("mtlite", "rm_mtlite") # Fetch grouped
rm_mtlite[["mtlite"]]
resume()
exit

# Working with SQL Server

driver = "SQL Server" 
server = "fbmcsads.database.windows.net"
database = "WideWorldImporters-Standard"
uid = "adatumadmin"
pwd = "Pa55w.rdPa55w.rd"


con_string<-sprintf("Driver=%s;server=%s;database=%s;uid=%s; pwd=%s",
                    driver, server, database, uid, pwd)
airportdata <- RxSqlServerData(connectionString = con_string,
                               table = "flights", rowsPerRead = 1000)
rxSummary(~., airportdata)
head(airportdata)
rxGetVarInfo(airportdata)


## Transforming data during import ----
# Using the transform argument
base_tf_xdf <- rxDataStep(
  "airOT201201.csv",
  "airOT201201.xdf",
  transforms = list(
    MONTH = as.factor(MONTH),
    UNIQUE_CARRIER = paste0(UNIQUE_CARRIER, "example")
  ),
  maxRowsByCols = 10e7,
  overwrite = TRUE
)
colnames(base_tf_xdf)

# Using external variables
scaling_factor <- 1.1
scale_tf_xdf <- rxDataStep(
  "airOT201201.csv",
  "airOT201201.xdf",
  transforms = list(CarrierDelay_adj = CARRIER_DELAY * scaling_factor),
  transformObjects = list(scaling_factor = scaling_factor),
  maxRowsByCols = 10e7,
  overwrite = TRUE
)

# Using functions from packages not in basic R
pkg_tf_xdf <- rxDataStep(
  "airOT201201.csv",
  "airOT201201.xdf",
  transforms = list(UNIQUE_CARRIER = fct_lump(UNIQUE_CARRIER)),
  transformPackages = "forcats",
  maxRowsByCols = 10e7,
  overwrite = TRUE
)

pkg_tf_xdf_filt<- rxDataStep(pkg_tf_xdf, 
                                  rowSelection = UNIQUE_CARRIER=="Other")
nrow(pkg_tf_xdf_filt)

# Working with functions that transform the whole table
simple_function <- function(dataList) {
  dataList$new_col1 <- dataList$UNIQUE_CARRIER
  return(dataList)
}

func_tf_xdf <- rxDataStep(
  "airOT201201.csv",
  "airOT201201.xdf",
  transformFunc = simple_function,
  maxRowsByCols = 10e7,
  overwrite = TRUE
)

rxGetVarInfo(func_tf_xdf)

## Using rxFactors ----
# Turn something into a factor
flights_rxf <- rxFactors("airOT201201.xdf", factorInfo = c("UNIQUE_CARRIER"))
# Use alphabetical order to determine level
flights_rxf <- rxFactors("airOT201201.xdf",
                         factorInfo = c("UNIQUE_CARRIER"),
                         sortLevels = TRUE)
# Specify levels  of interest
flights_rxf <- rxFactors("airOT201201.xdf", 
                         factorInfo = list(ORIGIN = list(
                           levels = c("JFK", "LHR", "CPH"))))


## Base R over xdfs ----
summary(flights_rxf)
head(flights_rxf)
names(flights_rxf)
nrow(flights_rxf)

## ScaleR summary ----
rxSummary(~.,flights_rxf)
# Specief which statistics should be calculated
rxSummary(~.,flights_rxf,
          summaryStats = c("mean","StdDev"))
# Store summary results over one or more files
rxSummary(~.|CANCELLATION_CODE,flights_rxf,
          summaryStats = c("mean","StdDev"),
          byGroupOutFile = "summaries" ,
          overwrite=TRUE
)
# Extract components of summaries
totals<-rxSummary(~.,flights_rxf)
names(totals)
totals$sDataFrame
# Use filters and row transforms
filtered_sum<-rxSummary(~.,flights_rxf,
                        rowSelection = ArrivalDelay>0)

# Building plots when data is too big for ggplot2
flights_rxf %>% 
  rxHistogram(~DEP_DELAY | ORIGIN, data = .)

flights_rxf %>% 
  group_by(DAY_OF_MONTH, MONTH) %>% 
  summarise(avgDelay=mean(DEP_DELAY, na.rm=TRUE)) %>% 
  rxLinePlot(avgDelay ~ DAY_OF_MONTH | MONTH, 
                         data = .)

## Using rxExec to parallelise your code ----
## Run function with no parameters
rxExec(getwd)

## Pass the same set of arguments to each compute element
rxExec(list.files, all.files = TRUE, full.names = TRUE)
## Run function with the same vector sent as the first
## argument to each compute element
## The values 1 to 10 will be printed 10 times
x <- 1:100
rxExec(print, x, timesToRun = 10)

## Pass a different argument value to each compute element
## The values 1 to 10 will be printed once each
rxExec(print, rxElemArg(x))

## Extract different columns from a data frame on different nodes
set.seed(100)
myData <- data.frame(
  x = 1:100,
  y = rep(c("a", "b", "c", "d"), 25),
  z = rnorm(100),
  w = runif(100)
)
myVarsToKeep = list(c("x", "y"),
                    c("x", "z"),
                    c("x", "w"),
                    c("y", "z"),
                    c("z", "w"))
# myVarDataFrames will be a list of data frames
myVarDataFrames <-
  rxExec(rxDataStep,
         inData = myData,
         varsToKeep = rxElemArg(myVarsToKeep))

## Extract different rows from the data frame on different nodes
myRowSelection = list(
  expression(y == 'a'),
  expression(y == 'b'),
  expression(y == 'c'),
  expression(y == 'd'),
  expression(z > 0)
)

myRowDataFrames <-
  rxExec(rxDataStep,
         inData = myData,
         rowSelection = rxElemArg(myRowSelection))

## Use the taskChunkSize argument
system.time(
  rxExec(sqrt, rxElemArg(1:100000), taskChunkSize = 500)
)

system.time(
  rxExec(sqrt, rxElemArg(1:1000000), taskChunkSize = 50)
)
## Using foreach ----
library(doRSR)
registerDoRSR()
foreach_res<-foreach(i = 1:3) %dopar% sqrt(i)
rxExec(sqrt, rxElemArg(1:3))

kMeansForeach <- function(x,
                          centers = 5,
                          iter.max = 10,
                          nstart = 1)
{
  numTimes <- 20
  results <-
    foreach(i = 1:numTimes) %dopar% kmeans(
      x = x,
      centers = centers,
      iter.max = iter.max,
      nstart = nstart
    )
  best <- 1
  bestSS <- sum(results[[1]]$withinss)
  for (j in 1:numTimes)
  {
    jSS <- sum(results[[j]]$withinss)
    if (bestSS > jSS)
    {
      best <- j
      bestSS <- jSS
    }
  }
  results[[best]]
}
x <- matrix(rnorm(250000), nrow = 5000, ncol = 50)
kMeansForeach(x, 10, 35, 20)


## Clustering ----
## Import some simple data
k_data<-rxImport(inData = "airOT201201.csv", outFile = "clustervars.xdf",
                 varsToKeep=c("DAY_OF_WEEK", "ARR_DELAY", "CRS_DEP_TIME", "DEP_DELAY"),
                 colClasses = c(CRS_DEP_TIME="integer", DAY_OF_WEEK="factor"),
                 overwrite = TRUE)
rxGetVarInfo(k_data)
## Build a model
kclusts1 <- rxKmeans(formula= ~ARR_DELAY + CRS_DEP_TIME, 
                     data = k_data,
                     seed = 10,
                     outFile =k_data, numClusters=5)
kclusts1
kclusts1$betweenss / kclusts1$totss

## Try with scaling
vals_for_later_scaling<-rxSummary( ~ARR_DELAY + CRS_DEP_TIME, "clustervars.xdf")
vals_for_later_scaling

k_data %>% 
  mutate(ARR_DELAY=scale(ARR_DELAY), 
         CRS_DEP_TIME=scale(CRS_DEP_TIME)) %>% 
  rxKmeans(formula= ~ARR_DELAY + CRS_DEP_TIME, 
           data = .,
           seed = 10,
           numClusters=5) ->
  kclusts2
kclusts2
kclusts2$betweenss / kclusts2$totss

## Check multiple candidate models
my_rxKmeans<-function(x){
  res<-rxKmeans(formula= ~ARR_DELAY + CRS_DEP_TIME, 
                data = k_data,
                seed = 10,
                numClusters=x)
  data.frame(n=x,val=scales::percent(res$betweenss / res$totss))
}

my_rxKmeans(5)

rxExec(my_rxKmeans,rxElemArg(x=1:10)) %>% 
  purrr::map_df(rbind)

## Use prior centers
kclusts3 <- rxKmeans(formula= ~ARR_DELAY + CRS_DEP_TIME, 
                     data = k_data,
                     seed = 10,
                     centers = kclusts1$centers)
kclusts3

## Use generated data
rxGetVarInfo(k_data)
rxCube(ARR_DELAY ~ F(DAY_OF_WEEK):F(.rxCluster), k_data)
rxCrossTabs(ARR_DELAY ~ F(DAY_OF_WEEK):F(.rxCluster), k_data,means = TRUE)

## Regression ----
## Linear regression
lm_1<-rxLinMod(ARR_DELAY~CRS_DEP_TIME + DAY_OF_WEEK, k_data)
lm_1

lm_res1<-rxPredict(lm_1,k_data)
rxGetVarInfo(lm_res1)
rxHistogram(~ARR_DELAY_Pred,k_data)
# k_data %>% 
#   mutate(ArrDelayBin=cut(ArrDelay, 20)) %>% 
#   group_by(ArrDelayBin) %>% 
rxLinePlot(ARR_DELAY~ARR_DELAY_Pred, k_data, type = c("smooth"))
summary(lm_1)

## Linear mod lots of output
lm_2<-rxLinMod(ARR_DELAY~CRS_DEP_TIME + DAY_OF_WEEK, k_data,covCoef = TRUE)

lm_res2<-rxPredict(lm_2,k_data,predVarNames = "ARR_DELAY_Pred2",
                   computeStdErrors=TRUE,
                   interval="confidence", writeModelVars = TRUE)
summary(lm_2)
summary(lm_res2)
rxGetVarInfo(k_data)
## Logistic regression
### Prep data
rxDataStep(k_data,k_data,
           transforms=list(WasDelayed= ARR_DELAY>0),
           overwrite = TRUE)
rxGetVarInfo(k_data)
### Build model
glm_1<-rxLogit(WasDelayed~DAY_OF_WEEK+CRS_DEP_TIME, k_data)
summary(glm_1)
glm_res1<-rxPredict(glm_1,k_data)
rxGetVarInfo(k_data)
rxRocCurve(actualVarName = "WasDelayed", predVarNames = "WasDelayed_Pred",
           data = k_data, numBreaks = 20, title = "ROC for Delay Predictions")

k_data


## Decision trees ----
d_data<-rxImport(inData = "airOT201201.csv", outFile = "treevars.xdf",#
                 transforms=list(WasDelayed= factor(ARR_DELAY>0)),
                 colClasses = c(CRS_DEP_TIME="integer", DAY_OF_WEEK="factor", 
                                ORIGIN="factor",DEST="factor",
                                UNIQUE_CARRIER="factor",
                                DAY_OF_MONTH="factor",MONTH="factor"),
                 overwrite = TRUE)
rxGetVarInfo(d_data)

dt_1<-rxDTree(WasDelayed~ORIGIN+DEST+UNIQUE_CARRIER+DAY_OF_WEEK+DAY_OF_MONTH+MONTH,
              d_data,cp = 0.001)
dt_1$cptable
plotcp(rxAddInheritance(dt_1))
dt_2<-prune.rxDTree(dt_1,0.0025)
plotcp(rxAddInheritance(dt_2))
dt_res1<-rxPredict(dt_1,d_data,writeModelVars = TRUE, type = "class")
rxGetVarInfo(d_data)
sum(dt_res1$WasDelayed==dt_res1$WasDelayed_Pred, na.rm = TRUE)/nrow(dt_res1)
library(RevoTreeView)
plot(createTreeView(dt_2))
plot(rxAddInheritance(dt_2))
text(rxAddInheritance(dt_2))
rxTreeDepth(dt_2)

## splits ----
create_partition <- function(xdf, partition_size = 0.7) {
  splitDS <-
    rxSplit(
      inData = xdf,
      transforms = list(traintest = factor(ifelse(
        rbinom(.rxNumRows,
               size = 1, prob = splitperc), "train", "test"
      ))),
      transformObjects = list(splitperc = partition_size),
      outFileSuffixes = c("train", "test"),
      splitByFactor = "traintest",
      overwrite = TRUE
    )
  
  return(splitDS)
  
}
d_data_split<-create_partition(d_data)


# Load the MicrosoftML library
library(MicrosoftML)

###
# Function to read in text file and perform data preparation
###
getData <- function(tempDir, dataFile) {
  
  # Unzip and read in the file
  data <- read.csv(unz(temp, dataFile),
                   sep = "\t")
  
  # Add column names. 1st column is the text, 2nd column is the rating
  colnames(data) <- c("Text", "Rating")
  
  # Convert to a string based dataframe
  data <- data.frame(lapply(data, as.character), stringsAsFactors=FALSE)
  
  # Convert the Rating column to numeric
  data$Rating <- as.integer(data$Rating)
  
  return(data)
}

# The data we'll use is the Sentiment Labelled Sentences Data Set
# http://archive.ics.uci.edu/ml/datasets/Sentiment+Labelled+Sentences#

# We'll pull the data from the UCI database directly
# Since it's a zip file, we'll need to store and extract from some local location
library(MicrosoftML)
# So get a local temp location
temp <- tempfile()

# Download the zip file to the temp location
zipfile <- download.file("http://archive.ics.uci.edu/ml/machine-learning-databases/00331/sentiment%20labelled%20sentences.zip",temp)

# We'll use the imdb_labelled.txt file for training
dataTrain <- rxImport("sentiment labelled sentences/imdb_labelled.txt")

# Now let's setup the text featurizer transform
textTransform = list(featurizeText(vars = c(Features = "V1")))

# Train a linear model on featurized text
model <- rxFastLinear(
  V2 ~ Features, 
  data = dataTrain,
  mlTransforms = textTransform
)

# Look at the characteristics of the model
summary(model)

dataTest <- rxImport("sentiment labelled sentences/yelp_labelled.txt")
# We'll use the yelp_labelled.txt data as the test set

# Get the predictions based on the test dataset
score <- rxPredict(model, data = dataTest, extraVarsToWrite = c("V2"))

# Let's look at the prediction
head(score)

# How good was the prediction?
rxRocCurve(actualVarName = "V2", predVarNames = "Probability.1", data = score)

