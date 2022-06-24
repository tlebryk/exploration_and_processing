library(dplyr)
library(tidyr)
library(stringr)
library(tidytext)

library(tm)
library(SnowballC)
library(stm)
library(quanteda)
library(glue)
library(ggplot2)

library(aws.s3)

source("textProcessor.R")
set.seed(123456)

# get training data
publications <- c("hkfp",
                  "scmp",
                  "nyt",
                  "globaltimes",
                  "chinadaily")

rootpath <- r"{C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data}"

mystops <- readLines(r"{C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\code\mystops.txt}")
polistops <- readLines(r"{C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\code\polistops.txt}")
fullstop <- c(mystops, polistops)

# alibaba ownership categorical

data <- 
  aws.s3::s3read_using(read.csv, object = glue("s3://newyorktime/{publication}/tts_mask/train_main1.csv"))


loaddata <- function(publication, filename="train_main1.csv") {
  lookup <-
    c(
      doc_id = "Index",
      doc_id = "id",
      doc_id = "x_id",
      doc_id = "Art_id",
      doc_id = "sourceurl",
      Body = "plainText",
      Body = "text",
      Headline = "title" # nyt and gt?
    )
  ttsmask <- aws.s3::s3read_using(read.csv, object = glue("s3://newyorktime/{publication}/tts_mask/train_main1.csv")) %>%
    # read.csv(file.path(rootpath, publication, "tts_mask", filename)) %>%
    rename(any_of(lookup))
  
  polimask <- 
    aws.s3::s3read_using(read.csv, object = glue("s3://newyorktime/{publication}/polimask/pmask_.csv")) %>%
    
    # read.csv(file.path(rootpath, publication, "polimask", "pmask_.csv")) %>%
    rename(any_of(lookup)) %>%
    filter(poliestimation>=0.5) %>%
    distinct(doc_id)
    
  
  hkmask <-
    aws.s3::s3read_using(read.csv, object = glue("s3://newyorktime/{publication}/hk_mask/hkmask.csv")) %>%
    # read.csv(file.path(rootpath, publication, "hk_mask", "hkmask.csv")) %>%
    rename(any_of(lookup)) %>%
    distinct(doc_id)
  
  full <-
    aws.s3::s3read_using(read.csv, object = glue("s3://newyorktime/{publication}/{publication}_full.csv")) %>%
    # read.csv(file.path(rootpath, publication, glue('{publication}_full.csv'))) %>%
    # standardize colnames
    rename(any_of(lookup)) %>%
    # deduplicate
    select(-any_of(c("X"))) %>%
    distinct(doc_id, .keep_all=TRUE) %>%
    # create publication column
    mutate(Publication = factor(publication, levels=publications),
           text = paste(Headline, "; ", Body)
           )
           
  dt <-
    aws.s3::s3read_using(read.csv, object = glue("s3://newyorktime/{publication}/date/date.csv")) %>%
    # read.csv(file.path(rootpath, publication, "date", "date.csv")) %>%
    rename(any_of(lookup))
  
  full %>%
    merge(ttsmask, by = "doc_id") %>%
    merge(dt, by = "doc_id") %>%
    merge(hkmask, by = "doc_id") %>%
    merge(polimask, by = "doc_id") %>%
    mutate(doc_id = as.character(doc_id),
           Alibaba_own = baba_ownership) %>%
    # comment me out later
    # head() %>%
    select("doc_id",  "text",  "Year", "Publication","Headline", "Alibaba_own") 
}


df <- publications %>%
  lapply(loaddata) %>%
  bind_rows() %>%
  sample_n(50)


# final columns of import:
# Author
# Body
# Headline
# doc_id
# Date
# Year
# Alibaba_own

# tokenize
# lower case
# punctuation
# stop words 1
# remove numbers
# stem
### stop words 2
# output

processed <- textProcessor(df$text,
                           onlycharacter=TRUE,
                           metadata = df,
                           customstemmedstops = fullstop,
                           )
out <- prepDocuments(processed$documents,
                     processed$vocab, 
                     lower.thresh = length(processed$documents) *.001, 
                     meta=processed$meta,
                     upper.thresh = length(processed$documents) *.9
)
docs <- out$documents
vocab <- out$vocab
meta <-out$meta

# prev_equation <- "~Publication + s(Year) + Alibaba_own"

fitmodel <- stm(docs, out$vocab, K=5,
                max.em.its = 5, data = out$meta,
                init.type = "Spectral",
                prevalence=~Publication + s(Year) + Alibaba_own, # + as.factor(Publication)
                content=~Publication,
                seed=1,
                # gamma.prior="L1"
                # data = out$meta
                )




summary(fitmodel)
findThoughts(fitmodel, out$Textcol, topics=1, n=2)

eff <- estimateEffect(c(1) ~ Publication + s(Year), stmobj = fitmodel,
                      metadata = out$meta, uncertainty = "Global")
# this will be scmp at somepoint
# 
plot.estimateEffect(eff, "Publication", topics=1, method="difference", cov.value1 = "scmp", cov.value2 = "chinadaily")
summary(eff, topics=1)


tcorre <- topicCorr(fitmodel)
install.packages("igraph")
plot.topicCorr(tcorre)
# plot(eff, "Year", method = "continuous", topics = 5,
#      model = fitmodel, printlegend = FALSE, xaxt = "n", xlab = "Time (2008)")
# yearseq <- seq(from = as.Date("2011-01-01"),
#                 to = as.Date("2021-12-01"), by = "year")
# # monthnames <- years(monthseq)
# axis(1,at = as.numeric(yearseq) - min(as.numeric(yearseq)),
#      labels = yearseq)


# SAVE THE MODEL
# saveRDS(fitmodel, "./fitmodel.RDS")



 # Running on sample after textprocessing #####################
# processed is stm object output from TextProcessor
# indices <- sample(rownames(processed$meta), 10)
# 
# 
# out$meta[indices,]
# docs2 <- out$documents[indices]
# fitmodel <- stm(outhd$documents, outhd$vocab, K=3,
#                 max.em.its = 5,
#                 data = outhd$meta,
#                 init.type = "Spectral",
#                 prevalence=~Publication + s(Year) + Alibaba_own, # + as.factor(Publication)
#                 content=~Publication,
#                 seed=1,
#                 # gamma.prior="L1"
#                 # data = out$meta
# )
# 
# summary(fitmodel)

# Running on test set ##################################
# inspired by experiment 1 from 
# https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/QHJN8V
test <- publications %>%
  lapply(loaddata, filename="test_main1.csv") %>%
  bind_rows() %>% 
  sample_n(50)
  

processedtest <- textProcessor(test$text,
                               onlycharacter=TRUE,
                               metadata = test,
                               customstemmedstops = fullstop,
)
new <- stm::alignCorpus(processedtest,out$vocab)

testfit <- fitNewDocuments(fitmodel, documents=new$documents, newData=new$meta, 
                           origData=out$meta, prevalencePrior = "Average", prevalence= ~Publication + s(Year) + Alibaba_own)

trainfit <- fitNewDocuments(fitmodel, documents=out$documents, newData=out$meta, 
                            origData=out$meta, prevalencePrior = "Average", prevalence= ~Publication + s(Year) + Alibaba_own)
nocovfit <- fitNewDocuments(fitmodel, documents=new$documents, newData=new$meta, 
                            origData=out$meta, prevalencePrior = "None", prevalence= ~Publication + s(Year) + Alibaba_own)


K <- 5


testeffect <- fitmodel
testeffect$theta <- testfit$theta
traineffect <- fitmodel
traineffect$theta <- trainfit$theta
nocov <- fitmodel
nocov$theta <- nocovfit$theta
prepfulltrain <- estimateEffect(c(1:K) ~Publication + s(Year) + Alibaba_own, fitmodel, meta=out$meta, uncertainty = "None")
prepfulltrainplot <- plot(prepfulltrain, "Alibaba_own", method="difference", cov.value1=1, cov.value2=0, 
                          main="Training Set", labeltype="custom",
                          # custom.labels=topic,
                          xlab="Treatment - Control",xlim=c(-.06,.07))
preptest <- estimateEffect(c(1:K) ~Publication + s(Year) + Alibaba_own, testeffect, meta=new$meta, uncertainty = "None")
preptestplot <- plot(preptest, "Alibaba_own", method="difference", cov.value1=1, cov.value2=0, 
                     main="Test Set", labeltype="custom",
                     # custom.labels=topic, 
                     xlab="Treatment - Control",xlim=c(-.06,.07))
prep <- estimateEffect(c(1:K) ~Publication + s(Year) + Alibaba_own, traineffect, meta=out$meta, uncertainty="None")
prepnocov <- estimateEffect(c(1:K) ~Publication + s(Year) + Alibaba_own, nocov, meta=new$meta, uncertainty = "None")
prepnocovplot <- plot(prepnocov, "Alibaba_own", method="difference", cov.value1=1, cov.value2=0, 
                      main="Test Set", labeltype="custom",
                      # custom.labels=topic,
                      xlab="Treatment - Control",xlim=c(-.06,.07))


# plot 1 
plot(preptest, "Alibaba_own", method="difference", cov.value1=1, cov.value2=0, main="", labeltype="custom",
     # custom.labels=topic, 
     xlab="Treatment - Control", xlim=c(-.26,.2), nsims = 10000)
dev.off()



# plot(s) 2
par(mfrow=c(1,1))
plot(prep, "Alibaba_own", method="difference", cov.value1=1, cov.value2=0, main="", labeltype="custom",
     # custom.labels=topic, 
     xlab="Treatment - Control", xlim=c(-.2,.22), nsims = 10000)
points <- unlist(preptestplot$means)
lower <- unlist(lapply(preptestplot$cis, function (x) x[1]))
upper <- unlist(lapply(preptestplot$cis, function (x) x[2]))
pointstr <- unlist(prepfulltrainplot$means)
lowertr <- unlist(lapply(prepfulltrainplot$cis, function (x) x[1]))
uppertr <- unlist(lapply(prepfulltrainplot$cis, function (x) x[2]))
pointsnc <- unlist(prepnocovplot$means)
lowernc <- unlist(lapply(prepnocovplot$cis, function (x) x[1]))
uppernc <- unlist(lapply(prepnocovplot$cis, function (x) x[2]))
for(i in 1:K){
  points(points[i], K-i+.7, col="red", pch=15)
  lines(c(lower[i], upper[i]),  c( K-i+.7, K-i+.7), col="red", lty=2)
  points(pointstr[i], K-i+1.3, col="darkgreen", pch=17)
  lines(c(lowertr[i], uppertr[i]),  c( K-i+1.3, K-i+1.3), col="darkgreen", lty=3)
  #points(pointsnc[i], K-i+.9, col="purple", pch=19)
  #lines(c(lowernc[i], uppernc[i]),  c( K-i+.9, K-i+.9), col="purple", lty=4)
}
legend(.043, 3, c("Training Set", "Training Set With \n Averaged Prior", "Test Set"),
       col=c("darkgreen", "black", "red"), lty=c(3,1,2), pch=c(17,16,15))
