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
library(sjPlot)
# library(aws.s3)

source("textProcessor.R")
source("printsagelabels.R")
source("printsummaryeffect.R")

# PREPARE DATA ###################################################
set.seed(123456)

# get training data
publications <- c("hkfp",
                  "scmp",
                  "nyt",
                  "globaltimes",
                  "chinadaily")

rootpath <- r"{C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data}"

mystops <-
  readLines(r"{C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\code\R\mystops.txt}")
polistops <-
  readLines(r"{C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\code\R\polistops.txt}")
fullstop <- c(mystops, polistops)

# alibaba ownership categorical

# data <-
#   aws.s3::s3read_using(read.csv, object = glue("s3://aliba/{publication}/tts_mask/train_main1.csv"))


loaddata <- function(publication, filename = "train_main1.csv") {
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
  ttsmask <-
    aws.s3::s3read_using(read.csv,
                         object = glue("s3://aliba/{publication}/tts_mask/{filename}")) %>%
    # read.csv(file.path(rootpath, publication, "tts_mask", filename)) %>%
    rename(any_of(lookup))
  
  
  full <-
    aws.s3::s3read_using(read.csv,
                         object = glue("s3://aliba/{publication}/{publication}_full.csv")) %>%
    # read.csv(file.path(rootpath, publication, glue('{publication}_full.csv'))) %>%
    # standardize colnames
    rename(any_of(lookup)) %>%
    # deduplicate
    select(-any_of(c("X"))) %>%
    distinct(doc_id, .keep_all = TRUE) %>%
    # create publication column
    mutate(
      Publication = factor(publication, levels = publications),
      text = paste(Headline, "; ", Body),
    )
  
  
  dt <-
    aws.s3::s3read_using(read.csv,
                         object = glue("s3://aliba/{publication}/date/date.csv")) %>%
    # read.csv(file.path(rootpath, publication, "date", "date.csv")) %>%
    rename(any_of(lookup))
  
  full %>%
    merge(ttsmask, by = "doc_id") %>%
    merge(dt, by = "doc_id") %>%
    mutate(
      doc_id = as.character(doc_id),
      Alibaba_own = baba_ownership,
      ContentPublication = factor(paste(
        Publication, as.integer(Year >= 2016), sep = "_"
      )),
      # DEDUPLICATE SHARED HEADLINES
      # ADD THIS TO STM INIT TOO
      headlow = gsub(" ", "", tolower(Headline))
    ) %>%
    group_by(headlow, Year) %>%
    filter(n() <= 1) %>%
    ungroup() %>%
    # ) %>%
    # comment me out later
    # head() %>%
    select(
      "doc_id",
      "text",
      "Year",
      "Publication",
      "ContentPublication",
      "Headline",
      "Alibaba_own"
    )
}


df <- publications %>%
  lapply(loaddata) %>%
  bind_rows() # %>%
# sample_n(50)

df2 <- df %>%
  mutate(alicnt = str_count(text, "Ali"),
         mentiontwice = alicnt >= 2) %>%
  filter(alicnt >= 3)

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

processed <- textProcessor(
  df2$text,
  onlycharacter = TRUE,
  metadata = df2,
  customstemmedstops = mystops,
)
out <- prepDocuments(
  processed$documents,
  processed$vocab,
  lower.thresh = length(processed$documents) * .0015,
  meta = processed$meta,
  # upper.thresh = length(processed$documents) * .9
  # we want to keep baba articles right?
)
# docs <- out$documents
# vocab <- out$vocab
# meta <-out$meta
out$meta %>% colnames()
out$meta %>% count(ContentPublication) %>% summarize(sum(n))
# prev_equation <- "~Publication + s(Year) + Alibaba_own"

# FIT MODEL ###################################

fitmodel <- stm(
  out$documents,
  out$vocab,
  K = 15,
  max.em.its = 75,
  data = out$meta,
  init.type = "Spectral",
  prevalence =  ~ Publication + s(Year) + Alibaba_own,
  # + as.factor(Publication)
  content =  ~ ContentPublication,
  # used to be just content
  seed = 123456,
  # gamma.prior="L1"
)
summary(fitmodel)


saveRDS(fitmodel, "./fitmodelbaba3.RDS")
fitmodel <- readRDS("./fitmodelbaba.RDS")

fitmodel$settings

# EXPLORE MODEL ################################
# use all topics
my_topics <- 1:15

# findTopic(fitmodel, c("commerce"))

labels <- labelTopics(fitmodel)
labels

p <- "scmp"

plot.STM(fitmodel, type = "summary", n = 3)#, covarlevels = c("scmp", "chinadaily"))
ast(express)

pubtopthought <- function(p, topic = my_topics[1]) {
  # sad, hacky way to add current publication to namespace for finding thoughts
  # for a publication
  newmeta <- out$meta %>%
    mutate(pub = p)
  findThoughts(
    fitmodel,
    df2$Headline,
    where = Publication == pub,
    n = 3,
    meta = newmeta
  )
  
}

findThoughts(fitmodel, df2$Headline,   n = 3, meta = out$meta)


pubtopplot <- function(p, topic =  my_topics[1]) {
  thoughts <- pubtopthought(p, topic)
  plotQuote(
    thoughts$docs[[1]],
    main = p,
    width = 80,
    text.cex = 2.0,
    cex.main = 2
  )
  thoughts
}

thoughts <- pubtopthought(p, topic)

plotQuote(
  thoughts$docs[[1]],
  main = p,
  width = 80,
  text.cex = 2.0,
  cex.main = 2
)

topic <- my_topics[1]
makeplot <- function(topic) {
  # dev.off()
  n <- 3
  par(
    mfrow = c(ceiling(length(publications) / 2), 2),
    oma = c(0, 0, 2, 0),
    mar = c(1, 1, 4, 1)
  )
  topicls <- publications %>%
    lapply(pubtopplot, topic = topic)
  title(
    paste0(
      "Figure _._: Top ",
      n,
      " headlines for topic ",
      topic,
      ": ",
      names(topic)
    ),
    line = -1,
    outer = TRUE,
    cex.main = 2.5
  )
}




makeplot(my_topics[1])

makeplot(my_topics[3])


plotQuote(thoughts[[2]][["Topic 3"]])

dev.off()
par(mfrow = c(ceiling(length(my_topics) / 2), 2), mar = c(3, 3, 4, 1))
# plot.new()

my_topics %>%
  lapply(function(x)
    plotQuote(
      thoughts[[2]][[glue("Topic {x}")]],
      main = x,
      width = 75,
      text.cex = 1.2
    ))
mtext(
  "Figure _._: Headlines of Top 4 articles related to each topic",
  side = 3,
  line = -2,
  outer = TRUE
)

# labelTopics



# findThoughts(fitmodel, out$Headline, topics=my_topics, n=5, meta=out$meta)
fitmodel$settings$covariates$yvarlevels

## Effect exploration ################################
effall <-
  estimateEffect(
    my_topics ~ Publication + s(Year) + Alibaba_own,
    stmobj = fitmodel,
    metadata = out$meta,
    uncertainty = "Global"
  )


# eff <-
#   estimateEffect(
#     my_topics ~ Publication + s(Year) + Alibaba_own,
#     stmobj = fitmodel,
#     metadata = out$meta,
#     uncertainty = "Global"
#   )
# this will be scmp at somepoint


display_tab <- function(tabl, topic) {
  # Saves html of summary table with topic num and topic label.
  dff <- data.frame(tabl, check.names = F)
  tab_df(
    dff,
    title = paste("Topic", topic, ": ", names(topic)),
    show.rownames = T,
    col.header = names(dff),
    digits = 3,
    # file=file.path("../reports/",  gsub(" ", "_", paste(topic, gsub("/", "-", names(topic)), ".html")))
  )#, CSS=css_theme("regression"))
}


summm <- summary(effall)
for (i in 1:length(summm$tables)) {
  print(display_tab(summm$tables[i], summm$topics[i]))
}
i<-9
print(display_tab(summm$tables[i], summm$topics[i]))


out$meta %>%
  count(Alibaba_own)
effall
# dev.off()
plot(
  effall,
  covariate = "Alibaba_own",
  method = "difference",
  topics = my_topics,
  cov.value1 = 1,
  cov.value2 =0,
  xlim = c(-.15, .05),
  # title=
)
topc 12title("Figure _._: Alibaba Ownership Effect Size and Standard Errors for Relevant Topics")
# pretty print regression output coefficients
print.summary.estimateEffect(effall, topics = effall$topics)

names(effall$topics[1])



tcorre <- topicCorr(fitmodel)
install.packages("igraph")
plot.topicCorr(tcorre)



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
  lapply(loaddata, filename = "test_main1.csv") %>%
  bind_rows() %>% 
  mutate(alicnt = str_count(text, "Ali"),
         mentiontwice = alicnt >= 2) %>%
  filter(alicnt >= 3)

df2 %>%
  nrow
test %>%
  nrow
processedtest <- textProcessor(
  test$text,
  onlycharacter = TRUE,
  metadata = test,
  customstemmedstops = mystops,
)
new <- stm::alignCorpus(processedtest, out$vocab)

testfit <-
  fitNewDocuments(
    fitmodel,
    documents = new$documents,
    newData = new$meta,
    origData = out$meta,
    prevalencePrior = "Covariate",
    contentPrior = "Covariate",
    betaIndex = new$meta$ContentPublication,
    prevalence = ~ Publication + s(Year) + Alibaba_own
    # content =  ~ ContentPublication
    
  )
# model <- fitmodel

# levels <- model$settings$covariates$yvarlevels
# betaindex <- as.numeric(factor(new$meta$Publication, levels = levels))
# length(betaindex)
# as.numeric(factor(
#   new$meta$Publication,
#   fitmodel$settings$covariates$yvarlevels
# )) %>%
#   tail

trainfit <-
  fitNewDocuments(
    fitmodel,
    documents = out$documents,
    newData = out$meta,
    origData = out$meta,
    prevalencePrior = "Average",
    # content=~Publication
    prevalence = ~ Publication + s(Year) + Alibaba_own
  )
nocovfit <-
  fitNewDocuments(
    fitmodel,
    documents = new$documents,
    newData = new$meta,
    origData = out$meta,
    prevalencePrior = "None",
    # content=~Publication
    prevalence = ~ Publication + s(Year) + Alibaba_own
  )


K <- 15
testeffect <- fitmodel
testeffect$theta <- testfit$theta
traineffect <- fitmodel
traineffect$theta <- trainfit$theta
nocov <- fitmodel
nocov$theta <- nocovfit$theta
prepfulltrain <-
  estimateEffect(
    my_topics ~ Publication + s(Year) + Alibaba_own,
    fitmodel,
    meta = out$meta,
    uncertainty = "None"
  )
summary(prepfulltrain)

prepfulltrainplot <-
  plot(
    prepfulltrain,
    "Alibaba_own",
    method = "difference",
    cov.value1 = 1,
    cov.value2 = 0,
    main = "Training Set",
    labeltype = "custom",
    # custom.labels=topic,
    xlab = "Treatment - Control",
    xlim = c(-.06, .07)
  )
preptest <-
  estimateEffect(
    my_topics ~ Publication + s(Year) + Alibaba_own,
    testeffect,
    meta = new$meta,
    uncertainty = "None"
  )
dev.off()
preptestplot <-
  plot(
    preptest,
    "Alibaba_own",
    method = "difference",
    cov.value1 = 1,
    cov.value2 = 0,
    main = "Test Set",
    labeltype = "custom",
    # custom.labels=topic,
    xlab = "Treatment - Control",
    xlim = c(-.06, .07)
  )
prep <-
  estimateEffect(
    my_topics ~ Publication + s(Year) + Alibaba_own,
    traineffect,
    meta = out$meta,
    uncertainty = "None"
  )
prepnocov <-
  estimateEffect(
    my_topics ~ Publication + s(Year) + Alibaba_own,
    nocov,
    meta = new$meta,
    uncertainty = "None"
  )
prepnocovplot <-
  plot(
    prepnocov,
    "Alibaba_own",
    method = "difference",
    cov.value1 = 1,
    cov.value2 = 0,
    main = "Test Set",
    labeltype = "custom",
    # custom.labels=topic,
    xlab = "Treatment - Control",
    xlim = c(-.06, .07)
  )


plot(
  preptest,
  covariate = "Alibaba_own",
  method = "difference",
  topics = my_topics,
  cov.value1 = 1,
  cov.value2 = 0,
  xlim = c(-.08, .09),
  # title=
)
title("Figure _._: Alibaba Ownership Effect Size and Standard Errors for Relevant Topics")
# plot 1
plot(
  preptest,
  "Alibaba_own",
  method = "difference",
  cov.value1 = 1,
  cov.value2 = 0,
  main = "",
  # labeltype = "custom",
  # custom.labels=topic,
  # xlab = "Treatment - Control",
  # xlim = c(-.26, .2),
  # nsims = 10000
)
dev.off()

summary(preptest)

topic = c("Jack Ma",
           "Yahoo/stocks",
           "Controversy",
           "Cloud",
           "Gender",
           "Health",
           "Singles day",
           "Digital payments",
           "Stocks",
          "Entertainment",
           "Logistics",
           "Rural", 
           "Cars",
           "Billionaires",
           "Payments(2)")
dev.off()
pdf("../reports/Alibabaeffbabatopics.pdf", width=10, height=7)

par(mfrow = c(1, 1))
plot(
  prep,
  "Alibaba_own",
  method = "difference",
  cov.value1 = 1,
  cov.value2 = 0,
  main = "",
  labeltype = "custom",
  custom.labels=topic,
  xlab = "Treatment - Control",
  xlim = c(-.12, .1),
  nsims = 10000
)
points <- unlist(preptestplot$means)
lower <- unlist(lapply(preptestplot$cis, function (x)
  x[1]))
upper <- unlist(lapply(preptestplot$cis, function (x)
  x[2]))
pointstr <- unlist(prepfulltrainplot$means)
lowertr <- unlist(lapply(prepfulltrainplot$cis, function (x)
  x[1]))
uppertr <- unlist(lapply(prepfulltrainplot$cis, function (x)
  x[2]))
pointsnc <- unlist(prepnocovplot$means)
lowernc <- unlist(lapply(prepnocovplot$cis, function (x)
  x[1]))
uppernc <- unlist(lapply(prepnocovplot$cis, function (x)
  x[2]))
for (i in 1:K) {
  points(points[i], K - i + .7, col = "red", pch = 15)
  lines(c(lower[i], upper[i]),
        c(K - i + .7, K - i + .7),
        col = "red",
        lty = 2)
  points(pointstr[i], K - i + 1.3, col = "darkgreen", pch = 17)
  lines(c(lowertr[i], uppertr[i]),
        c(K - i + 1.3, K - i + 1.3),
        col = "darkgreen",
        lty = 3)
  #points(pointsnc[i], K-i+.9, col="purple", pch=19)
  #lines(c(lowernc[i], uppernc[i]),  c( K-i+.9, K-i+.9), col="purple", lty=4)
}
legend(
  -.1,
  3,
  c("Training Set", "Training Set With \n Averaged Prior", "Test Set"),
  col = c("darkgreen", "black", "red"),
  lty = c(3, 1, 2),
  pch = c(17, 16, 15)
)

title("Figure _._: Alibaba Ownership Effect Size and Standard Errors on Topic Prevalence")

dev.off()
my_topics

sglabs <- sageLabels(fitmodel)

print.sageLabels3(sglabs, my_topics)

sglabs$cov.betas

# ALIBABA /TECH TOPIC ANALYSIS #######################

tech_topic <- c("tech" = 41)
labelTopics(fitmodel, tech_topic)
# find sage labels
print.sageLabels3(sglabs, tech_topic)

# nothing all that interesting...
techeff <-
  estimateEffect(
    tech_topic ~ Publication + s(Year) + Alibaba_own,
    stmobj = fitmodel,
    metadata = out$meta,
    uncertainty = "Global"
  )

str(techeff)

print.summary.estimateEffect(techeff, topics = techeff$topics)
summm <- summary(techeff)
for (i in 1:length(summm$tables)) {
  print(display_tab(summm$tables[i], summm$topics[i]))
}

round(summm$tables[1], 2)

is.num <- sapply(summm$tables[1], is.numeric)
summm$tables[1][is.num] <- lapply(summm$tables[1][is.num], round, 3)

write.table(summm$tables[1], file = 'temp.txt', sep = ";")
# SCRATCH FNS ######################################


summary(fitmodel)
plot(fitmodel,
     "labels",
     topics = my_topics,
     covarlevels = c("scmp", "nyt"))
labeledtopics <- labelTopics(fitmodel, topics = my_topics)
labeledtopics$interaction %>% names()

tabl <- summm$tables[[2]]
summm$topics

display_tab(summm$tables[2], summm$topics[2])
i
sjt(summm$tables[[2]])

# deduplicate global times and china daily
# df2 %>%
#   # subset(Publication %in% c("chindadaily", "globaltimes")) %>%
#   # count(Alibaba_own) %>%
#   mutate(headlow = gsub(" ", "", tolower(Headline))) %>%
#   group_by(headlow) %>%
#   filter(n() <= 1) %>%
#   ungroup() %>%
#   # count(headlow)
#   select(c(headlow, Publication)) %>%
#   arrange(headlow)