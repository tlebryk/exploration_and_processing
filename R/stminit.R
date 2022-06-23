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

source("textProcessor.R")

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


loaddata <- function(publication) {
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
    read.csv(file.path(rootpath, publication, "tts_mask", "train_main1.csv")) %>%
    rename(any_of(lookup))
  
  polimask <- read.csv(file.path(rootpath, publication, "polimask", "pmask_.csv")) %>%
    rename(any_of(lookup)) %>%
    filter(poliestimation>=0.5) %>%
    distinct(doc_id)
    
  
  hkmask <-
    read.csv(file.path(rootpath, publication, "hk_mask", "hkmask.csv")) %>%
    rename(any_of(lookup)) %>%
    distinct(doc_id)
  
  full <-
    read.csv(file.path(rootpath, publication, glue('{publication}_full.csv'))) %>%
    # standardize colnames
    rename(any_of(lookup)) %>%
    # deduplicate
    select(-any_of(c("X"))) %>%
    distinct(doc_id, .keep_all=TRUE) %>%
    # create publication column
    mutate(Publication = publication,
           text = paste(Headline, "; ", Body)
           )
           
  dt <-
    read.csv(file.path(rootpath, publication, "date", "date.csv")) %>%
    rename(any_of(lookup))
  
  full %>%
    merge(ttsmask, by = "doc_id") %>%
    merge(dt, by = "doc_id") %>%
    merge(hkmask, by = "doc_id") %>%
    merge(polimask, by = "doc_id") %>%
    select("doc_id",  "text",  "Year", "Publication","Headline") %>%
    mutate(doc_id = as.character(doc_id))
}




df <- publications %>%
  lapply(loaddata) %>%
  bind_rows()


# final columns of import:
# Author
# Body
# Headline
# doc_id
# Date
# Year

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

fitmodel <- stm(out$documents, out$vocab, K=25,
                max.em.its = 5, data = out$meta,
                init.type = "Spectral",
                prevalence=~Publication + s(Year), # + as.factor(Publication)
                content=~Publication
                # data = out$meta
                )

summary(fitmodel)
findThoughts(fitmodel, bnd$Headline)

eff <- estimateEffect(c(14) ~ Publication + s(Year), stmobj = fitmodel,
                      metadata = out$meta, uncertainty = "Global")
# this will be scmp at somepoint
# 
plot.estimateEffect(eff, "Publication", topics=14, method="difference", cov.value1 = "nyt", cov.value2 = "chinadaily")
summary(eff, topics=14)


tcorre <- topicCorr(fitmodel)
install.packages("igraph")
plot.topicCorr(tcorre)
plot(eff, "Year", method = "continuous", topics = 5,
     model = fitmodel, printlegend = FALSE, xaxt = "n", xlab = "Time (2008)")
yearseq <- seq(from = as.Date("2011-01-01"),
                to = as.Date("2021-12-01"), by = "year")
# monthnames <- years(monthseq)
axis(1,at = as.numeric(yearseq) - min(as.numeric(yearseq)),
     labels = yearseq)