library(conText)
library(dplyr)
library(glue)
library(text2vec)
library(tm)
library(quanteda)

set.seed(123456)
rootpath <- r"{C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data}"
publications <- c("hkfp",
                  "scmp",
                  "nyt",
                  "globaltimes",
                  "chinadaily")


# load scmp
loaddata <- function(publication, filename="train_main1.csv") {
  # For now only experimenting with training data to keep runtime down. 
  # later we can comment out ttsmask parts
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
  # ttsmask <-
  #   read.csv(file.path(rootpath, publication, "tts_mask", filename)) %>%
  #   rename(any_of(lookup))
  # 
  # polimask <- read.csv(file.path(rootpath, publication, "polimask", "pmask_.csv")) %>%
  #   rename(any_of(lookup)) %>%
  #   filter(poliestimation>=0.5) %>%
  #   distinct(doc_id)
  # 
  # 
  # hkmask <-
  #   read.csv(file.path(rootpath, publication, "hk_mask", "hkmask.csv")) %>%
  #   rename(any_of(lookup)) %>%
  #   distinct(doc_id)
  publication <- "scmp"
  full <-
    read.csv(file.path(rootpath, publication, glue('{publication}_full.csv'))) %>%
    # standardize colnames
    rename(any_of(lookup)) %>%
    # deduplicate
    # select(-any_of(c("X"))) %>%
    distinct(doc_id, .keep_all=TRUE) %>%
    # create publication column
    mutate(Publication = factor(publication, levels=publications),
           text = paste(Headline, "; ", Body)
    )
  
  dt <-
    read.csv(file.path(rootpath, publication, "date", "date.csv")) %>%
    rename(any_of(lookup))
  
  full %>%
    # merge(ttsmask, by = "doc_id") %>%
    merge(dt, by = "doc_id") %>%
    # merge(hkmask, by = "doc_id") %>%
    # merge(polimask, by = "doc_id") %>%
    mutate(doc_id = as.character(doc_id),
           Alibaba_own = baba_ownership) %>%
    # comment me out later
    # head() %>%
    select("doc_id",  "text",  "Year", "Publication","Headline", "Alibaba_own") 
}


df <- loaddata("scmp")

# filter alibaba articles

baba <- df %>% 
  mutate(text = tolower(text)) %>% 
  filter(grepl("alibaba", text))

# find every alibaba instance; look at window

corp <- baba %>%
  corpus 

summary(corp)

corp[1]

length(corp)

# these are really part of the entity so
more_stops<- c("group", 
               "groups", 
               "company", 
               "companies", 
               "said", "
               according",
               "post",
               "south",
               "morning",
               "own",
               "owns",
               "owned",
               "ownership",
               "owner",
               "owners",
               "holding",
               "holdings",
               # "also"
               
               )

toks <- tokens(corp, remove_punct=T, remove_symbols=T, remove_numbers=T, remove_separators=T)

toks_nostop <- tokens_select(toks, pattern = c(more_stops, stopwords("en")), selection = "remove", min_nchar=3)

# do i need min term freq? og is 5 but I think 3 seems fine? 
feats <- dfm(toks_nostop, tolower=T, verbose = FALSE) %>% dfm_trim(min_termfreq = 3) %>% featnames()

toks <- tokens_select(toks_nostop, feats, padding = TRUE)

baba_toks <- tokens_context(x = toks, pattern = "alibaba", window = 6L)

head(docvars(baba_toks), 3)

baba_dfm <- dfm(baba_toks)
baba_dfm[1:3,1:3]

# get alc embeddings for alibaba
baba_dem <- dem(x = baba_dfm, pre_trained = cr_glove_subset, transform = TRUE, transform_matrix = cr_transform, verbose = TRUE)
# head(baba_dem@docvars)
# head(baba_dem@features)

# dropped documents
setdiff(docnames(baba_dfm), baba_dem@Dimnames$docs)

#get averages for two groups of ALC em
baba_wv_party <- dem_group(baba_dem, groups = baba_dem@docvars$Alibaba_own)
dim(baba_wv_party)


baba_nns <- nns(baba_wv_party, pre_trained = cr_glove_subset, N = 10, candidates = baba_wv_party@features, as_list = TRUE)

# df$Alibaba_own

baba_nns[["0"]]
baba_nns[["1"]]

# 1 1      group         1 0.686
# 2 1      company       2 0.442
# 3 1      companies     3 0.379
# 4 1      groups        4 0.361
# 5 1      found         5 0.360
# 6 1      according     6 0.355
# 7 1      called        7 0.354
# 8 1      report        8 0.336
# 9 1      came          9 0.324
# 10 1      said         10 0.311
# clean out words like group and company bc they're part of the entity. 

# how similar is it to some words of interest? innovate doesn't have pretrained :(
cos_sim(baba_wv_party, pre_trained = cr_glove_subset, features = c('power', 'good'), as_list = FALSE)



nns_ratio(x = baba_wv_party, N = 10, numerator = "1", candidates = baba_wv_party@features, pre_trained = cr_glove_subset, verbose = FALSE)

baba_ncs <- ncs(x = baba_wv_party, contexts_dem = baba_dem, contexts = baba_toks, N = 5, as_list = TRUE)

baba_ncs[["1"]]
baba_ncs[["0"]]

