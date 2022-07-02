#' Alternative to print.sagelabels function which allows for
#' topic filtering on print. 

commas <- function(text){  
  paste(text[nchar(text)>0], collapse=", ")
}


print.sageLabels2 <- function(x, topicnums=NULL, ...) {
  if(is.null(topicnums)) topicnums <- 1:x$K
  
  # topicnums <- 1:x$K
  
  #copying old stuff below
  for(i in topicnums) {
    toprint <- sprintf("Topic %i: \n \t Marginal Highest Prob: %s \n \t Marginal FREX: %s \n \t Marginal Lift: %s \n \t Marginal Score: %s \n \n", 
                       i, 
                       commas(x$marginal$prob[i,]),
                       commas(x$marginal$frex[i,]),
                       commas(x$marginal$lift[i,]),
                       commas(x$marginal$score[i,]))
    toprint2 <- sprintf(" \t Topic Kappa: %s \n \t Kappa with Baseline: %s \n \n",
                        commas(x$kappa[i,]),
                        commas(x$kappa.m[i,]))
    combine <- paste0(toprint, toprint2)
    for(a in 1:length(x$cov.betas)) {
      text <- sprintf(" \t Covariate %s: \n \t \t Marginal Highest Prob: %s \n \t \t Marginal FREX: %s \n \t \t Marginal Lift: %s \n \t \t Marginal Score: %s \n", 
                      x$covnames[a], 
                      commas(x$cov.betas[[a]]$problabels[i,]),
                      commas(x$cov.betas[[a]]$frexlabels[i,]),
                      commas(x$cov.betas[[a]]$liftlabels[i,]),
                      commas(x$cov.betas[[a]]$scorelabels[i,]))
      combine <- paste0(combine,text)
    }
    cat(combine)
  }
}



print.sageLabels3 <- function(x, topicnums=NULL, ...) {
  if(is.null(topicnums)) topicnums <- 1:x$K
  
  # topicnums <- 1:x$K
  
  #copying old stuff below
  for(i in topicnums) {
    toprint <- sprintf("Topic %i:  \n \t Marginal Lift: %s \n", 
                       i, 
                       # commas(x$marginal$prob[i,]),
                       # commas(x$marginal$frex[i,]),
                       commas(x$marginal$lift[i,])
                       # commas(x$marginal$score[i,])
                       )
    # toprint2 <- sprintf(" Topic Kappa: %s \n \t Kappa with Baseline: %s \n \n",
    #                     commas(x$kappa[i,]),
    #                     commas(x$kappa.m[i,]))
    combine <- toprint# paste0(toprint, toprint2)
    for(a in 1:length(x$cov.betas)) {
      text <- sprintf("%s: %s \n", 
                      x$covnames[a], 
                      # commas(x$cov.betas[[a]]$problabels[i,]),
                      # commas(x$cov.betas[[a]]$frexlabels[i,]),
                      commas(x$cov.betas[[a]]$liftlabels[i,])
                      # commas(x$cov.betas[[a]]$scorelabels[i,])
                      )
      combine <- paste0(combine,text)
    }
    cat(combine)
  }
}
