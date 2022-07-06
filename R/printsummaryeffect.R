#' Prints topics with a name
#' Doesn't really handle Null names tho... 
#' See https://github.com/bstewart/stm/blob/master/R/estimateEffect.R for OG
print.summary.estimateEffect <- function(x, digits = max(3L, getOption("digits") - 3L), 
                                         signif.stars = getOption("show.signif.stars"), ...) {
  cat("\nCall:\n", paste(deparse(x$call), sep = "\n", collapse = "\n"), 
      "\n\n", sep = "")
  
  for(i in 1:length(x$tables)) {
    cat(sprintf("\nTopic %i: %s:\n", x$topics[i], names(x$topics[i])))
    cat("\nCoefficients:\n")
    coefs <- x$tables[[i]]
    stats::printCoefmat(coefs, digits = digits, signif.stars = signif.stars, 
                        na.print = "NA", ...)
    cat("\n")
  }
  invisible(x)
}