library(bigrquery)
library(tidyverse)
library(readxl)

##########
# All of this will be in the plumber API... saved in Global environment to
# save big query hits...  obviously, remove my id...
###########
bq_auth(email = "druss@nih.gov")


#  read dictionary from Github
dictionary <- rio::import("https://episphere.github.io/conceptGithubActions/aggregate.json",format = "json")          

# read rules from local disk (should be from GitHub)


convertToVector <- function(x){
  if (is.na(x) || nchar(x)==0) return(NA)
  str_trim(unlist(str_split(x,pattern = ",")))
}
  
rules <- read_excel("~/QCRules_test.xlsx") %>% 
  mutate(ValidValues=map(ValidValues,convertToVector),
         CrossVariableConceptID1Value=map(CrossVariableConceptID1Value,convertToVector),
         CrossVariableConceptID2Value=map(CrossVariableConceptID2Value,convertToVector),
         CrossVariableConceptID3Value=map(CrossVariableConceptID3Value,convertToVector) )

#data <- rio::import("test_data.json") %>% as_tibble()


# BigQuery table where QC report will be saved---------------
QC_report_location <- "nih-nci-dceg-connect-stg-5519.Biospecimens.QC_report"

# 2 part definition for querying the data sitting in BigQuery
project <- "nih-nci-dceg-connect-stg-5519"
#sql <- "SELECT * FROM `nih-nci-dceg-connect-stg-5519.Biospecimens.flatBoxes_WL`"
sql <- "SELECT * FROM `nih-nci-dceg-connect-stg-5519.Connect.recruitment1` where Connect_ID is not NULL"

# Notice that I read the data once...  If the data gets larger
# you will want to move this into the function.  However,
# don't get into the habit of downloading the same data multiple times.
tb <- bq_project_query(project, sql)
data <- bq_table_download(tb, bigint = c("character"))

## add a failure...
## invalid SITE
test <- data
test$d_827220437[[1]] <- "3"
## xvalid1 invalid
test$d_827220437[[2]] <- "4"
test$d_512820379[[2]] <- "854703046"


# your %!in% function is not my favorite, but infix operator are cool so +1. I'm surprise 
# your function works.  I changed the syntax a bit to make it slightly more standard. 
# Be careful in using this pipes, which I think are much cooler and easier to read.
`%!in%` <- function(x,y){ !`%in%`(x,y) }

valid <- function(ConceptID,ValidValues,data,ids,...){
  # ok, this is a difficult concept, but the conceptId is passed in as string.
  # if you try to say "is the value of the conceptId that I passed in within a set of valid values
  # you would be testing in the string "d_xxxxxxx" is in the set of value values.  We need to let
  # R know that is a variable name (a symbol).  So we convert the conceptId to a symbol.  Then
  # we need to ask R if the value that the symbol points is in the set of valid values, not the symbol
  # itself.  So the !! dereferences the symbol to the value.  
  symbol_conceptId = ensym(ConceptID)
  
  ## select all the invalid rows. and save it in the "failed" tibble...
  failed <- data %>% filter( !!symbol_conceptId %!in% ValidValues)

#  quoted_conceptId = enquo(conceptId)
  ## for each failed row, get the Ids we want to place in the report...
  ## I also set the test to VALID
  ## you will need to set other values to as needed.
  failed %>% mutate(value=!!symbol_conceptId) %>%
    select( {{ids}},value) %>% 
    mutate(value2="",
           ConceptID=ConceptID,
           qc_test = "VALID",
           ValidValues=list(ValidValues),
           
           ) 
}

## just return 1 error...
crossValid1 <- function(ConceptID,ValidValues,
                        CrossVariableConceptID1,CrossVariableConceptID1Value,
                        data,ids,...){
  symbol_conceptId = ensym(ConceptID)
  symbol_cv1_Id = ensym(CrossVariableConceptID1)
  failed <- data %>% filter( !!symbol_cv1_Id %in% CrossVariableConceptID1Value, 
                             !!symbol_conceptId %!in% ValidValues)
  
  failed %>% mutate(value=!!symbol_conceptId,value2=!!symbol_cv1_Id) %>%
    select( {{ids}},value,value2) %>% 
    mutate(ConceptID=ConceptID,
           qc_test = "CROSSVALID1",
           ValidValues=list(ValidValues),
           CrossVariableConceptID1Value=list(CrossVariableConceptID1Value)) 
}


runQC <- function(data, rules, QC_report_location){
  # no need to convert int64 -> character. in the download, we already did that...
  
  # I assume you have a document that can be read into a tibble, a column can be something 
  # other than a simple primitive (e.g. a list/vector).  This is referred to as a list-column.
  # one column of the tibble is the QC Type (valid/crossvalid1).  You will want to split the 
  # table by QC_type...
  # check_valid <- rules_tibble %>% filter(QCType=="valid")
  # of course you can just stick this in the bind_rows command...

  bind_rows(
    rules %>% filter(Qctype=="valid") %>% pmap_dfr(valid,data=data,ids=Connect_ID),
    rules %>% filter(Qctype=="crossValid1") %>% pmap_dfr(crossValid1,data=data,ids=Connect_ID)
  )
}
runQC(test, rules, QC_report_location)

