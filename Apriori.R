#import cctxn 
#將CSV所放的資料夾路徑設為path
path = "F:/hackathon-encoded/cctxn/partition_time=3696969600/"
#path中的所有CSV檔變為list指定為files
files <- list.files(path=path, pattern="*.csv")
a <- function(x) read.csv(x,quote = "")
#將path及file合併，並使用a的function
cctxn = lapply(paste(path,files,sep=""), a)
head(cctxn)

colnames(cctxn[[1]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
colnames(cctxn[[2]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
colnames(cctxn[[3]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
colnames(cctxn[[4]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
colnames(cctxn[[5]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
colnames(cctxn[[6]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
colnames(cctxn[[7]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
colnames(cctxn[[8]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
colnames(cctxn[[9]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')

cctxn = do.call(rbind,cctxn)

cctxn <- cctxn[,-c(1,5,7,16,17)]
head(cctxn)
str(cctxn)


cctxn$PID = as.character(cctxn$PID)
cctxn$txn_amt = as.character(cctxn$txn_amt)
cctxn$original_currency = as.character(cctxn$original_currency)
cctxn$c_category_desc = as.character(cctxn$c_category_desc)
cctxn$mcc_desc = as.character(cctxn$mcc_desc)
cctxn$mcc_code = as.character(cctxn$mcc_code)
cctxn$card_type = as.character(cctxn$card_type)
cctxn$card_level = as.character(cctxn$card_level)
str(cctxn)
head(cctxn)
install.packages("stringr")
library(stringr)

#s_amt = str_split_fixed(cctxn[,6],":",n=3)
txn_amt = str_extract_all(cctxn$txn_amt, "[0-9]+\\.[0-9]+", simplify = TRUE)
head(txn_amt)
cctxn$txn_amt <- txn_amt
head(cctxn$txn_amt)

original_currency = str_split_fixed(cctxn$original_currency,"\\\\\"",n=5)
head(original_currency)
cctxn$original_currency <- original_currency[,4]
head(cctxn$original_currency)

c_category_desc = str_split_fixed(cctxn$c_category_desc,"\\\\\"",n=7)
head(c_category_desc)
cctxn$c_category_desc <- c_category_desc[,6]
head(cctxn$c_category_desc)

s_mccdesc = str_split_fixed(cctxn$mcc_desc,"\\\\\\\\",n=10)
head(s_mccdesc)


s_mcccode = str_split_fixed(cctxn$mcc_code,"\\\\\"",n=5)
head(s_mcccode)
cctxn$mcc_code <- s_mcccode[,4]
head(cctxn$mcc_code)

s_cardtype = str_split_fixed(cctxn$card_type,"\\\\\"",n=7)
head(s_cardtype)
cctxn$card_type <- s_cardtype[,6]
head(cctxn$card_type)

s_cardlevel = str_split_fixed(cctxn$card_level,"\\\\\"",n=5)
head(s_cardlevel)
cctxn$card_level <- s_cardlevel[,4]
head(cctxn$card_level)
summary(cctxn)
str(cctxn)


#轉換型態
options(scipen=999)
cctxn$txn_amt <- as.numeric(cctxn$txn_amt)
cctxn$mcc_code <- as.factor(cctxn$mcc_code)
cctxn$original_currency <- as.factor(cctxn$original_currency)
cctxn$c_category_desc <- as.factor(cctxn$c_category_desc)
cctxn$card_type <- as.factor(cctxn$card_type)
cctxn$card_level <- as.factor(cctxn$card_level)
str(cctxn)
#清洗cctxn資料完成


#cctxn與profile合併
data = read.csv("F:/hackathon-encoded/profile/partition_time=3696969600/part-00000-74cda679-a63c-4af7-8c30-cf0a24a44e8f.csv",header = F)
colnames(data) = c('PID','Birthday','Gender','Comm_Area','Comm_Areacode','House_Area','House_AreaCode','BOA','Rev','Date_Arrival','Credit_Card','Loan','Saving','FM','Data_Date')
library(dplyr)
install.packages("arules")
library(arules)
install.packages("arulesViz")
library(arulesViz)
mydata <- merge(x = cctxn, y = data, by = "PID", all.x = TRUE)
str(mydata)

#變更類別變數名稱
mydata$card_level<- factor(mydata$card_level, levels = c(1, 2,3,4,5,6,7), labels = c("card_level1", "card_level2","card_level3","card_level4","card_level5","card_level6","card_level7"))
mydata$Gender <- factor(mydata$Gender, levels = c("","M","F"), labels = c("unknown","Male", "Female"))
mydata$Credit_Card <- factor(mydata$Credit_Card, levels = c("N","Y"), labels = c("Credit_Card_N","Credit_Card_Y"))
mydata$Loan <- factor(mydata$Loan, levels = c("N","Y"), labels = c("Loan_N","Loan_Y"))
mydata$Saving <- factor(mydata$Saving, levels = c("N","Y"), labels = c("Saving_N","Saving_Y"))
mydata$FM <- factor(mydata$FM, levels = c("N","Y"), labels = c("FM_N","FM_Y"))

#將合併big table 改為稀疏矩陣
install.packages("nnet")
library(nnet)
Apri_mydata <- cbind(mydata, class.ind(mydata$action_type))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$original_currency))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$c_category_desc))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$mcc_code))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$card_type))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$card_level))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$Gender))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$Comm_Area))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$House_Area))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$Credit_Card))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$Loan))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$Saving))
Apri_mydata <- cbind(Apri_mydata, class.ind(mydata$FM))
Apri_mydata = as.data.frame(Apri_mydata)

str(Apri_mydata)
Apri_mydata1 = Apri_mydata[,-(2:25)]
str(Apri_mydata1)


# 
# mcc.code <- unique(unlist(cctxn$card_type))
# mcc.code
# for dummy in cctxn['']:
# const
# 
# function a(column){
#   sum(column)
# }

apply(mydata,2,sum)

summarise(apply())





mydata <- mydata[complete.cases(mydata),]
dim(mydata) #查維度

#Group BY PID
library(sqldf)
library(dplyr)
sqldf('select PID,sum(Apri_mydata(c[,-1])) from Apri_mydata group by PID limit 10')
test <- Apri_mydata %>% group_by(PID) %>% summarise(comsumpsionN=n(),total=sum(Apri_mydata(c[,-1])))
test <- Apri_mydata %>% group_by(PID) %>% colSums(Apri_mydata)
test <- Apri_mydata1 %>% group_by(PID) %>% summarise(apply(Apri_mydata1,2,sum))





#Apriori
# apriori.data <- mydata[,c("action_type","original_currency","c_category_desc","card_type","card_level","Gender","Credit_Card","Loan","Saving","FM")]
# apriori.data <- as(apriori.data,"transactions")
# summary(apriori.data)
# options(scipen=999)
# sort(itemFrequency(apriori.data),decreasing=T)#比例
# 
# itemFrequencyPlot(apriori.data,support=0.2,cex.names=0.8)
# rules <- apriori(apriori.data, parameter=list(support=0.2,confidence=0.5))
# summary(rules)
# plot(rules,measure=c("confidence","lift"),shading="support") #支持度、信賴度、增益
# plot(rules,method="grouped") #圖怪怪
# 
# 
# rulesff0 <- subset(rules, subset=rhs%in%"card_type=68c0679dee09a8a84415575141e7bff0"&lift>1)
# #inspect(head(sort(rulesff0,by="support"),n=5))  #set of 0 rules
# #inspect(rulesff0)  #set of 0 rules 
# 
# 
# 
# rules <- apriori(apriori.data, parameter=list(support=0.3,confidence=0.5))
# sort.rules <- sort(rules, by="lift")
# inspect(sort.rules)
# inspect(head(sort(rules,by="lift"),n=10))
# 
# rule <- apriori(apriori.data, 
#                 # min support & confidence, 最小規則長度(lhs+rhs)
#                 parameter=list(minlen=3, supp=0.3, conf=0.5),  
#                 appearance = list(default="lhs",
#                                   rhs="card_type=.") 
#                 )
# 
# inspect(rules)
# rule
# sort.rule <- sort(rule, by="lift")
# inspect(sort.rule)
# sort.rule








