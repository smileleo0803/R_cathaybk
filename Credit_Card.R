
#import profile
path = "F:/hackathon-encoded/profile"
dirs <- list.dirs(path=path)
dirs <- dirs[-1]
files <- list.files(path=dirs,pattern="*.csv")
a <- function(x) read.csv(x,quote = "",header=F)
myfiles = lapply(paste(dirs,"/",files,sep=""), a)

for(i in c(1:13)){
  colnames(myfiles[[i]]) <- c('PID','Birthday','Gender','Comm_Area','Comm_Areacode','House_Area','House_AreaCode','BOA','Rev','Date_Arrival','Credit_Card','Loan','Saving','FM','Data_Date')
}
profile_rbind = do.call(rbind,myfiles)
head(profile_rbind)
library(anytime)
profile_rbind$Data_Date <- as.Date(anytime(profile_rbind$Data_Date))
install.packages("reshape")
library(reshape)
profile_rbind.pivot_Card <-profile_rbind[,c("PID","Data_Date","Credit_Card")]
#做個(pv,condition,value)
use_card <- cast(profile_rbind.pivot_Card,PID~Data_Date)#use_card僅有PID及各期是否有辦卡

#做個profile完整表格(含各期是否辦卡)→profile_card
profile1 = read.csv("F:/hackathon-encoded/profile/partition_time=3696969600/1.csv",header = F)
colnames(profile1) = c('PID','Birthday','Gender','Comm_Area','Comm_Areacode','House_Area','House_AreaCode','BOA','Rev','Date_Arrival','Credit_Card','Loan','Saving','FM','Data_Date')
basic_profile <- profile1[,1:14]
profile_card = merge(x = basic_profile , y = use_card , by = "PID", all = TRUE)
head(profile_card)



#查剪卡比率

library(sapply)
library(apply)
a <- sapply(use_card,function(x){x=="N"})
a <- data.frame(a)
k = apply(a,1,sum)
use_card<-cbind(use_card,k)
colnames(use_card)[15]= 'N_of_NoCard'
str(use_card)

#起初有卡，最後剪卡的人數
library(dplyr)
filter(use_card,`2087-02-25` =="Y" & `2088-02-26` =="N") %>% summarise(number=n())
filter(use_card,`2088-02-26` =="Y") %>% summarise(number=n())
filter(use_card,`2087-02-25` =="Y" & `2088-02-26` =="Y") %>% summarise(number=n())

#剪卡比率
r = filter(use_card,`2087-02-25` =="Y" & `2088-02-26` =="N") %>% summarise(number=n()) / filter(use_card,`2087-02-25` =="Y") %>% summarise(number=n()) *100
paste (round(r,2),"%",sep="")

library(sqldf)
sqldf("select `PID`,`2087-02-25`,`2088-02-26`,count(PID) as`count` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'N'")




#import cti
path2 = "F:/hackathon-encoded/cti"
dirs2 <- list.dirs(path=path2)
dirs2 <- dirs2[-1]
for(dir in dirs2){
  assign(dir,list.files(path=dir, pattern="*.csv"))
}

pathes = list()
for(dir in dirs2){
  for(ele in eval(as.symbol(dir))){
    pathes[length(pathes)+1] <- paste(dir,"/",ele,sep="") 
  }
}

length(pathes)
pathes

a <- function(x) tryCatch(read.csv(x,quote = "",header = FALSE),error=function(e) NULL)

myfiles2 <- lapply(pathes,a)
length(myfiles2)
head(myfiles2)

for(i in c(1:211)){
  colnames(myfiles2[[i]]) <- c('1','PID','3','inbound_time','5','call_purpose','7','8','call_nbr','end_call_date','calltype_desc','detail_desc','business_desc','14','15')
}

head(myfiles2[[1]])

cti = do.call(rbind, myfiles2)

cti <- cti[,-c(1,3,5,7,8,14,15)]
head(cti)
summary(cti)
str(cti)


#清洗
library(stringr)
call_nbr = str_split_fixed(cti$call_nbr,"\\\\\"",n=10)
head(call_nbr)
cti$call_nbr <- call_nbr[,6]
head(cti$call_nbr)

library(lubridate)
library(anytime)
e <- as.Date(anytime(cti$inbound_time))

# difftime(cti$inbound_time[1,2], cti$inbound_time[1,1], units="secs")

cti$end_call_date = str_extract_all(cti$end_call_date, "[0-9]+", simplify = TRUE)
head(cti$end_call_date)

d <- as.numeric(cti$end_call_date)
str(d)

d <- as.Date(anytime(d))


end_call_date <- as.numeric(cti$end_call_date)
cti$end_call_date <- as.POSIXct(anytime(end_call_date))
e<- as.numeric(end_call_date)
e <- as.POSIXct(e)
head(cti$end_call_date)

calltype_desc = str_split_fixed(cti$calltype_desc,"\\\\\"",n=10)
head(calltype_desc)
cti$calltype_desc <- calltype_desc[,6]
head(cti$calltype_desc)


detail_desc = str_split_fixed(cti$detail_desc,"\\\\\"",n=7)
head(detail_desc)
cti$detail_desc <- detail_desc[,4]
head(cti$detail_desc)

business_desc = str_split_fixed(cti$business_desc,"\\\\\"",n=7)
head(business_desc)
cti$business_desc <- business_desc[,4]
head(cti$business_desc)

str(cti)


#轉換時間及格式
cti[,2] <- as.POSIXct(anytime(cti[,2]))
cti$call_nbr <- as.factor(cti$call_nbr)
cti$calltype_desc <- as.factor(cti$calltype_desc)
cti$detail_desc <- as.factor(cti$detail_desc)
cti$business_desc <- as.factor(cti$business_desc)
cti$call_purpose <- as.factor(cti$call_purpose)
head(cti)
str(cti)


#剪卡客戶打客服之記錄
library(sqldf)
cut_call <- sqldf("
      select `PID`, call_purpose, business_desc,calltype_desc,detail_desc
      from cti
      where `PID` IN (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'N')
      order by PID
      ")
cut_call$call_purpose <- as.factor(cut_call$call_purpose)
summary(cut_call)

#call_purpose次數分配
a <- cut_call %>% group_by(call_purpose) %>% summarise(N=n()) %>% arrange(desc(N))
canvas <- ggplot(data= a[1:10,])
canvas +
  geom_bar(aes(x=call_purpose,
               y=N),stat = "identity", 
           fill = "steelblue")
# canvas +
#   geom_histogram(aes(x=call_purpose,
#                y = ..N..),stat = "identity", 
#                binwidth = 0.1,
#            fill = "steelblue")
# canvas <- ggplot(data= a[1:10,], aes(call_purpose)) +
#   geom_histogram(aes(y = ..N..))


#calltype_desc次數分配
a <- cut_call %>% group_by(calltype_desc) %>% summarise(N=n()) %>% arrange(desc(N))
canvas <- ggplot(data= a[1:10,])
canvas +
  geom_bar(aes(x=calltype_desc,
               y=N),stat = "identity", 
           fill = "steelblue")

#detail_desc次數分配
a <- cut_call %>% group_by(detail_desc) %>% summarise(N=n()) %>% arrange(desc(N))
canvas <- ggplot(data= a[1:10,])
canvas +
  geom_bar(aes(x=detail_desc,
               y=N),stat = "identity", 
           fill = "steelblue")

#剪卡-客服原因直方圖
library(ggplot2)
canvas <- ggplot(data=cut_call)
canvas +
  geom_bar(aes(x=business_desc),fill = "steelblue")     



oldget_call <- sqldf("
      select `PID`, call_purpose, business_desc ,calltype_desc,detail_desc
      from cti
      where `PID` IN (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'Y')
      order by PID
      ")
oldget_call$call_purpose <- as.factor(oldget_call$call_purpose)
summary(oldget_call)


options(scipen=999)
#call_purpose次數分配
a <- oldget_call %>% group_by(call_purpose) %>% summarise(N=n()) %>% arrange(desc(N))
canvas <- ggplot(data= a[1:10,])
canvas +
  geom_bar(aes(x=call_purpose,
               y=N),stat = "identity", 
           fill = "steelblue")
#calltype_desc次數分配
a <- oldget_call %>% group_by(calltype_desc) %>% summarise(N=n()) %>% arrange(desc(N))
canvas <- ggplot(data= a[1:10,])
canvas +
  geom_bar(aes(x=calltype_desc,
               y=N),stat = "identity", 
           fill = "steelblue")
#detail_desc次數分配
a <- oldget_call %>% group_by(detail_desc) %>% summarise(N=n()) %>% arrange(desc(N))
canvas <- ggplot(data= a[1:10,])
canvas +
  geom_bar(aes(x=detail_desc,
               y=N),stat = "identity", 
           fill = "steelblue")




#老卡友-客服原因直方圖
canvas <- ggplot(data=oldget_call)
canvas +
  geom_bar(aes(x=business_desc),fill = "steelblue")


#合併客服原因
c <- rep(1,23388)
c <- cbind(cut_call,c)
colnames(c)[4]= 'Type'
cut_call <- c
head(cut_call)

c <- rep(0,1447047)
c<- cbind(oldget_call,c)
colnames(c)[4]= 'Type'
oldget_call <- c
call <- rbind(cut_call,oldget_call)
call$Type <- as.factor(call$Type) 
summary(call)
str(call)

#合併客服原因直方圖
canvas <- ggplot(data=call)
canvas +
  geom_bar(aes(x=business_desc,
           fill = Type))



##打客服次數
library(dplyr)
#死的平均每人打電話次數  #8.623894
nrow(cut_call)/filter(use_card,`2087-02-25` =="Y" & `2088-02-26` =="N") %>% summarise(number=n())
#活的平均每人打電話次數  #6.68348
nrow(oldget_call)/filter(use_card,`2087-02-25` =="Y" & `2088-02-26` =="Y") %>% summarise(number=n())

ggplot(diamonds, aes(carat)) +
  geom_histogram(aes(y = ..density..), binwidth = 0.1)


# N.cut_call <- cut_call %>% group_by(PID) %>% order_by(N) %>%summarise(N=n())
# cut_call %>% group_by(PID)  %>%summarise(N=n()) %>% arrange(desc(N))
# summary(N.cut_call)

#剪卡客服次數直方圖
canvas <- ggplot(data=N.cut_call)
canvas +
  geom_bar(aes(x=N,
           fill = "steelblue"
           ))
#箱型圖
boxplot(N.cut_call$N,horizontal = T,col='#f8f3c4')
title("剪卡客戶撥打客服次數箱型圖")
hist(N.cut_call$N,breaks=50,main="剪卡客戶撥打客服次數分配圖",xlab="剪卡客戶撥打客服次數")



N.oldget_call <- oldget_call %>% group_by(PID) %>% summarise(N=n())
summary(N.oldget_call)
oldget_call %>% group_by(PID)  %>%summarise(N=n()) %>% arrange(desc(N))

canvas <- ggplot(data=N.oldget_call)
canvas +
  geom_bar(aes(x=N,
               fill = "blue"
  ))

#箱型圖
boxplot(N.oldget_call$N,horizontal = T,col='#f8f3c4')
title("忠實客戶撥打客服次數箱型圖")
hist(N.oldget_call$N,breaks=50,main="忠實客戶撥打客服次數分配圖",xlab="忠實客戶撥打客服次數")


##合併
c = rep(1,2634)
N.cut_call <- cbind(N.cut_call,c)
colnames(N.cut_call)[3]= 'Type'
head(N.cut_call)


c = rep(0,180795)
N.oldget_call <- cbind(N.oldget_call,c)
colnames(N.oldget_call)[3]= 'Type'
head(N.oldget_call)

callcount <- rbind(N.cut_call,N.oldget_call)
callcount$Type <- as.factor(callcount$Type) 
summary(callcount)
str(callcount)

#合併盒鬚圖
qplot(x=Type,                               
      y=N,
      data=callcount,                     
      geom="boxplot",      
      xlab="忠實VS剪卡客戶",                          
      color= Type      
)

#合併直方圖
canvas <- ggplot(data=callcount)
canvas +
  geom_histogram(aes(x=N,     
                     fill=Type  
  ) 
  )     

library(lattice)
histogram(x= ~ N | Type,  # 根據月份(Month)的條件，繪製臭氧(Ozone)的直方圖
          data=callcount,     
          xlab="來電客服次數",  
          layout=c(2,1))  

#打1246的瘋子都問什麼問題
s <- oldget_call %>% filter(PID == '817e16c5c55953a382a7194717005b7a') %>% group_by(business_desc)
summary(s)





# #import cctxn 
# #將CSV所放的資料夾路徑設為path
# path = "F:/hackathon-encoded/cctxn/partition_time=3696969600/"
# #path中的所有CSV檔變為list指定為files
# files <- list.files(path=path, pattern="*.csv")
# a <- function(x) read.csv(x,quote = "")
# #將path及file合併，並使用a的function
# cctxn = lapply(paste(path,files,sep=""), a)
# head(cctxn)
# 
# colnames(cctxn[[1]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
# colnames(cctxn[[2]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
# colnames(cctxn[[3]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
# colnames(cctxn[[4]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
# colnames(cctxn[[5]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
# colnames(cctxn[[6]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
# colnames(cctxn[[7]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
# colnames(cctxn[[8]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
# colnames(cctxn[[9]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17')
# 
# cctxn = do.call(rbind,cctxn)
# 
# cctxn <- cctxn[,-c(1,5,7,16,17)]
# head(cctxn)
# str(cctxn)
# cctxn$PID = as.character(cctxn$PID)
# cctxn$txn_amt = as.character(cctxn$txn_amt)
# cctxn$original_currency = as.character(cctxn$original_currency)
# cctxn$c_category_desc = as.character(cctxn$c_category_desc)
# cctxn$mcc_desc = as.character(cctxn$mcc_desc)
# cctxn$mcc_code = as.character(cctxn$mcc_code)
# cctxn$card_type = as.character(cctxn$card_type)
# cctxn$card_level = as.character(cctxn$card_level)
# str(cctxn)
# head(cctxn)
# install.packages("stringr")
# library(stringr)
# 
# #s_amt = str_split_fixed(cctxn[,6],":",n=3)
# txn_amt = str_extract_all(cctxn$txn_amt, "[0-9]+\\.[0-9]+", simplify = TRUE)
# head(txn_amt)
# cctxn$txn_amt <- txn_amt
# head(cctxn$txn_amt)
# 
# original_currency = str_split_fixed(cctxn$original_currency,"\\\\\"",n=5)
# head(original_currency)
# cctxn$original_currency <- original_currency[,4]
# head(cctxn$original_currency)
# 
# c_category_desc = str_split_fixed(cctxn$c_category_desc,"\\\\\"",n=7)
# head(c_category_desc)
# cctxn$c_category_desc <- c_category_desc[,6]
# head(cctxn$c_category_desc)
# 
# s_mccdesc = str_split_fixed(cctxn$mcc_desc,"\\\\\\\\",n=10)
# head(s_mccdesc)
# 
# 
# s_mcccode = str_split_fixed(cctxn$mcc_code,"\\\\\"",n=5)
# head(s_mcccode)
# cctxn$mcc_code <- s_mcccode[,4]
# head(cctxn$mcc_code)
# 
# s_cardtype = str_split_fixed(cctxn$card_type,"\\\\\"",n=7)
# head(s_cardtype)
# cctxn$card_type <- s_cardtype[,6]
# head(cctxn$card_type)
# 
# s_cardlevel = str_split_fixed(cctxn$card_level,"\\\\\"",n=5)
# head(s_cardlevel)
# cctxn$card_level <- s_cardlevel[,4]
# head(cctxn$card_level)
# summary(cctxn)
# str(cctxn)
# 
# 
# #轉換型態
# options(scipen=999)
# cctxn$txn_amt <- as.numeric(cctxn$txn_amt)
# cctxn$mcc_code <- as.factor(cctxn$mcc_code)
# cctxn$original_currency <- as.factor(cctxn$original_currency)
# cctxn$c_category_desc <- as.factor(cctxn$c_category_desc)
# cctxn$card_type <- as.factor(cctxn$card_type)
# cctxn$card_level <- as.factor(cctxn$card_level)
# str(cctxn)
# #清洗cctxn資料完成
# 
# 
# #cctxn與profile合併
# data = read.csv("F:/hackathon-encoded/profile/partition_time=3696969600/part-00000-74cda679-a63c-4af7-8c30-cf0a24a44e8f.csv",header = F)
# colnames(data) = c('PID','Birthday','Gender','Comm_Area','Comm_Areacode','House_Area','House_AreaCode','BOA','Rev','Date_Arrival','Credit_Card','Loan','Saving','FM','Data_Date')
# library(dplyr)
# install.packages("arules")
# library(arules)
# install.packages("arulesViz")
# library(arulesViz)
# mydata <- merge(x = cctxn, y = data, by = "PID", all.x = TRUE)
# str(mydata)
# 
# #profile_card + cti
# sqldf("SELECT CustomerId, Product, State 
#         FROM df1
#       LEFT JOIN df2 USING(CustomerID)")
