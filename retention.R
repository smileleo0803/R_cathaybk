
# 將信用卡(cctxn)partition_time=3696969600所有CSV合併
# 比對Profile中partition_time=3696969600的table


#import cctxn 
#將CSV所放的資料夾路徑設為path
path = "F:/hackathon-encoded/cctxn/partition_time=3696969600/"
#path中的所有CSV檔變為list指定為files
files <- list.files(path=path, pattern="*.csv")
a <- function(x) read.csv(x,quote = "")
#將path及file合併，並使用a的function
cctxn = lapply(paste(path,files,sep=""), a)
head(cctxn)


# for(i in length(cctxn)){
#   colnames(cctxn[[i]]) <- c('客戶id','PID','action_type','action_time','店家編號','merchant_nbr','信用卡','card_id','txn_amt','original_currency','c_category_desc','mcc_desc','mcc_code','card_type','card_level','16','17');
# }

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


#把時間轉換還原
library(anytime)
cctxn$action_time <- as.Date(anytime(cctxn$action_time))
str(cctxn$action_time)

summary(cctxn)
str(cctxn)

#轉換型態
cctxn$txn_amt <- as.numeric(cctxn$txn_amt)
cctxn$mcc_code <- as.numeric(cctxn$mcc_code)
cctxn$card_level <- as.numeric(cctxn$card_level)
cctxn$original_currency <- as.factor(cctxn$original_currency)
cctxn$c_category_desc <- as.factor(cctxn$c_category_desc)
cctxn$card_type <- as.factor(cctxn$card_type)
cctxn$card_level <- as.factor(cctxn$card_level)
str(cctxn)


##信用卡交易金額敘述統計值與boxplot 檢查分佈與極端值
options(scipen=999)
summary(cctxn$txn_amt)
range(cctxn$txn_amt)
quantile(cctxn$txn_amt)
IQR(cctxn$txn_amt)
sd(cctxn$txn_amt)
var(cctxn$txn_amt)
library(moments)
skewness(cctxn$txn_amt)  #偏度(偏態值 < 0，為負偏態，分配集中在平均數以上，高分群的個體較多)
kurtosis(cctxn$txn_amt)  #峰度(偏態值 < 0，為負偏態，分配集中在平均數以上，高分群的個體較多)
hist(cctxn$txn_amt,breaks=70,main="交易金額分配圖",xlab="交易金額")
boxplot(cctxn$txn_amt,horizontal = T,col='#f8f3c4')
title("信用卡消費金額箱型圖")


boxplot(formula = cctxn$txn_amt~cctxn$card_type,data=cctxn,xlab="卡種",ylab="交易金額",col="#f8f3c4")
title("交易金額與卡種箱型圖")

boxplot(formula = cctxn$txn_amt~cctxn$card_level,data=cctxn,xlab="卡片等級",ylab="交易金額",col="#f8f3c4")
title("交易金額與卡片等級箱型圖")


library(ggplot2)
##散佈圖
#交易金額-卡片種類散布圖
qplot(x=txn_amt,                               
      y=card_type,                              
      data=cctxn,                      
      geom="point",                         # 圖形=scatter plot
      main = "交易金額-卡片種類散布圖",  
      xlab="交易金額",                          
      ylab="卡片種類",                    
      color= card_type                          # 以顏色標註card_type，複合式的散布圖
)

#交易金額-卡片等級散布圖
qplot(x=txn_amt,                               
      y=card_level,                              
      data=cctxn,                      
      geom="point",                         # 圖形=scatter plot
      main = "交易金額-卡片等級散布圖",  
      xlab="交易金額",                          
      ylab="卡片等級",                    
      color= card_level                          # 以顏色標註card_level，複合式的散布圖
)

##合鬚圖(boxplot)
#交易金額-卡片種類合鬚圖
qplot(x=card_type,                               
      y=txn_amt,
      data=cctxn,                     
      geom="boxplot",
      main = "交易金額-卡片種類合鬚圖",
      xlab="card_type",                          
      color= card_type          # 以顏色標註card_type，複合式的合鬚圖
)

#交易金額-卡片等級合鬚圖
qplot(x=card_level,
      y=txn_amt,                               
      data=cctxn,                     
      geom="boxplot",
      main = "交易金額-卡片等級合鬚圖",
      xlab="card_level",                          
      color= card_level
)

boxplot(cctxn$txn_amt,ylab="amount")
Amount <- ggplot(data=cctxn,aes(x=txn_amt))
Amount + geom_histogram(color="black",aes(fill=card_type)) 
Amount + geom_histogram(color="black",aes(fill=card_type)) +facet_grid(card_type~.,scale="free")

box <- ggplot(data=cctxn,aes(x=card_type,y=txn_amt,color=card_type))

box + geom_jitter() + geom_boxplot(alpha=0.5)
?geom_jitter(size=0.)
Amount + geom_density()
Amount + geom_boxplot(aes(x=action_type,y=txn_amt)) + geom_jitter(aes(x=action_type,y=txn_amt))

?geom_boxplot
Cardtype <- unique(unlist(cctxn$card_type))


r <- ggplot(data=cctxn,aes(x=txn_amt))
r + geom_histogram()

##查信用卡睡卡比例
data = read.csv("F:/hackathon-encoded/profile/partition_time=3696969600/part-00000-74cda679-a63c-4af7-8c30-cf0a24a44e8f.csv",header = F)
colnames(data) = c('PID','Birthday','Gender','Comm_Area','Comm_Areacode','House_Area','House_AreaCode','BOA','Rev','Date_Arrival','Credit_Card','Loan','Saving','FM','Data_Date')
data$PID = as.character(data$PID)
str(data)
library(dplyr)

data_cctxn <- inner_join(cctxn,data)
head(data_cctxn)
nrow(data_cctxn)

Group <- cctxn %>% group_by(PID) %>% summarise(comsumpsionN=n(),totalAmt=sum(txn_amt))

Group <- Cathay.cctxn %>% group_by(PID) %>% summarise(comsumpsionN=n(),totalAmt=sum(txn_amt),card_type)
head(Group)
summary(Group)

nrow(Group)  #信用卡消費客戶數
nrow(data)   #總客戶數
nrow(data)-nrow(Group)  #睡卡數
round((nrow(data)-nrow(Group))/nrow(data)*100,2)  #睡卡比例



##信用卡交易次數敘述統計值與boxplot 檢查分佈與極端值
summary(Group$comsumpsionN)
str(Group)


##刷卡次數與交易金額散佈圖
plot(x=Group$comsumpsionN,      
     y=Group$totalAmt,      
     main="刷卡次數與交易金額散佈圖",    # 圖片名稱
     xlab="刷卡次數",                          
     ylab="交易金額",
     col="steelblue"
)
lm_cNTotalAmt.model <- lm(totalAmt~comsumpsionN, Group)    # 建立一個線性回歸
# 畫上回歸的趨勢線
abline(lm_cNTotalAmt.model,                          
       lwd=2,
       col="red"
)     # lwd 代表線的粗細
p <- qplot(comsumpsionN, totalAmt, data=Group,
           geom=c("point","smooth"),color= "steelblue", method="lm",color="blue")
p
#刷卡次數與交易金額散佈圖
qplot(Group$comsumpsionN, geom="histogram") 
q <- ggplot(data=Group,aes(x=comsumpsionN))
q + geom_histogram(binwidth=2, bins =0.1)
q + geom_point(aes(y=totalAmt), color= "steelblue" )+abline(lsfit(Group$comsumpsionN,Group$totalAmt))
q + abline(lsfit(Group$comsumpsionN,Group$totalAmt))

qplot(x=comsumpsionN,                               
      y=totalAmt,                              
      data=Group,                      
      geom="point",                         # 圖形=scatter plot
      main = "刷卡次數與交易金額散佈圖",  
      xlab="刷卡次數",                          
      ylab="交易金額",                   
      color= "steelblue"                          # 以顏色標註月份，複合式的散布圖
)

#交易次數直方圖
qplot(Group$comsumpsionN,
     geom="histogram",
     binwidth = 0.5,  
     main = "交易次數直方圖", 
     xlab = "comsumpsionN",  
     fill=I("blue"),
     col=I("black"),
     alpha=I(.2),
     xlim=c(0,30),
     )

#信用卡交易金額與交易手續費點散佈圖
ijgd <- inner_join(Group,data)
head(ijgd)
str(ijgd)
summary(ijgd)

plot(x=ijgd$Rev,      
     y=ijgd$totalAmt,      
     main="信用卡交易金額與交易手續費點散佈圖",    # 圖片名稱
     xlab="交易手續費",                          
     ylab="交易金額",
     col="steelblue"
)
lm_RevTotalAmt.model <- lm(Rev~totalAmt, ijgd)    # 建立一個線性回歸
# 畫上回歸的趨勢線
abline(lm_RevTotalAmt.model,                          
       lwd=2,
       col="red"
       )     # lwd 代表線的粗細

#信用卡交易金額與交易手續費點散佈圖+第三維變數
qplot(x=Rev,                               
      y=totalAmt,                              
      data=ijgd,                      
      geom="point",                         # 圖形=scatter plot
      main = "信用卡交易金額與交易手續費點散佈圖",  
      xlab="交易手續費",                          
      ylab="交易金額",                   
      color= Credit_Card                         
)



#信用卡交易次數與交易手續費點散佈圖
plot(x=ijgd$Rev,      
     y=ijgd$comsumpsionN,      
     main="信用卡交易次數與交易手續費點散佈圖",    # 圖片名稱
     xlab="交易手續費",                          
     ylab="交易次數",
     col="steelblue"
)
lm_RevComsumpsionN.model <- lm(Rev~comsumpsionN, ijgd)    # 建立一個線性回歸
# 畫上回歸的趨勢線
abline(lm_RevComsumpsionN.model,                          
       lwd=2,
       col="red"
)     # lwd 代表線的粗細

#信用卡交易金額與交易手續費點散佈圖+第三維變數
qplot(x=Rev,                               
      y=comsumpsionN,                              
      data=ijgd,                      
      geom="point",                         # 圖形=scatter plot
      main = "信用卡交易次數與交易手續費點散佈圖",  
      xlab="交易手續費",                          
      ylab="交易次數",                   
      color= totalAmt                       
)

##信用卡消費分類比例（consumption_category_desc）比例，與各組別平均刷卡次數，與平均刷卡金額。用倍數解讀意義。

#a = sqldf('select AVG(Count(c_category_desc)) , AVG(txn_amt) from cctxn group by c_category_desc')
category <- cctxn %>% group_by(c_category_desc) %>% summarise(N=n(),avg = mean(txn_amt))
category %>% arrange(desc(N))
a <- round(category[,2]/(nrow(cctxn)/26)*100,2)
category <- cbind(category,a)
colnames(category)<-c("c_category_desc","N","avg","percent")
category
#信用卡消費店家型態（object type)比例，與各組別平均刷卡次數，與平均刷卡金額  #數量太多
merchant <- cctxn %>% group_by(merchant_nbr) %>% summarise(N=n(),avg = mean(txn_amt))
merchant %>% arrange(desc(N))
b <- round(merchant[,2]/(nrow(cctxn)/39901)*100,2)
merchant <- cbind(merchant,b)
colnames(merchant)<-c("merchant","N","avg","percent")
merchant
#信用卡種類（card type）比例， 與各組別平均刷卡次數，與平均刷卡金額。
cardtype <- cctxn %>% group_by(card_type) %>% summarise(N=n(),avg = mean(txn_amt))
cardtype %>% arrange(desc(N))
b <- round(cardtype[,2]/(nrow(cctxn)/11)*100,2)
cardtype <- cbind(cardtype,b)
colnames(cardtype)<-c("cardtype","N","avg","percent")
cardtype

#信用卡等級（card level）比例， 與各組別平均刷卡次數，與平均刷卡金額。
cardlevel <- cctxn %>% group_by(card_level) %>% summarise(N=n(),avg = mean(txn_amt))
cardlevel %>% arrange(desc(N))
c <- round(cardlevel[,2]/(nrow(cctxn)/7)*100,2)
cardlevel <- cbind(cardlevel,c)
colnames(cardlevel)<-c("cardlevel","N","avg","percent")
cardlevel


#qqplot2
q <- ggplot(data=Group,aes(x=comsumpsionN,y=totalAmt))

q + geom_point(color='steelblue') + geom_smooth()
q + geom_histogram(aes(x=comsumpsionN))

box <- ggplot(data=cctxn,aes(x=card_type,y=txn_amt,color=card_type))

box + geom_jitter() + geom_boxplot(alpha=1)
?geom_jitter(size=0.)
Amount + geom_density()
Amount + geom_boxplot(aes(x=action_type,y=txn_amt)) + geom_jitter(aes(x=action_type,y=txn_amt))

