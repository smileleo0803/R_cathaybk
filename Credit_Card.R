#連接資料庫
install.packages("RJDBC")
install.packages("rJava")
install.packages("DBI")
library(RJDBC)
library(rJava)
library(DBI)
drv<-JDBC("com.mysql.jdbc.Driver","E:/JDBC/mysql-connector-java-5.1.42/mysql-connector-java-5.1.42-bin.jar","")
conn <- dbConnect(drv,"jdbc:mysql://localhost:3306/cathay","root","kk534820")
dbListTables(conn)
dbGetQuery(conn,"select * from city")

dbDisconnect(conn)
dbWriteTable(conn,"use_Card",use_Card)
dbWriteTable(conn,profile_rbind,row.names = FALSE, overwrite = FALSE, append = TRUE) on.exit(dbDisconnect(conn))

#轉CSV檔
write.csv(profile_rbind, "profile_rbind.csv", row.names = FALSE)

##############################################################################################################################

#import profile
path = "F:/hackathon-encoded/profile"
dirs <- list.dirs(path=path)
dirs <- dirs[-1]
files <- list.files(path=dirs,pattern="*.csv")
a <- function(x) read.csv(x,quote = "",header=F)
myfiles = lapply(paste(dirs,"/",files,sep=""), a)

for(i in c(1:13)){
  colnames(myfiles[[i]]) <- c('PID','Birthday','Gender','Comm_Area','Comm_Areacode','House_Area','House_AreaCode','Date_Arrival','BOA','Rev','Credit_Card','Loan','Saving','FM','Data_Date')
}
profile_rbind = do.call(rbind,myfiles)
head(profile_rbind)
str(profile_rbind)


library(anytime)
profile_rbind$Birthday <- as.Date(anytime(profile_rbind$Birthday))
profile_rbind$Date_Arrival <- as.Date(anytime(profile_rbind$Date_Arrival))
profile_rbind$Data_Date <- as.Date(anytime(profile_rbind$Data_Date))
str(profile_rbind)



install.packages("reshape")
library(reshape)
profile_rbind.pivot_Card <-profile_rbind[,c("PID","Data_Date","Credit_Card")]
#做個(pv,condition,value)
use_card <- cast(profile_rbind.pivot_Card,PID~Data_Date)#use_card僅有PID及各期是否有辦卡
head(use_card)
#做沒卡的欄位
library(sapply)
library(apply)
a <- sapply(use_card,function(x){x=="N"})
a <- data.frame(a)
k = apply(a,1,sum)
use_card<-cbind(use_card,k)
colnames(use_card)[15]= 'N_of_NoCard'
str(use_card)

#做個profile完整表格(含各期是否辦卡)→profile_card
profile1 = read.csv("F:/hackathon-encoded/profile/partition_time=3696969600/1.csv",header = F)
profile13 = read.csv("F:/hackathon-encoded/profile/partition_time=3728592000/13.csv",header = F)

colnames(profile1) = c('PID','Birthday','Gender','Comm_Area','Comm_Areacode','House_Area','House_AreaCode','Date_Arrival','BOA','Rev','Credit_Card','Loan','Saving','FM','Data_Date')
basic_profile <- profile1[,1:3]
profile_card = merge(x = basic_profile , y = use_card , by = "PID", all = TRUE)
head(profile_card)

x = merge(profile_card , profile_Loan , by=c("PID","Birthday",'Gender'), all = TRUE)
y = merge(profile_Saving , profile_FM , by=c("PID","Birthday",'Gender'), all = TRUE)
z = merge(x , y , by=c("PID","Birthday",'Gender'), all = TRUE)
head(z)
colnames(z)= c('PID','Birthday','Gender','2087-02-25.c','2087-03-26.c','2087-04-26.c','2087-05-26.c','2087-06-26.c',
                 '2087-07-26.c','2087-08-26.c','2087-09-26.c','2087-10-26.c','2087-11-26.c','2087-12-26.c','2088-01-26.c',
                 '2088-02-26.c','N_of_NoCard','2087-02-25.l','2087-03-26.l','2087-04-26.l','2087-05-26.l','2087-06-26.l',
               '2087-07-26.l','2087-08-26.l','2087-09-26.l','2087-10-26.l','2087-11-26.l','2087-12-26.l','2088-01-26.l',
               '2088-02-26.l','N_of_NoLoan','2087-02-25.s','2087-03-26.s','2087-04-26.s','2087-05-26.s','2087-06-26.s',
               '2087-07-26.s','2087-08-26.s','2087-09-26.s','2087-10-26.s','2087-11-26.s','2087-12-26.s','2088-01-26.s',
               '2088-02-26.s','N_of_NoSaving','2087-02-25.f','2087-03-26.f','2087-04-26.f','2087-05-26.f','2087-06-26.f',
               '2087-07-26.f','2087-08-26.f','2087-09-26.f','2087-10-26.f','2087-11-26.f','2087-12-26.f','2088-01-26.f',
               '2088-02-26.f','N_of_NoFM')
profile_use = z
#############################################################################################################################
#做個(pv,condition,value)
profile_rbind.pivot_Loan <-profile_rbind[,c("PID","Data_Date","Loan")]
use_Loan <- cast(profile_rbind.pivot_Loan,PID~Data_Date)
head(use_Loan)
#做沒卡的欄位
a <- sapply(use_Loan,function(x){x=="N"})
a <- data.frame(a)
k = apply(a,1,sum)
use_Loan<-cbind(use_Loan,k)
colnames(use_Loan)[15]= 'N_of_NoLoan'
str(use_Loan)
#做個profile完整表格(含各期是否貸款)→profile_Loan
profile_Loan = merge(x = basic_profile , y = use_Loan , by = "PID", all = TRUE)
head(profile_Loan)


#做個(pv,condition,value)
profile_rbind.pivot_Saving <-profile_rbind[,c("PID","Data_Date","Saving")]
use_Saving <- cast(profile_rbind.pivot_Saving,PID~Data_Date)
head(use_Saving)
#做沒卡的欄位
a <- sapply(use_Saving,function(x){x=="N"})
a <- data.frame(a)
k = apply(a,1,sum)
use_Saving<-cbind(use_Saving,k)
colnames(use_Saving)[15]= 'N_of_NoSaving'
str(use_Saving)
#做個profile完整表格(含各期是否貸款)→profile_Saving
profile_Saving = merge(x = basic_profile , y = use_Saving , by = "PID", all = TRUE)
head(profile_Saving)


#做個(pv,condition,value)
profile_rbind.pivot_FM <-profile_rbind[,c("PID","Data_Date","FM")]
use_FM <- cast(profile_rbind.pivot_FM,PID~Data_Date)
head(use_FM)
#做沒卡的欄位
a <- sapply(use_FM,function(x){x=="N"})
a <- data.frame(a)
k = apply(a,1,sum)
use_FM<-cbind(use_FM,k)
colnames(use_FM)[15]= 'N_of_NoFM'
str(use_FM)
#做個profile完整表格(含各期是否貸款)→profile_Saving
profile_FM = merge(x = basic_profile , y = use_FM , by = "PID", all = TRUE)
head(profile_FM)

###############################################################################################################################


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
  colnames(myfiles2[[i]]) <- c('1','PID','3','inbound_time','5',
                               'call_purpose','7','8','call_nbr',
                               'end_call_date','calltype_desc',
                               'detail_desc','business_desc','14','15')
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
cti$inbound_time <- as.POSIXct(cti$inbound_time,origin = "1970-01-01")
head(cti$inbound_time)

cti$end_call_date = str_extract_all(cti$end_call_date, "[0-9]+", simplify = TRUE)
head(cti$end_call_date)
str(cti$end_call_date)
cti$end_call_date <- as.POSIXct(as.numeric(cti$end_call_date),origin = "1970-01-01")

call_length = difftime(cti$end_call_date,cti$inbound_time, units="secs")
cti <- cbind(cti,call_length)
str(cti)


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
cti$call_nbr <- as.factor(cti$call_nbr)
cti$calltype_desc <- as.factor(cti$calltype_desc)
cti$detail_desc <- as.factor(cti$detail_desc)
cti$business_desc <- as.factor(cti$business_desc)
cti$call_purpose <- as.factor(cti$call_purpose)
cti$calltime <- as.numeric(cti$call_length)
head(cti)
str(cti)


#剪卡客戶打客服之記錄
library(sqldf)
cut_call <- sqldf("
      select `PID`, call_purpose, business_desc,calltype_desc,detail_desc,inbound_time ,end_call_date ,calltime
      from cti
      where `PID` IN (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'N')
      order by PID
      ")
cut_call$call_purpose <- as.factor(cut_call$call_purpose)
summary(cut_call)


oldget_call <- sqldf("
      select `PID`, call_purpose, business_desc,calltype_desc,detail_desc,inbound_time ,end_call_date ,calltime
                     from cti
                     where `PID` IN (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'Y')
                     order by PID
                     ")
oldget_call$call_purpose <- as.factor(oldget_call$call_purpose)
summary(oldget_call)



#合併客服原因
c <- rep(1,23388)
c <- cbind(cut_call,c)
colnames(c)[9]= 'Type'
cut_call <- c
head(cut_call)


c <- rep(0,1447047)
c<- cbind(oldget_call,c)
colnames(c)[9]= 'Type'
oldget_call <- c
head(oldget_call)
call <- rbind(cut_call,oldget_call)
call$Type <- as.factor(call$Type) 
summary(call)
str(call)


##打客服次數
library(dplyr)
#死的平均每人打電話次數  #8.623894
nrow(cut_call)/filter(use_card,`2087-02-25` =="Y" & `2088-02-26` =="N") %>% summarise(number=n())
#活的平均每人打電話次數  #6.68348
nrow(oldget_call)/filter(use_card,`2087-02-25` =="Y" & `2088-02-26` =="Y") %>% summarise(number=n())

#死的來電次數   23388
nrow(cut_call)
#活的來電次數   1447047
nrow(oldget_call)
#死的來電人數   2634
a<- sqldf("select distinct(PID) from cut_call")
nrow(a)
#活的來電人數   2634
a<- sqldf("select distinct(PID) from oldget_call")
nrow(a)

#死的總通時  4889739
a <- sqldf("select sum(calltime) from cti 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'N') 
           ")
a
#活的總通時  278566119
a <- sqldf("select sum(calltime) from cti 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'Y') 
           ")


#剪卡-客服原因直方圖
library(ggplot2)
canvas <- ggplot(data=cut_call)
canvas +
  geom_bar(aes(x=business_desc),fill = "steelblue")   

a <- sqldf("select business_desc,count(PID) from cut_call group by business_desc ")
a


#call_purpose次數分配
a <- cut_call %>% group_by(call_purpose) %>% summarise(N=n()) %>% arrange(desc(N))
canvas <- ggplot(data= a[1:10,])
canvas +
  geom_bar(aes(x=call_purpose,
               y=N),stat = "identity", 
           fill = "steelblue")

a <- sqldf("select call_purpose,count(PID) from cut_call group by call_purpose order by count(PID) desc ")
a
row(cut_call)
b <- sqldf("select call_purpose,count(PID) from oldget_call group by call_purpose order by count(PID) desc ")
b
row(b)
c <- merge(a, b, by=c("call_purpose"), all=TRUE) 
c
write.csv(c, "c.csv", row.names = FALSE)

#calltype_desc次數分配
a <- cut_call %>% group_by(calltype_desc) %>% summarise(N=n()) %>% arrange(desc(N))
canvas <- ggplot(data= a[1:10,])
canvas +
  geom_bar(aes(x=calltype_desc,
               y=N),stat = "identity", 
           fill = "steelblue")

a <- sqldf("select calltype_desc,count(PID) from cut_call group by calltype_desc order by count(PID) desc ")
a

b <- sqldf("select calltype_desc,count(PID) from oldget_call group by calltype_desc order by count(PID) desc ")
b

c <- merge(a, b, by=c("calltype_desc"), all=TRUE) 
c
write.csv(c, "c.csv", row.names = FALSE)


#detail_desc次數分配
a <- cut_call %>% group_by(detail_desc) %>% summarise(N=n()) %>% arrange(desc(N))
canvas <- ggplot(data= a[1:10,])
canvas +
  geom_bar(aes(x=detail_desc,
               y=N),stat = "identity", 
           fill = "steelblue")

a <- sqldf("select detail_desc,count(PID) from cut_call group by detail_desc order by count(PID) desc ")
a

b <- sqldf("select detail_desc,count(PID) from oldget_call group by detail_desc order by count(PID) desc ")
b

c <- merge(a, b, by=c("detail_desc"), all=TRUE) 
c
write.csv(c, "c.csv", row.names = FALSE)



#老卡友-客服原因直方圖
canvas <- ggplot(data=oldget_call)
canvas +
  geom_density(aes(x=business_desc),fill = "steelblue")

a <- sqldf("select business_desc,count(PID) from oldget_call group by business_desc order by count(PID) desc")
a

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



#合併客服原因直方圖
canvas <- ggplot(data=call)
canvas +
  geom_bar(aes(x=business_desc,
           fill = Type))


#合併通時直方圖
canvas <- ggplot(data=call)
canvas +geom_density(aes(x=calltime,fill = Type))+ scale_x_continuous(limits = c(0,2500))

ggplot(data=call)+geom_density(aes(x=calltime,colour = Type))#+ scale_x_continuous(limits = c(0,2500))


qplot(x=calltime,                             
      data=call,                     
      geom="density",        # 圖形=density
      xlab="通話時間(秒)",
      xlim=c(0,700),
      color= Type           # 以顏色標註月份，複合式的機率密度圖
)

#查最長通時
sqldf("
      select `PID`, call_purpose, business_desc,calltype_desc,detail_desc,inbound_time ,end_call_date ,calltime,max(calltime)
      from call
      ")

#通時盒鬚圖
qplot(x=Type,                               
      y=calltime,
      data=call,                     
      geom="boxplot",       
      xlab="活的(0) VS 死的(1)",
      ylim=c(0,800),
      color= Type         
)


d <- sqldf("
      select `PID`, inbound_time ,end_call_date ,calltime
      from call
      where Type = 1
      ")
summary(d)





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
cctxn <- cctxn[,-c(9)]
str(cctxn)
#清洗cctxn資料完成



#前10大mcc_code分配
a <- sqldf("select mcc_code,count(PID) as count ,count(PID)/601925
      from cctxn 
      group by mcc_code
      order by count(PID) desc
      limit 10 ")


#查死活刷卡數
#死的刷卡數4583，活的刷卡數595346
d <- sqldf("select PID from cctxn 
      where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'Y')")
d


a <- sqldf("select mcc_code,count(PID) from cctxn 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'N') 
           group by mcc_code order by count(PID) desc ")
a

b <- sqldf("select mcc_code,count(PID) from cctxn 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'Y') 
           group by mcc_code order by count(PID) desc ")
b

c <- merge(a, b, by=c("mcc_code"), all=TRUE) 
c
write.csv(c, "c.csv", row.names = FALSE)




#card_type分配
#死的卡片張數4583，活的卡片張數595346
a <- sqldf("select PID, card_type from cctxn group by PID")
sqldf("select card_type,count(PID) from a group by card_type order by count(PID) desc")
nrow(a)

a <- sqldf("select card_type,count(PID) from cctxn 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'N') 
           group by card_type order by count(PID) desc ")
a

b <- sqldf("select card_type,count(PID) from cctxn 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'Y') 
           group by card_type order by count(PID) desc ")
b

c <- merge(a, b, by=c("card_type"), all=TRUE) 
c
write.csv(c, "c.csv", row.names = FALSE)



#card_level分配
a <- sqldf("select PID, card_level from cctxn group by PID")
sqldf("select card_level ,count(PID) from a group by card_level order by count(PID) desc")
nrow(a)

a <- sqldf("select card_level,count(PID) from cctxn 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'N') 
           group by card_level order by count(PID) desc ")
a

b <- sqldf("select card_level,count(PID) from cctxn 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'Y') 
           group by card_level order by count(PID) desc ")
b

c <- merge(a, b, by=c("card_level"), all=TRUE) 
c
write.csv(c, "c.csv", row.names = FALSE)




#cctxn、cti與profile合併

two <- merge(x = profile_use, y = call, by = "PID", all = TRUE)
head(two)
str(two)

total_data <-merge(x = two, y = cctxn, by = "PID", all = TRUE)
head(total_data)
str(total_data)

#型態轉換
total_data$Birthday <- as.Date(anytime(total_data$Birthday))
total_data$Data_Date<- as.Date(anytime(total_data$Data_Date))
total_data$action_time<- as.Date(anytime(total_data$action_time))
#total_data$action_time <- as.POSIXct(total_data$action_time,origin = "1970-01-01")
total_data$Month<- as.factor(total_data$Data_Date)
str(total_data)
summary(total_data$action_time)



##死的貸款比例
a<- sqldf("select PID , `2087-02-25.l` from total_data 
          where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y') and `2087-02-25.l` == 'Y'
          group by PID")
nrow(a) 
#死的貸款人數427、活的貸款人數22220
#活的存款人數2650、活的存款人數190447
#活的理財人數198、活的理財人數23529


##死的前10大mcc_code比例
a<- sqldf("select mcc_code , count(PID) from total_data 
          where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
          group by mcc_code order by count(PID) desc limit 10")
a
#死的消費次數61805  活的消費次數5795079


##死的card_type比例
a<- sqldf("select card_type , count(PID) from total_data 
          where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
          group by card_type order by count(PID) desc ")
a

##死的card_level比例
a<- sqldf("select card_level , count(PID) from total_data 
          where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
          group by card_level order by count(PID) desc ")
a

##########################################################################################################################################
cctxn$action_time.Day <- as.Date(anytime(cctxn$action_time))

qplot(x=action,                      
      data=airquality,              
      geom="histogram",             # 圖形=histogram
      main = "Histogram of Ozone",  
      xlab="Ozone(ppb)",            

    
)
qplot(x=action_time.Day,                             
      data=cctxn,                     
      geom="histogram",      
      xlab="action time",
      binwidth = 25
)


qplot(x=action_time,                             
      data=cctxn,                     
      geom="density",        
      xlab="action time"
)


a <- sqldf("select action_time , sum(txn_amt) from cctxn
       group by action_time
              ")
p <- ggplot(data=a, aes(x=action_time, y=`sum(txn_amt)`)) +
  geom_bar(stat="identity")

g<- ggplot(data=a)
g + geom_bar(stat = "identity",
             x= action_time,
             y= `sum(txn_amt)`,
             xlab="action_time",
             ylab="sum(txn_amt)"
             )

str(cctxn)




##########################################################################################################################################
##四大業務比例(死VS活)
r <- sqldf("select PID from profile_use where `2087-02-25.c`== 'Y' and `2088-02-26.c`== 'Y' and `2087-02-25.f`== 'N' ")
str(r)
r <- sqldf("select PID from profile_use where `2087-02-25.c`== 'Y' and `2088-02-26.c`== 'N' and `2087-02-25.l`== 'N' ")
str(r)


##性別(死的VS活的)
a<- sqldf("select Gender , count(PID)  
       from profile1
       where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
       group by Gender order by count(PID) desc")
a
#死的女生1571，死的男生1141,總人數2712
#活的女生133454，活的男生83057,總人數216511

#####################################################################################################

##年齡(死的VS活的)

summary(profile1$Brithday)
#     Min.    1st Qu.     Median       Mean    3rd Qu.       Max. 
#411000000 2120000000 2420000000 2370000000 2640000000 3660000000 

profile1_age_G1 <- filter(profile1, 411000000 <= Birthday & Birthday< 2120000000)
profile1_age_G2 <- filter(profile1, 2120000000 <= Birthday & Birthday< 2420000000)
profile1_age_G3 <- filter(profile1, 2420000000 <= Birthday & Birthday< 2640000000)
profile1_age_G4 <- filter(profile1, 2640000000 <= Birthday & Birthday< 3660000000)

profile1_age_G1$Birthday_Day = as.Date(anytime(profile1_age_G1$Birthday))
profile1_age_G2$Birthday_Day = as.Date(anytime(profile1_age_G2$Birthday))
profile1_age_G3$Birthday_Day = as.Date(anytime(profile1_age_G3$Birthday))
profile1_age_G4$Birthday_Day = as.Date(anytime(profile1_age_G4$Birthday))

as.Date(anytime(411000000)) #"1983-01-09"
as.Date(anytime(2120000000)) #"2037-03-07"
as.Date(anytime(2420000000)) #"2046-09-08"
as.Date(anytime(2640000000)) #"2053-08-28"
as.Date(anytime(3660000000)) #"2085-12-24"

# a11:活著老的、a12死著老的
# a21:活著中的、a22死著中的
# a31:活著青的、a32死著青的
# a41:活著少的、a42死著少的

a11<- sqldf("select PID, Birthday_Day from profile1_age_G1
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
      order by Birthday_Day")
nrow(a11)
type <- rep(11,53928)
a11 <- cbind(a11,type)

a12<- sqldf("select PID, Birthday_Day from profile1_age_G1
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
      order by Birthday_Day")
nrow(a12)
type <- rep(12,653)
a12 <- cbind(a12,type)

a21<- sqldf("select PID, Birthday_Day from profile1_age_G2
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
      order by Birthday_Day")
nrow(a21)
type <- rep(21,55544)
a21 <- cbind(a21,type)

a22<- sqldf("select PID, Birthday_Day from profile1_age_G2
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
      order by Birthday_Day")
nrow(a22)
type <- rep(22,658)
a22 <- cbind(a22,type)

a31<- sqldf("select PID, Birthday_Day from profile1_age_G3
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
      order by Birthday_Day")
nrow(a31)
type <- rep(31,52510)
a31 <- cbind(a31,type)

a32<- sqldf("select PID, Birthday_Day from profile1_age_G3
            where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
            order by Birthday_Day")
nrow(a32)
type <- rep(32,694)
a32 <- cbind(a32,type)

a41<- sqldf("select PID, Birthday_Day from profile1_age_G4
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
      order by Birthday_Day")
nrow(a41)
type <- rep(41,54528)
a41 <- cbind(a41,type)+707

a42<- sqldf("select PID, Birthday_Day from profile1_age_G4
            where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
            order by Birthday_Day")
nrow(a42)
type <- rep(42,707)
a42 <- cbind(a42,type)

#合併為age_Card.YN
e <- rbind(a11,a12)
a <- rbind(a21,a22)
b <- rbind(a31,a32)
c <- rbind(a41,a42)
d <- rbind(b,c)
age_Card.YN <- rbind(age_Card.YN,a)
age_Card.YN <- rbind(age_Card.YN,d)

age_Card.YN$type <- as.factor(age_Card.YN$type)
str(age_Card.YN)
summary(age_Card.YN)
library(lattice)

e$type <- as.factor(e$type)
qplot(x=Birthday_Day,                             
      data=e,                     
      geom="density",        # 圖形=density
      xlab="Birthday",
      size=1,
      color= type           # 以顏色標註月份，複合式的機率密度圖
)

qplot(x=Birthday_Day,                             
      data=age_Card.YN,                     
      geom="density",        # 圖形=density
      xlab="Birthday",
      # size=1,
      color= type           # 以顏色標註月份，複合式的機率密度圖
)


######################################################################################################
##
str(cctxn)
cctxn$action_time.Day = as.Date(anytime(cctxn$action_time))
cctxn$action_time.Day <- as.factor(cctxn$action_time.Day)
#18天
card_day <-sqldf("select `action_time.Day` , sum(txn_amt), count(PID),
          count(distinct(PID)) ,
          sum(txn_amt)/count(PID) as 'Avg_Amount.Bill',
          sum(txn_amt)/count(distinct(PID)) as 'Avg_Amount.PID'
          from cctxn
          group by `action_time.Day`")
card_day

g <- ggplot(data= card_day)
g + geom_bar(stat = "identity",
             aes(x=action_time.Day,
                 y=`sum(txn_amt)`))

#分週一
card_day.w1 <-sqldf("select * ,round(`sum(txn_amt)`/200446775814*100,2) as 'percent' from card_day
                     where `action_time.Day`== '2087-02-25' or `action_time.Day`== '2087-02-26' or `action_time.Day`== '2087-02-27'
                    or `action_time.Day`== '2087-02-28' or `action_time.Day`== '2087-03-01'or `action_time.Day`== '2087-03-02'
                    or `action_time.Day`== '2087-03-03' 
                    group by `action_time.Day`") 

q <- sqldf("select sum(`sum(txn_amt)`) from `card_day.w1`")
#q=200446775814

g <- ggplot(data= card_day.w1)
g + geom_bar(stat = "identity",
             aes(x=action_time.Day,
                 y= percent))

#分週二
card_day.w2 <-sqldf("select * ,round(`sum(txn_amt)`/163095755327*100,2) as 'percent' from card_day
                    where `action_time.Day`== '2087-03-04' or `action_time.Day`== '2087-03-05' or `action_time.Day`== '2087-03-06'
                    or `action_time.Day`== '2087-03-07' or `action_time.Day`== '2087-03-08'or `action_time.Day`== '2087-03-09'
                    or `action_time.Day`== '2087-03-10' 
                    group by `action_time.Day`") 

q2 <- sqldf("select sum(`sum(txn_amt)`) from `card_day.w2`")
#q2=163095755327

g <- ggplot(data= card_day.w2)
g + geom_bar(stat = "identity",
             aes(x=action_time.Day,
                 y= percent))

#合併2週
type_w <- rep(1,7)
week <- c(2:7,1)
card_day.w1 <- cbind(card_day.w1,type_w)
card_day.w1 <- cbind(card_day.w1,week)

type_w <- rep(2,7)
week <- c(2:7,1)
card_day.w2 <- cbind(card_day.w2,type_w)
card_day.w2 <- cbind(card_day.w2,week)

card_day.week <- rbind(card_day.w1,card_day.w2)
card_day.week$type_w <- as.factor(card_day.week$type_w)

g <- ggplot(data= card_day.week)
g + geom_bar(stat = "identity",
             aes(x=week,
                 y= percent,
                 fill=type_w))

histogram(x= ~ week | type_w, 
          data=card_day.week,     
          xlab="week",  
          layout=c(1,2))


ggplot(card_day.week,aes(week))+geom_freqpoly(aes(group =type_w, colour = type_w))



#做table
c <- merge(card_day.w1, card_day.w2, by=c("week"), all=TRUE) 
c
write.csv(c, "c.csv", row.names = FALSE)








#分小時
qplot(x=action_time.Day,                             
      data=cctxn,                     
      geom="density",        # 圖形=density
      xlab="action time",
      # size=1
)

cctxn$action_time.t <- anytime(cctxn$action_time)
str(cctxn)
summary(cctxn$action_time.t)

analyst1 <- cctxn %>% group_by(interval=cut(action_time.t , breaks="hour")) %>% summarise(n=n(),interaction=sum(txn_amt),avg=mean(txn_amt))
cctxn$action_time.w <- weekdays(cctxn$action_time)


#死的各月剪卡人數(%)
sqldf("select count(PID) from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N' and `2087-03-26` == 'N' ")



#死活最早往來時間
#把最早開始跟銀行建立關係的時間,切1到12個月。萃取出哪一個月建立關係。然後跟死活交叉
profile1$Date_Arrival <- as.Date(anytime(profile1$Date_Arrival))
profile1$Date_Arrival_M <- strftime(profile1$Date_Arrival , "%m")
a <- sqldf("select count(PID) , Date_Arrival_M from profile1 
       where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
       group by Date_Arrival_M ")
a
sum(a$`count(PID)`)#2712
a$percent_count <- round(a$`count(PID)`/sum(a$`count(PID)`)*100,2)
a$type <- rep(1,12)

b <- sqldf("select count(PID) , Date_Arrival_M from profile1 
       where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
          group by Date_Arrival_M ")
b$percent_count <- round(b$`count(PID)`/sum(b$`count(PID)`)*100,2)
b$type <- rep(0,12)
b
Date_Arrival_cardYN <- rbind(a,b) 
Date_Arrival_cardYN$Date_Arrival_M <- as.factor(Date_Arrival_cardYN$Date_Arrival_M)
Date_Arrival_cardYN$type <- as.factor(Date_Arrival_cardYN$type)
Date_Arrival_cardYN

g <- ggplot(data=Date_Arrival_cardYN,aes(x=Date_Arrival_M,y=`percent_count`,fill=type))
g + geom_bar(stat = "identity")
str(Date_Arrival_cardYN)

#初往來時間分前後四段
summary(profile1$Date_Arrival)
# Min.    1st Qu.     Median       Mean    3rd Qu.       Max. 
# 2310000000 3170000000 3310000000 3320000000 3520000000 3700000000 

profile1_DA_G1 <- filter(profile1, 2310000000 <= Date_Arrival & Date_Arrival< 3170000000)
profile1_DA_G2 <- filter(profile1, 3170000000 <= Date_Arrival & Date_Arrival< 3310000000)
profile1_DA_G3 <- filter(profile1, 3310000000 <= Date_Arrival & Date_Arrival< 3520000000)
profile1_DA_G4 <- filter(profile1, 3520000000 <= Date_Arrival & Date_Arrival< 3700000000)

profile1_DA_G1$Date_Arrival = as.Date(anytime(profile1_DA_G1$Date_Arrival))
profile1_DA_G2$Date_Arrival = as.Date(anytime(profile1_DA_G2$Date_Arrival))
profile1_DA_G3$Date_Arrival = as.Date(anytime(profile1_DA_G3$Date_Arrival))
profile1_DA_G4$Date_Arrival = as.Date(anytime(profile1_DA_G4$Date_Arrival))


as.Date(anytime(2310000000)) #"2043-03-15"
as.Date(anytime(3170000000)) #"2070-06-14"
as.Date(anytime(3310000000)) #"2074-11-21"
as.Date(anytime(3520000000)) #"2081-07-17"
as.Date(anytime(3700000000)) #"2087-04-01"

# a11:活著老的、a12死著老的
# a21:活著中的、a22死著中的
# a31:活著青的、a32死著青的
# a41:活著少的、a42死著少的

a11<- sqldf("select PID, Date_Arrival from profile1_DA_G1
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
      order by Date_Arrival")
nrow(a11)
type <- rep(11,54331)
a11 <- cbind(a11,type)

a12<- sqldf("select PID, Date_Arrival from profile1_DA_G1
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
            order by Date_Arrival")
nrow(a12)
type <- rep(12,635)
a12 <- cbind(a12,type)

a21<- sqldf("select PID, Date_Arrival from profile1_DA_G2
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
            order by Date_Arrival")
nrow(a21)
type <- rep(21,55389)
a21 <- cbind(a21,type)

a22<- sqldf("select PID, Date_Arrival from profile1_DA_G2
            where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
            order by Date_Arrival")
nrow(a22)
type <- rep(22,613)
a22 <- cbind(a22,type)


a31<- sqldf("select PID, Date_Arrival from profile1_DA_G3
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
            order by Date_Arrival")
nrow(a31)
type <- rep(31,51583)
a31 <- cbind(a31,type)

a32<- sqldf("select PID, Date_Arrival from profile1_DA_G3
            where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
            order by Date_Arrival")
nrow(a32)
type <- rep(32,817)
a32 <- cbind(a32,type)


a41<- sqldf("select PID, Date_Arrival from profile1_DA_G4
      where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'Y')
            order by Date_Arrival")
nrow(a41)
type <- rep(41,55208)
a41 <- cbind(a41,type)

a42<- sqldf("select PID, Date_Arrival from profile1_DA_G4
            where PID in (select PID from profile_card where `2087-02-25`== 'Y' and `2088-02-26`== 'N')
            order by Date_Arrival")
nrow(a42)
type <- rep(42,647)
a42 <- cbind(a42,type)


#合併為DateArrival_Card.YN
e <- rbind(a11,a12)
a <- rbind(a21,a22)
b <- rbind(a31,a32)
c <- rbind(a41,a42)
d <- rbind(b,c)
DateArrival_Card.YN <- rbind(e,a)
DateArrival_Card.YN <- rbind(DateArrival_Card.YN,d)

DateArrival_Card.YN$type <- as.factor(DateArrival_Card.YN$type)
str(DateArrival_Card.YN)
summary(age_Card.YN)
library(lattice)

e$type <- as.factor(e$type)
qplot(x=Birthday_Day,                             
      data=e,                     
      geom="density",        # 圖形=density
      xlab="Birthday",
      size=1,
      color= type           # 以顏色標註月份，複合式的機率密度圖
)

qplot(x=Date_Arrival,                             
      data=DateArrival_Card.YN,                     
      geom="density",        # 圖形=density
      xlab="DateArrival",
      # size=1,
      color= type           # 以顏色標註月份，複合式的機率密度圖
)



a <- sqldf("select type,count(PID) from `DateArrival_Card.YN` 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'N') 
           group by type order by count(PID) desc ")
a

b <- sqldf("select type,count(PID) from `DateArrival_Card.YN` 
           where `PID` in (select `PID` from use_card where `2087-02-25` ='Y' and `2088-02-26` = 'Y') 
           group by type order by count(PID) desc ")
b

c <- merge(a, b, by=c("type"), all=TRUE) 
c
write.csv(c, "c.csv", row.names = FALSE)






#############################################################################################################################################################

#import atm
path2 = "F:/hackathon-encoded/atm"
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
  colnames(myfiles2[[i]]) <- c('1','PID','action_type','action_time','account_type',
                               'accout','7','channel_id','txn_amt',
                               'txn_fee_amt','trans_type',
                               'target_bank_code','target_acct_nbr','address_zipcode','machine_bank_code','16','17')
}

head(myfiles2[[1]])

atm = do.call(rbind, myfiles2)

atm <- atm[,-c(1,7,16,17)]
head(atm)
summary(atm)
str(atm)


#清洗
library(stringr)
call_nbr = str_split_fixed(cti$call_nbr,"\\\\\"",n=10)
head(call_nbr)
cti$call_nbr <- call_nbr[,6]
head(cti$call_nbr)

library(lubridate)
library(anytime)
cti$inbound_time <- as.POSIXct(cti$inbound_time,origin = "1970-01-01")
head(cti$inbound_time)

cti$end_call_date = str_extract_all(cti$end_call_date, "[0-9]+", simplify = TRUE)
head(cti$end_call_date)
str(cti$end_call_date)
cti$end_call_date <- as.POSIXct(as.numeric(cti$end_call_date),origin = "1970-01-01")

call_length = difftime(cti$end_call_date,cti$inbound_time, units="secs")
cti <- cbind(cti,call_length)
str(cti)


calltype_desc = str_split_fixed(cti$calltype_desc,"\\\\\"",n=10)
head(calltype_desc)
cti$calltype_desc <- calltype_desc[,6]
head(cti$calltype_desc)

############################################################################################################################################################
##各月刷卡總金額分配
#土法硬求~待改善
# str(cctxn)
# cctxn$action_time<- as.Date(anytime(cctxn$action_time))
# sqldf("
#       select action_time, sum(txn_amt), count(DISTINCT`PID`) as `people_amount` ,sum(PID) `n_usecard`
#       from cctxn
#       group by action_time
# ")
# 
# cctxn$action_time<- as.Date(anytime(cctxn$action_time))
# sqldf("
#       select action_time, sum(txn_amt), count(DISTINCT`PID`) as `people_amount` ,sum(PID) `n_usecard`
#       from cctxn
#       where PID IN (select PID from profile_card
#                     where `2087-02-25` == 'Y' and `2088-02-26` == 'N')
#       group by action_time
#       ")
# 
# 
# 
#
# 
# #最後剪卡者每月刷卡總金額及平均每次交易金額
# q <- sqldf("
#                   select sum(txn_amt),avg(txn_amt) 
#                   from total_data
#                   where `2087-02-25`=='Y'and`2088-02-26`=='Y'and `2087-03-26`=='Y'
#                   ")
# q
# 
# #最後剪卡者每月刷卡總人數及平均交易金額By PID
# z <- sqldf("
#                   select PID 
#                   from total_data
#                   where `2087-02-25`=='Y'and`2088-02-26`=='Y'and `2087-03-26`=='Y'
#                   group by PID
#                   ")
# nrow(z)


##每月消費前5大細項

# z =sqldf("
#       select count(PID), mcc_code from cctxn
#       group by mcc_code
#       order by count(PID) desc
#       limit 50
#       ")
# library(ggplot2)
# g <- ggplot(data=z)
# g + geom_bar(x=mcc_code,
#              )
# 
# stat = "identity"
# color= mcc_code
# 
# z1 <- total_data %>% filter(`2087-02-25`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-02-25")
# z2 <- total_data %>% filter(`2087-03-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-03-26")
# z3 <- total_data %>% filter(`2087-04-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-04-26")
# z4 <- total_data %>% filter(`2087-05-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-05-26")
# z5 <- total_data %>% filter(`2087-06-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-06-26")
# z6 <- total_data %>% filter(`2087-07-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-07-26")
# z7 <- total_data %>% filter(`2087-08-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-08-26")
# z8 <- total_data %>% filter(`2087-09-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-09-26")
# z9 <- total_data %>% filter(`2087-10-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-10-26")
# z10 <- total_data %>% filter(`2087-11-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-11-26")
# z11 <- total_data %>% filter(`2087-12-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-12-26")
# z12 <- total_data %>% filter(`2088-01-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2088-01-26")
# z13 <- total_data %>% filter(`2088-02-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2088-02-26")
# 
# month_mcc <- rbind(z1[1:5,], z2[1:5,])
# month_mcc <- rbind(month_mcc, z3[1:5,])
# month_mcc <- rbind(month_mcc, z4[1:5,])
# month_mcc <- rbind(month_mcc, z5[1:5,])
# month_mcc <- rbind(month_mcc, z6[1:5,])
# month_mcc <- rbind(month_mcc, z7[1:5,])
# month_mcc <- rbind(month_mcc, z8[1:5,])
# month_mcc <- rbind(month_mcc, z9[1:5,])
# month_mcc <- rbind(month_mcc, z10[1:5,])
# month_mcc <- rbind(month_mcc, z11[1:5,])
# month_mcc <- rbind(month_mcc, z12[1:5,])
# month_mcc <- rbind(month_mcc, z13[1:5,])
# # month_mcc$month <- as.factor(month_mcc$month)
# c=c(rep(5858817,5),rep(5858207,5),rep(5855012,5),rep(5852641,5),rep(5849808,5),rep(5845889,5),rep(5841572,5),rep(5833880,5),rep(5825301,5),rep(5828594,5),rep(5817596,5),rep(5810074,5),rep(5804958,5))
# month_mcc <- cbind(month_mcc,c)
# month_mcc <- month_mcc %>% mutate(P_mcc_code = round(N_mcc_code/c*100,2))
# 
# str(month_mcc)
# 
# ggplot(data = month_mcc, mapping = aes(x = factor(month), y = P_mcc_code, fill = mcc_code)) + geom_bar(stat= 'identity', position = 'stack')



# ##每月消費前5大細項(死的)
# z1 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-02-25")
# z2 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-03-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-03-26")
# z3 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-04-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-04-26")
# z4 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-05-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-05-26")
# z5 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-06-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-06-26")
# z6 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-07-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-07-26")
# z7 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-08-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-08-26")
# z8 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-09-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-09-26")
# z9 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-10-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-10-26")
# z10 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-11-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-11-26")
# z11 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2087-12-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2087-12-26")
# z12 <- total_data %>% filter(`2087-02-25`=='Y'&`2088-02-26`=='N'&`2088-01-26`=='Y') %>% group_by(mcc_code) %>% summarise(N_mcc_code=n())%>% arrange(desc(N_mcc_code))%>%mutate(month= "2088-01-26")
# 
# 
# c1 = c('NA','05411','05541','05311','00020')
# c2 = c(0,0,0,0,0)
# c3 = c('2088-02-26','2088-02-26','2088-02-26','2088-02-26','2088-02-26')
# z13 = cbind(c1,c2)
# z13 = cbind(z13,c3)
# 
# z13[,2] <- as.integer(z13[,2])
# z13[,3] <- as.character(z13[,3])
# 
# cut_month_mcc <- rbind(z1[1:5,], z2[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z3[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z4[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z5[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z6[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z7[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z8[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z9[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z10[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z11[1:5,])
# cut_month_mcc <- rbind(cut_month_mcc, z12[1:5,])
# cut_month_mcc <- as.matrix(cut_month_mcc)
# cut_month_mcc <- rbind(cut_month_mcc, z13)
# cut_month_mcc
# cut_month_mcc = data.frame(cut_month_mcc)
# str(cut_month_mcc)
# 
# cut_month_mcc[,2] <- as.integer(cut_month_mcc[,2])
# cut_month_mcc[,3] <- as.character(cut_month_mcc[,3])
#做P_mcc_code
# cut_call %>% filter(`2087-02-25`=='Y') %>% summarise(N=n())
# c= c()
# ggplot(data = cut_month_mcc, mapping = aes(x = factor(month), y = P_mcc_code, fill = mcc_code)) + geom_bar(stat= 'identity', position = 'stack')
# 
# 
# 
# g <- ggplot(data=month_mcc)
# g + geom_bar(stat = "identity",
#              x=month,
#              y=P_mcc_code,
#              color= mcc_code)

# 製做table(死活刷卡總金額/各月)
# a= c(2712,	0,	40053740628,	14769078.4,	785860.6)
# b= c(2662,	50,	39947041924,	15006401.9,	786173.4)
# c= c(2537,	125,	38760002251,	15277888.2,	787676.8)
# d= c(2379,	158,	36709553637,	15430665.7,	786964.9)
# e= c(2195,	184,34540828020,	15736140.3,	786233.9)
# f= c(2020,	175,	32330837260	,16005365	,788307.1)
# g= c(1790,	230	,29605658764,	16539474.2	,788895.2)
# h= c(1516,	274,	25961394904,	17124930.7,	791216.5)
# i= c(1298,	218,	21883880807,	16859692.5,	782265.6)
# j= c(1014,	284	,17722728077,	17478035.6,	786523.2)
# k= c(621,	393,	9642376508,	15527176.3,	801194.6)
# l= c(307,	314	,4385136440,	14283832.1,	792255.9)
# m= c(0,	307,	0	,0,	0)
# 
# z= rbind(a,b)
# z= rbind(z,c)
# z= rbind(z,d)
# z= rbind(z,e)
# z= rbind(z,f)
# z= rbind(z,g)
# z= rbind(z,h)
# z= rbind(z,i)
# z= rbind(z,j)
# z= rbind(z,k)
# z= rbind(z,l)
# y = c('2087-02-25','2087-03-26','2087-04-26','2087-05-26','2087-06-26','2087-07-26','2087-08-26','2087-09-26','2087-10-26','2087-11-26','2087-12-26','2088-01-26','2088-02-26')
# z <- cbind(z,y)
# rownames(z)=c('2087-02-25','2087-03-26','2087-04-26','2087-05-26','2087-06-26','2087-07-26','2087-08-26','2087-09-26','2087-10-26','2087-11-26','2087-12-26','2088-01-26','2088-02-26')
# colnames(z)=c('n(PID)','剪卡人數','總刷卡金額','平均每人刷卡金額','平均每筆刷卡金額','月份')
# mat1 = as.data.frame(z)


# library(ggplot2)
# ggplot(data=mat1) +   
#   
#   # 要畫線的話，對應的函式是geom_line()
#   geom_line(aes(x='月份',  
#                 y='總刷卡金額',
#                 color='月份') 
#   ) +
#   
#   # 用labs()，進行文字上的標註(Annotation)
#   labs(title="Line Plot of Temp-Ozone",
#        x="月份",
#        y="總刷卡金額") +
#   
#   theme_bw()
# 
# 
# library(lattice)
# densityplot( ~ '總刷卡金額' ,      
#              data=mat1
# )


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

################################################################################################











