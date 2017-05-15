# 將信用卡(cctxn)partition_time=3696969600所有CSV合併

path = "F:/hackathon-encoded/cctxn/partition_time=3696969600/"
#path中的所有CSV檔變為list指定為files
files <- list.files(path=path, pattern="*.csv")
a <- function(x) read.csv(x,quote = "")
#將path及file合併，並使用a的function
cctxn = lapply(paste(path,files,sep=""), a)

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
cctxn$mcc_code <- as.factor(cctxn$mcc_code)
cctxn$card_level <- as.numeric(cctxn$card_level)
cctxn$original_currency <- as.factor(cctxn$original_currency)
cctxn$c_category_desc <- as.factor(cctxn$c_category_desc)
cctxn$card_type <- as.factor(cctxn$card_type)
cctxn$card_level <- as.factor(cctxn$card_level)
str(cctxn)



##信用卡種類（card type）簡易差異介紹(各組別平均刷卡次數，與平均刷卡金額...)
install.packages("sqldf")
library(sqldf)
library(dplyr)
card_type.sql <- sqldf("select card_type,sum(txn_amt) TotalAmt,avg(txn_amt) AvgAmt,count(PID) count from cctxn group by card_type order by count(PID) desc")
card_typeff0 <- subset(cctxn, card_type =="68c0679dee09a8a84415575141e7bff0" ) 
str(card_typeff0)
summary(card_typeff0)

#(card_type)平均刷卡金額及刷卡次數點散佈圖
ggplot(data=card_type.sql) +   
  # 散布圖對應的函式是geom_point()
  geom_point(aes(x=TotalAmt,  # 用aes()，描繪散布圖內的各種屬性
                 y=count,
                 size = 4,
                 color=card_type) 
  ) + 
  # 用labs()，進行文字上的標註(Annotation)
  labs(title="刷卡金額及刷卡次數點散佈圖",
       x="刷卡金額",
       y="刷卡次數") +
  theme_bw()    


#(card_type)刷卡金額及刷卡次數點散佈圖   
library(lattice)
GroupPID.sql <- sqldf("select card_type,txn_amt,sum(txn_amt) TotalAmt,avg(txn_amt) AvgAmt,count(txn_amt) count from cctxn  group by PID order by count(txn_amt) desc")
xyplot(x=count~txn_amt | card_type,  # count放在Y軸，txn_amt放在X軸，並根據card_type條件分別繪圖
       data=GroupPID.sql,      
       layout = c(11,1),      # 以5x1的方式呈現圖
       
       # 在這裡使用panel function，畫出中位數的線
       panel=function(x,y){  
         # function的寫法，會用大括號包起來，裡面表示要進行的動作：
         # 在這個panel function裡面，我們進行了兩個動作
         panel.xyplot(x, y)                    # 1.繪製x-y的散布圖
         panel.abline(h = median(y), lty = 2)  # 2.標示出中位數的線段
       }
       
)


library(ggplot2)
qplot(x=txn_amt,                             
      data=GroupPID.sql,                     
      geom="density",        # 圖形=density
      xlab="txn_amt",
      color= card_type       # 以顏色標註月份，複合式的機率密度圖
)

head(GroupPID.sql)

Auntie <- ggplot(data=GroupPID.sql,aes(x=txn_amt,y=count))

Auntie + geom_line(aes(color=card_type))
                 