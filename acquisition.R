---
title: "acquisition"
author: "Karen"
date: "2017年4月16日"
output: html_document
---

## 一次匯入多筆CSV檔

#import data 土法~待解聰明方法
partition_time3696969600 = read.csv("F:/hackathon-encoded/profile/partition_time=3696969600/1.csv",header = F)
partition_time3699475200 = read.csv("F:/hackathon-encoded/profile/partition_time=3699475200/2.csv",header = F)
partition_time3702153600 = read.csv("F:/hackathon-encoded/profile/partition_time=3702153600/3.csv",header = F)
partition_time3704745600 = read.csv("F:/hackathon-encoded/profile/partition_time=3704745600/4.csv",header = F)
partition_time3707424000 = read.csv("F:/hackathon-encoded/profile/partition_time=3707424000/5.csv",header = F)
partition_time3710016000 = read.csv("F:/hackathon-encoded/profile/partition_time=3710016000/6.csv",header = F)
partition_time3712694400 = read.csv("F:/hackathon-encoded/profile/partition_time=3712694400/7.csv",header = F)
partition_time3715372800 = read.csv("F:/hackathon-encoded/profile/partition_time=3715372800/8.csv",header = F)
partition_time3717964800 = read.csv("F:/hackathon-encoded/profile/partition_time=3717964800/9.csv",header = F)
partition_time3720643200 = read.csv("F:/hackathon-encoded/profile/partition_time=3720643200/10.csv",header = F)
partition_time3723235200 = read.csv("F:/hackathon-encoded/profile/partition_time=3723235200/11.csv",header = F)
partition_time3725913600 = read.csv("F:/hackathon-encoded/profile/partition_time=3725913600/12.csv",header = F)
partition_time3728592000 = read.csv("F:/hackathon-encoded/profile/partition_time=3728592000/13.csv",header = F)


#合併CSV檔(Merge只能兩兩併)
a=merge(partition_time3696969600, partition_time3699475200, all = T,ncol=15)
b=merge(partition_time3702153600, partition_time3704745600, all = T,ncol=15)
c=merge(partition_time3707424000, partition_time3710016000, all = T,ncol=15)
d=merge(partition_time3712694400, partition_time3715372800, all = T,ncol=15)
e=merge(partition_time3717964800, partition_time3720643200, all = T,ncol=15)
f=merge(partition_time3723235200, partition_time3725913600, all = T,ncol=15)
g=merge(a, b, all = T,ncol=15)
h=merge(c, d, all = T,ncol=15)
i=merge(e, f, all = T,ncol=15)
j=merge(g, h, all = T,ncol=15)
k=merge(i, partition_time3728592000, all = T,ncol=15)
total_data=merge(j, k, all = T,ncol=15)


#帶入Header
colnames(total_data) = c('PID','Birthday','Gender','Comm_Area','Comm_Areacode','House_Area','House_AreaCode','BOA','Rev','Date_Arrival','Credit_Card','Loan','Saving','FM','Data_Date')
summary(total_data)

#sorted
totaldata_sorted = total_data[order(total_data$PID , total_data$Birthday),] 
summary(totaldata_sorted)

install.packages('dplyr')
library(dplyr)
distinct_PID = distinct(totaldata_sorted, PID)
nrow(distinct_PID)



#客戶總數

#匯入profile data
data = read.csv("F:/hackathon-encoded/profile/partition_time=3696969600/part-00000-74cda679-a63c-4af7-8c30-cf0a24a44e8f.csv",header = F)
head(data)


colnames(data) = c('PID','Birthday','Gender','Comm_Area','Comm_Areacode','House_Area','House_AreaCode','BOA','Rev','Date_Arrival','Credit_Card','Loan','Saving','FM','Data_Date')
str(data)

options(scipen=999)
summary(data)

#檢查Null、NA值，Gender有一空值，Comm_Areacode等有NA暫不處理
which(data$Gender == "")
#將刪除Gender空值後的data指定為data1
data <- data[-28139,]
#刪除Gender空值後的data
summary(data)

#將時間轉換
library(anytime)
data$Birthday <- as.Date(anytime(data$Birthday))
str(data$Birthday)
range(data$Birthday)


#近三個月平均總資產餘額(BOA)敘述統計值與boxplot 檢查分佈與極端值

#table(data$BOA)  連續型看次數無意義
options(scipen=999)
summary(data$BOA)
range(data$BOA)
quantile(data$BOA)
IQR(data$BOA)
sd(data$BOA)
var(data$BOA)
library(moments)
skewness(data$BOA) #偏度(偏態值 < 0，為負偏態，分配集中在平均數以上，高分群的個體較多)
kurtosis(data$BOA) #峰度(偏態值 < 0，為負偏態，分配集中在平均數以上，高分群的個體較多)
boxplot(data$BOA,horizontal = T,col='#f8f3c4')
title("近三個月平均總資產餘額(BOA)箱型圖")

# Y ~ X (代表X和Y軸要放的數值)
boxplot(formula = data$BOA~data$Gender,data=data,xlab="Gender",ylab="BOA",col="#f8f3c4")
title("近三個月平均總資產(BOA)與性別箱型圖")

hist(data$BOA,breaks=50,main="BOA分配圖",xlab="BOA")


#近一年淨收(Rev)敘述統計值與boxplot 檢查分佈與極端值

options(scipen=999)
summary(data$Rev)
range(data$Rev)
quantile(data$Rev)
IQR(data$Rev)
sd(data$Rev)
var(data$Rev)
library(moments)
skewness(data$Rev) #偏度(偏態值 < 0，為負偏態，分配集中在平均數以上，高分群的個體較多)
kurtosis(data$Rev) #峰度(偏態值 < 0，為負偏態，分配集中在平均數以上，高分群的個體較多)
boxplot(data$Rev,horizontal = T,col='#f8f3c4')
title("近一年淨收(Rev)箱型圖")
boxplot(formula = data$Rev~data$Gender,data=data,xlab="Gender",ylab="Rev",col="#f8f3c4")
title("近一年淨收(Rev)與性別箱型圖")

hist(data$Rev,breaks=50,main="Rev分配圖",xlab="Rev")


#總資產與淨收益點散布圖

plot(x=data$BOA,
     y=data$Rev,
     main="總資產與淨收益點散布圖",
     xlab="總資產(BOA)",
     ylab="淨收益(Rev)",
     pch=16) # 點的圖形

#現在在這張圖片中，把男生的資料點用藍色標註上去
M_data <- data[data$Gender=="M", ]
#找出男性的資料
# 標上藍色的點
points(x=M_data$BOA,                       
       y=M_data$Rev, 
       pch=16,                 
       col="blue")              # 顏色
F_data <- data[data$Gender=="F", ]
points(x=F_data$BOA,                       
       y=F_data$Rev, 
       pch=16,                 
       col="red") 

# 在右上角做出標示
# 表示在右上角
# pch代表點的圖案
# col代表顏色 
#顏色所對應的名稱
legend("topright",pch=1,col=c("blue","red"),legend= c("男性", "女性")) 
       
# 建立一個線性回歸
lm.model <- lm(data$Rev~data$BOA, data)
# 畫上回歸的趨勢線
abline(lm.model,lwd=2)
# lwd 代表線的粗細


#淨收益與總資產獨立性檢定
# Pearson卡方檢定用來檢定兩個類別型變數間是否獨立
# 但在常態分配時 相關性和獨立性卻是等價的
# 也就是 假如今天X,Y都是常態分配 那麼
# X,Y獨立 <=> X,Y相關性=0
# 
# 淨收益與總資產皆為連續型變數，若為常態→改用
# 若不為常態→Hoeffding's Test

#BOA常態性檢定
library(stats)
qqnorm(data$BOA)
qqline(data$BOA,col='red')
BOA_sample = sample(data$BOA,size=1000)
shapiro.test(BOA_sample) 
#檢查是否符合常態分配，p值<0.05→樣本不是來自常態分布的母體

#Rev常態性檢定
qqnorm(data$Rev)
qqline(data$Rev,col='red')
Rev_sample = sample(data$Rev,size=1000)
shapiro.test(Rev_sample) 

cor_BOARev = cov(data$BOA,data$Rev) / (sd(data$BOA) * sd(data$Rev))  
cor_BOARev
cor.test(data$BOA,data$Rev)



#業務比例

#####一維次數分配表
#性別
table(data$Gender)
#信用卡
table(data$Credit_Card)
#貸款
table(data$Loan)
#存款
table(data$Saving)
#理財
table(data$FM)


#####相對比例
#性別
options(digits =2) 
(table(data$Gender) / nrow(data))*100
pie(table(data$Gender),main="客戶性別比率",radius=1)
#信用卡
(table(data$Credit_Card) / nrow(data))*100
pie(table(data$Credit_Card),main="客戶辦信用卡比率")
#貸款
(table(data$Loan) / nrow(data))*100
pie(table(data$Loan),main="客戶貸款比率")
#存款
(table(data$Saving) / nrow(data))*100
pie(table(data$Saving),main="客戶存款比率")
#理財
(table(data$FM) / nrow(data))*100
pie(table(data$FM),main="客戶理財比率")



##業務比率(ggplot2)
# test <- data.frame(gender=c("F", "M"),
#                    perc=c(61.17,38.83))
# #準備畫布
# ggplot(data=test) +
#   
#   # 先畫bar plot
#   geom_bar(aes(x=factor(1),
#                y=gender,
#                fill=gender),
#            stat = "identity"
#   ) +
#   
#   # 再沿著Y，轉軸成圓餅圖
#   coord_polar("y", start=0)+
#   labs(title="性別比",
#        x="性別"
#        )




#####二維次數分配表(以辦卡_貸款為例)
table_CL = table(data$Credit_Card,data$Loan)
margin.table(table_CL, 1) #辦卡人數
margin.table(table_CL, 2) #貸款人數

#####二維比列表
##信用卡-貸款
options(digits=2,scipen=999) #digits=2→小數點後2位 ； scipen=999→取消科學記號
table_CL = table(data$Credit_Card,data$Loan)
(prop.table(table_CL,))*100
(prop.table(table_CL,1))*100

pct = round(table(data$Credit_Card) / length(data$Credit_Card) *100,)
labels = paste(names(pct),pct,"%")
pie(table(data$Credit_Card), labels = labels)

C_L = table(data$Loan,data$Credit_Card)
mosaicplot(C_L)

#信用卡-存款
options(digits=2,scipen=999)
table_CS = table(data$Credit_Card,data$Saving)
(prop.table(table_CS,))*100
(prop.table(table_CS,1))*100

C_S = table(data$Saving,data$Credit_Card)
mosaicplot(C_S)

#信用卡-理財
options(digits=3,scipen=999)
table_CF = table(data$Credit_Card,data$FM)
(prop.table(table_CF,))*100
(prop.table(table_CF,1))*100

C_F = table(data$FM,data$Credit_Card)
mosaicplot(C_F)

#貸款-信用卡
options(digits=2,scipen=999) #digits=2→小數點後2位 ； scipen=999→取消科學記號
table_CL = table(data$Credit_Card,data$Loan)
t((prop.table(table_CL,))*100)
t((prop.table(table_CL,2))*100)

#貸款-存款
options(digits=2,scipen=999)
table_LS = table(data$Loan,data$Saving)
(prop.table(table_LS,))*100
(prop.table(table_LS,1))*100

L_S = table(data$Saving,data$Loan)
mosaicplot(L_S)

#貸款-理財
options(digits=3,scipen=999)
table_LF = table(data$Loan,data$FM)
(prop.table(table_LF,))*100
(prop.table(table_LF,1))*100


L_F = table(data$FM,data$Loan)
mosaicplot(L_F)

#存款-信用卡
table_CS = table(data$Credit_Card,data$Saving)
round(t(prop.table(table_CS,))*100,2)
round(t(prop.table(table_CS,2))*100,2)

#存款-貸款
table_LS = table(data$Loan,data$Saving)
round(t(prop.table(table_LS,))*100,2)
round(t(prop.table(table_LS,2))*100,2)

#存款-理財
table_SF = table(data$Saving,data$FM)
round((prop.table(table_SF,))*100,2)
round((prop.table(table_SF,1))*100,2)


S_F = table(data$FM,data$Saving)
mosaicplot(S_F)

pie(table(data$Gender),main="客戶性別比率",radius=1)
pie(table(data$Credit_Card))
pct = round(table(data$Gender) / length(data$Gender) *100,1)
labels = paste(names(pct),pct,"%")
pie(table(data$Gender), labels = labels)

gender_C = table(data$Gender,data$Credit_Card)
mosaicplot(gender_C)

pie(pct, labels=lbls) # 以pct為資料繪製圓餅圖, 並將分類名稱指定為lbls的名稱
pie(table(data$Credit_Card),main="客戶信用卡持卡比率",radius=1)
pie(table(data$Loan),main="客戶貸款比率",radius=1)
pie(table(data$Saving),main="客戶存款比率",radius=1)
pie(table(data$FM),main="客戶理財比率",radius=1)

#理財—信用卡
table_CF = table(data$Credit_Card,data$FM)
round(t(prop.table(table_CF,))*100,2)
round(t(prop.table(table_CF,2))*100,2)

#理財—貸款
table_LF = table(data$Loan,data$FM)
round(t(prop.table(table_LF,))*100,2)
round(t(prop.table(table_LF,2))*100,2)

#理財—存款
table_SF = table(data$Saving,data$FM)
round(t(prop.table(table_SF,))*100,2)
round(t(prop.table(table_SF,2))*100,2)

```

#profile分5群，對業務進行比較
```{R}
#依比例切
options(digits=9,scipen=999)
summary(data$Rev)

quantile(data$Rev,0.6) #3396988800
quantile(data$Rev,0.8) #3556742400 

#包含下界不包含上界
data_Rev_Goup1 <- filter(data, 0 <= Rev & Rev< 727130.823)
data_Rev_Goup2 <- filter(data, 727130.823 <= Rev & Rev< 1113334.51)
data_Rev_Goup3 <- filter(data, 1113334.51 <= Rev & Rev< 1315620.46)
data_Rev_Goup4 <- filter(data, 1315620.46 <= Rev & Rev< 1514272.53)
data_Rev_Goup5 <- filter(data, 1514272.53 <= Rev & Rev< 2348000)

nrow(data_Rev_Goup1)
summary(data_Rev_Goup1$Rev)

nrow(data_Rev_Goup5)
summary(data_Rev_Goup5$Rev)

##信用卡-貸款
options(digits=2,scipen=999) #digits=2→小數點後2位 ； scipen=999→取消科學記號
Rev_Goup1_table_CL = table(data_Rev_Goup1$Credit_Card,data_Rev_Goup1$Loan)
(prop.table(Rev_Goup1_table_CL,))*100
(prop.table(Rev_Goup1_table_CL,1))*100

Rev_Goup5_table_CL = table(data_Rev_Goup5$Credit_Card,data_Rev_Goup5$Loan)
(prop.table(Rev_Goup5_table_CL,))*100
(prop.table(Rev_Goup5_table_CL,1))*100



#信用卡-存款
options(digits=2,scipen=999)
Rev_Goup1_table_CS = table(data_Rev_Goup1$Credit_Card,data_Rev_Goup1$Saving)
(prop.table(Rev_Goup1_table_CS,))*100
(prop.table(Rev_Goup1_table_CS,1))*100

Rev_Goup5_table_CS = table(data_Rev_Goup5$Credit_Card,data_Rev_Goup5$Saving)
(prop.table(Rev_Goup5_table_CS,))*100
(prop.table(Rev_Goup5_table_CS,1))*100

C_S = table(data$Saving,data$Credit_Card)
mosaicplot(C_S)

#信用卡-理財
options(digits=2,scipen=999)
Rev_Goup1_table_CF = table(data_Rev_Goup1$Credit_Card,data_Rev_Goup1$FM)
(prop.table(Rev_Goup1_table_CF,))*100
(prop.table(Rev_Goup1_table_CF,1))*100

Rev_Goup5_table_CF = table(data_Rev_Goup5$Credit_Card,data_Rev_Goup5$FM)
(prop.table(Rev_Goup5_table_CF,))*100
(prop.table(Rev_Goup5_table_CF,1))*100

C_F = table(data$FM,data$Credit_Card)
mosaicplot(C_F)

#貸款-信用卡
options(digits=2,scipen=999) #digits=2→小數點後2位 ； scipen=999→取消科學記號
Rev_Goup1_table_CL = table(data_Rev_Goup1$Credit_Card,data_Rev_Goup1$Loan)
t((prop.table(Rev_Goup1_table_CL,))*100)
t((prop.table(Rev_Goup1_table_CL,2))*100)

Rev_Goup5_table_CL = table(data_Rev_Goup5$Credit_Card,data_Rev_Goup5$Loan)
t((prop.table(Rev_Goup5_table_CL,2))*100)

#貸款-存款
options(digits=2,scipen=999)
Rev_Goup1_table_LS = table(data_Rev_Goup1$Loan,data_Rev_Goup1$Saving)
(prop.table(Rev_Goup1_table_LS,))*100
(prop.table(Rev_Goup1_table_LS,1))*100

Rev_Goup5_table_LS = table(data_Rev_Goup5$Loan,data_Rev_Goup5$Saving)
(prop.table(Rev_Goup5_table_LS,1))*100

L_S = table(data$Saving,data$Loan)
mosaicplot(L_S)

#貸款-理財
options(digits=2,scipen=999)
Rev_Goup1_table_LF = table(data_Rev_Goup1$Loan,data_Rev_Goup1$FM)
(prop.table(Rev_Goup1_table_LF,))*100
(prop.table(Rev_Goup1_table_LF,1))*100

Rev_Goup5_table_LF = table(data_Rev_Goup5$Loan,data_Rev_Goup5$FM)
(prop.table(Rev_Goup5_table_LF,1))*100


L_F = table(data$FM,data$Loan)
mosaicplot(L_F)

#存款-信用卡
options(digits=2,scipen=999)
Rev_Goup1_table_CS = table(data_Rev_Goup1$Credit_Card,data_Rev_Goup1$Saving)
round(t(prop.table(Rev_Goup1_table_CS,2))*100,2)

Rev_Goup5_table_CS = table(data_Rev_Goup5$Credit_Card,data_Rev_Goup5$Saving)
round(t(prop.table(Rev_Goup5_table_CS,2))*100,2)

#存款-貸款
options(digits=2,scipen=999)
Rev_Goup1_table_LS = table(data_Rev_Goup1$Loan,data_Rev_Goup1$Saving)
round(t(prop.table(Rev_Goup1_table_LS,2))*100,2)

Rev_Goup5_table_LS = table(data_Rev_Goup5$Loan,data_Rev_Goup5$Saving)
round(t(prop.table(Rev_Goup5_table_LS,2))*100,2)

#存款-理財
Rev_Goup1_table_SF = table(data_Rev_Goup1$Saving,data_Rev_Goup1$FM)
round((prop.table(Rev_Goup1_table_SF,1))*100,2)

Rev_Goup5_table_SF = table(data_Rev_Goup5$Saving,data_Rev_Goup5$FM)
round((prop.table(Rev_Goup5_table_SF,1))*100,2)

#理財—信用卡
options(digits=3,scipen=999)
Rev_Goup1_table_CF = table(data_Rev_Goup1$Credit_Card,data_Rev_Goup1$FM)
round(t(prop.table(Rev_Goup1_table_CF,2))*100,2)

Rev_Goup5_table_CF = table(data_Rev_Goup5$Credit_Card,data_Rev_Goup5$FM)
round(t(prop.table(Rev_Goup5_table_CF,2))*100,2)

#理財—貸款
Rev_Goup1_table_LF = table(data_Rev_Goup1$Loan,data_Rev_Goup1$FM)
round(t(prop.table(Rev_Goup1_table_LF,2))*100,2)

Rev_Goup5_table_LF = table(data_Rev_Goup5$Loan,data_Rev_Goup5$FM)
round(t(prop.table(Rev_Goup5_table_LF,2))*100,2)

#理財—存款
Rev_Goup1_table_SF = table(data_Rev_Goup1$Saving,data_Rev_Goup1$FM)
round(t(prop.table(Rev_Goup1_table_SF,2))*100,2)

Rev_Goup5_table_SF = table(data_Rev_Goup5$Saving,data_Rev_Goup5$FM)
round(t(prop.table(Rev_Goup5_table_SF,2))*100,2)



#profile分5群，對各業務比例

summary(data_Rev_Goup1)
summary(data_Rev_Goup2)
summary(data_Rev_Goup3)
summary(data_Rev_Goup4)
summary(data_Rev_Goup5)



#profile分5群，看業務總數

#變更類別變數
data$Gender_f <- factor(data$Gender, levels = c("F","M",""), labels = c("0","1","na"))
data$Credit_Card_f <- factor(data$Credit_Card, levels = c("N","Y"), labels = c("0","1"))
data$Loan_f <- factor(data$Loan, levels = c("N","Y"), labels = c("0","1"))
data$Saving_f <- factor(data$Saving, levels = c("N","Y"), labels = c("0","1"))
data$FM_f <- factor(data$FM, levels = c("N","Y"), labels = c("0","1"))
str(data)

data$Gender_f <- as.numeric(data$Gender_f)
data$Credit_Card_f <- as.numeric(data$Credit_Card_f)
data$Loan_f <- as.numeric(data$Loan_f)
data$Saving_f <- as.numeric(data$Saving_f)
data$FM_f <- as.numeric(data$FM_f)

data$Gender_f <- as.numeric(data$Gender_f-1)
data$Credit_Card_f <- as.numeric(data$Credit_Card_f-1)
data$Loan_f <- as.numeric(data$Loan_f-1)
data$Saving_f <- as.numeric(data$Saving_f-1)
data$FM_f <- as.numeric(data$FM_f-1)

rowSums(select(data,Credit_Card_f,Loan_f,Saving_f,FM_f))
business_count = rowSums(select(data,Credit_Card_f,Loan_f,Saving_f,FM_f))

data = cbind(data,business_count)
head(data)

data_Rev_Goup1 <- filter(data, 0 <= Rev & Rev< 727130.823)
data_Rev_Goup2 <- filter(data, 727130.823 <= Rev & Rev< 1113334.51)
data_Rev_Goup3 <- filter(data, 1113334.51 <= Rev & Rev< 1315620.46)
data_Rev_Goup4 <- filter(data, 1315620.46 <= Rev & Rev< 1514272.53)
data_Rev_Goup5 <- filter(data, 1514272.53 <= Rev & Rev< 2348000)
#Group1業務量
nrow(filter(data_Rev_Goup1,business_count==0))
summary(filter(data_Rev_Goup1,business_count==1))
nrow(filter(data_Rev_Goup1,business_count==1))
nrow(filter(data_Rev_Goup1,business_count==2))
nrow(filter(data_Rev_Goup1,business_count==3))
nrow(filter(data_Rev_Goup1,business_count==4))

#Group2業務量
nrow(filter(data_Rev_Goup2,business_count==0))
nrow(filter(data_Rev_Goup2,business_count==1))
nrow(filter(data_Rev_Goup2,business_count==2))
nrow(filter(data_Rev_Goup2,business_count==3))
nrow(filter(data_Rev_Goup2,business_count==4))

#Group3業務量
nrow(filter(data_Rev_Goup3,business_count==0))
nrow(filter(data_Rev_Goup3,business_count==1))
nrow(filter(data_Rev_Goup3,business_count==2))
nrow(filter(data_Rev_Goup3,business_count==3))
nrow(filter(data_Rev_Goup3,business_count==4))

#Group4業務量
nrow(filter(data_Rev_Goup4,business_count==0))
nrow(filter(data_Rev_Goup4,business_count==1))
nrow(filter(data_Rev_Goup4,business_count==2))
nrow(filter(data_Rev_Goup4,business_count==3))
nrow(filter(data_Rev_Goup4,business_count==4))

#Group5業務量
nrow(filter(data_Rev_Goup5,business_count==0))
nrow(filter(data_Rev_Goup5,business_count==1))
nrow(filter(data_Rev_Goup5,business_count==2))
nrow(filter(data_Rev_Goup5,business_count==3))
nrow(filter(data_Rev_Goup5,business_count==4))




#四項業務兩兩卡方獨立性檢定

chisq.test(table(data$Credit_Card,data$Loan))#信用卡-貸款

chisq.test(table(data$Credit_Card,data$Saving))#信用卡-存款

chisq.test(table(data$Credit_Card,data$FM))#信用卡-理財

chisq.test(table(data$Loan,data$Saving))#貸款-存款

chisq.test(table(data$Loan,data$FM))#貸款-理財

chisq.test(table(data$Saving,data$FM))#存款-理財


