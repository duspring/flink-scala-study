# flink-scala-study
flink1.10 scala version study

##### 需求一：实时热门页面统计

1. 需求分析：每隔5秒统计最近5分钟热门页面

2. 数据展示

![image-20201210160809728](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210160809728.png)

| IP地址         | 用户ID | 事件时间            | 请求方式 | URL                   |
| -------------- | ------ | ------------------- | -------- | --------------------- |
| 24.233.162.179 | -      | 2020-12-10 11:11:11 | GET      | /images/jordan-80.png |

其中：**事件时间**和**URL**对于这个需求是核心

3. 实现思路

![image-20201210160433283](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210160433283.png)

4. 展示效果

![image-20201210160518874](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210160518874.png)

##### 需求二：实时统计广告点击（实时计算黑名单）

1. 需求分析：同一天，同一个用户，同一个广告被点击100次即为黑名单
2. 数据展示

![image-20201210165710847](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210165710847.png)

3. 实现思路

![image-20201210170004422](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210170004422.png)

4. 展示效果

![image-20201210171331412](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210171331412.png)

![image-20201210171622395](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210171622395.png)

![image-20201210171750861](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210171750861.png)

![image-20201210171809099](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210171809099.png)

![image-20201210172543234](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210172543234.png)

![image-20201210172627894](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210172627894.png)

![image-20201210172953267](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210172953267.png)

![image-20201210173028821](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210173028821.png)

![image-20201210173353975](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210173353975.png)

![image-20201210173625584](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210173625584.png)

![image-20201210173645191](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210173645191.png)

![image-20201210175018083](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210175018083.png)

![image-20201210175343155](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210175343155.png)

![image-20201210175846684](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210175846684.png)

![image-20201210180121421](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210180121421.png)

![image-20201210180324733](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210180324733.png)

![image-20201210180509680](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20201210180509680.png)