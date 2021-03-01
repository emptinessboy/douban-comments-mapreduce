### MapReduce数据清洗

#### 数据上传HDFS：

先将python爬取到的数据上传到我在Hadoop平台的个人文件夹下的文档目录中。


完成将数据从Windows宿主机上传到Linux虚拟机 后，需要 将Linux上的文件上传到Hadoop平台（HDFS）
上面。

这一步的操作可以使用hdfs -dfs 或者hadoop fs 命令来完成。

#### 命令执行完毕后，可以到HDFS提供的在线 浏览工具中查看下当前HDFS中的数据。

#### 可以看到，爬取得到的数据已经成功 上传到分布式文件系统HDFS上了。


**新建MapReduce项目：**

由于我较为习惯使用IDEA集成开发环境，因此这里我在Ubuntu虚拟机 上安装来IDEA开发工具，并使用了
IDEA来创建MapReduce项目。因为IDEA没有像Eclipse那样有Hadoop的插件来简化开发，本实验的项目
结构也较为简单，因此不依赖Maven之类的构建工具，直接新建一个普通的JAVA程序项目。

项目创建完成后，还需要导入Hadoop以及MapReduce依赖的相关包。


#### 导入依赖：

由于后续我还需要在项目中引入com.alibaba.FASTJSON等自定义或者第三方的包，因此我在项目目录下新建
了一个lib文件夹，用来存放第三方或者自定义的jar包。

接下来下载反序列化需要用到的阿里巴巴开源的JSON库（com.alibaba.fastjson）

#### 导入FASTJSON依赖：

#### 清洗爬取的JSON文件

由于Mapper阶段我们需要让程序从HDFS读取所有 存放在input文件夹中的json文件。 而电影 的排名信息
是以json的文件名来命名的。所以map阶段先要 通过InputSplit对象来获取文件名，然后 使用正则表达式提
取电影排名信息。


获取排名信息后，我使用阿里巴巴的fastJSON将文本的JSON文件来进行反序列化操作。然后从中很方便的
提取我们需要的信息存放到数组中。

提取完成信息后，我们对数组进行一次循环判空来过滤无效信息，最后使用将数据转为text类型并作为key
输出：context.write(text, NullWritable.get());

**Map阶段代码：**


**Reduce阶段代码：**

由于数据清洗的过程主要处理的是key，Map阶段没有产生需要处理的value。因此reduce阶段可以直接输
出。

#### 执行阶段代码：

这部分代码主要是指定Mapper类，Reduce类，指定文件的输入输出信息以及期交MapReduce作业。


**打包并运行MapReduce：**

先配置打包编译的参数，下一步就可以点击构建按钮输出jar包了。

打包完成后，使用hadoop jar命令运行我的MR程序， 效果如下：


#### 运行完成：

#### 查看运行结果：

#### 运行完成后可以通过浏览HDFS的输出文件夹查看效果。

将文件下载到本地进行查看， 可以看到MapReduce清洗后，剔除来JSON中无用的信息，并按照一行一部电
影的方式，使用分隔符 “|” 进行分割。


#### 至此，数据清洗结束。

#### 遇到的问题以及解决方案：

#### AUTO_TYPE异常

遇到抛出异常 **com.alibaba.fastjson.JSONException: autoType is not support** ，解决方案参考官网文档的开
启AUTO_TYPE方法。