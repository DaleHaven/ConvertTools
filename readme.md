# 数据转换工具
## Version 0.0.2 （Master）
### 功能介绍
#### 支持功能介绍
1. json转csv。
2. 高性能模式。
3. 严谨转换模式。
   1. 将每行JSON中的value们的里面的英文逗号转换成中文逗号，防止出现叉列。
4. 自动创建结果文件夹及其文件。
5. 正常性能模式的严谨或者不严谨模式下支持显示出异常无法转换成CSV的JSON行Index。
6. 只需要安装Python 3.6.X，无需安装任何第三方模组即可使用。
7. 跨计算机平台。
8. 输入与输出路径自动转换为当前系统可兼容的斜线(正斜线和反斜线)。
#### 不支持的功能
1. json转csv以外的转换模式。
2. 多层嵌套的json。
#### 不推荐的使用
1. JSON的Key或者Value中存在英文的逗号，这会导致CSV列分隔符产生歧义，请使用严谨模式。
#### 使用案例
- 第一步：
```shell
$PYTHON_HOME/bin/python3 ./ConvertTools.py
```
- 第二步： 
  - 注意事项:
  1. 结果输出路径需要绝对路径
  2. 结果输出的文件如果已存在，则会追加写入，不会进行覆盖.
```shell
请输入被转换文件(绝对路径)：C:\DaleHaven\pycharm_workspace\FileConvertTools\SourceData\TestData.json
请选择模式(0.严谨模式/1.不严谨模式): 0
请选择模式(2.高性能模式/3.正常性能模式): 3
请输入转换后目录及文件名(绝对路径)：C:\DaleHaven\pycharm_workspace\FileConvertTools\JobResult\ConvertResult.csv
```