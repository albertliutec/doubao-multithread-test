# multi_thread_test
## 模块信息
- 模块名称：multi_thread_test 
- 模块描述：提供基于多线程对doubao模型进行大规模数据测试的能力
- 作者：Albert Liu 
- 创建日期：2024-07-11 
- 版本：v2.0
## 模块使用
### 依赖
- openpyxl (安装方法: pip3 install openpyxl) 
- volcengine.maas.v2 (安装方法: pip3 install volcengine) 
- datasets (安装方法: pip3 install datasets)

### 使用说明
使用方式：
1. 按测试数据集的数据结构，修改数据清洗方法
- construct_test_data：读取test_data
    - construct_test_data_by_json: 读取json数据
    - construct_test_data_by_huggingface: 读取huggingface数据
- construct_system_prompt：读取system_prompt文件，默认读取同目录下system_prompt.txt文件
- construct_user_prompt：基于test_data中的数据结构，组装出user_prompt
- result_process：对于doubao的response进行数据清洗
- write_excel:将doubao返回内容写入文件，column 1默认写入当前数据在test_data中的index; column 2默认写入doubao返回的role为assistant的content

2. 选择以下4种任务中，任选其一执行任务
```python
- #任务1，单线程：5个样本，并行跑，index含0不含5
basic_run(0, 5, None, 0, excel_dic, start_row)

- #任务2，并行平均划分：200个样本到5个thread
run_average_parallel(20, 10, excel_dic, start_row)

- #任务3，index划分并行：15个样本，分别划分到3个thread中，每个thread分别负责0-3、4-10、11-15
sepreate_index_list = [0, 4, 11, 15]
run_index_parallel(sepreate_index_list, excel_dic, start_row)

- #任务4，串行点查：点查只查询index为40,55,100的三个值
point_index_list = [0, 40, 55, 110]
basic_run(0, 400, point_index_list, 0, excel_dic, start_row)
```

3. 结果输出
结果都生成在SUB_PATH路径下，一般会生成3个文件
- data.xlsx: 结果记录文件
- log.log: 业务运行日志
- num.txt: 多次重时候，仍失败的任务index（test_data中的index）,可以使用任务4重新跑
### 注意
1. 建议只修改全局参数 及 需要修改的数据清洗方法，其他方法一般不需要修改
2. 建议修改完各类construct_方法后，先用任务1跑一遍，无bug再起多线程并发跑数据
3. 核心代码逻辑在basic_run, 其他如run_average_parallel何run_index_parallel都是对basic_run的多线程封装