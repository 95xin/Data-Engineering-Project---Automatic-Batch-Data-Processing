# 数据工程项目 - 自动化批处理数据流水线

这个项目实现了一个完整的端到端数据处理流水线，用于处理纽约出租车数据。流水线使用Airflow进行编排，Spark进行大规模数据处理，PostgreSQL作为中间存储层，以及BigQuery作为最终的数据仓库。

## 项目架构

整个数据流水线遵循以下流程：

```
数据采集 → Spark清洗 → PostgreSQL存储 → 数据建模 → BigQuery同步 → 可视化
```

![数据流水线架构](https://via.placeholder.com/800x400?text=数据流水线架构图)

## 技术栈

- **Apache Airflow**: 工作流编排工具
- **Apache Spark**: 分布式数据处理引擎
- **PostgreSQL**: 关系型数据库，用作中间存储层
- **Google BigQuery**: 云数据仓库
- **Docker**: 容器化部署
- **Python**: 主要编程语言
- **Grafana/Looker Studio**: 数据可视化工具（可选）

## 项目结构

```
.
├── config/             # 配置文件目录（云服务账号凭证、数据库连接信息等）
├── dags/               # Airflow DAG定义（包含NYC出租车数据处理流程）
├── data/               # 数据存储目录（原始数据和处理后的数据）
├── plugins/            # Airflow插件（自定义Operators等）
├── spark-apps/         # Spark应用程序（数据清洗和转换逻辑）
├── spark-data/         # Spark数据文件（处理结果和临时数据）
├── lab/                # 实验性代码（概念验证和测试）
├── docker-compose.yaml # Docker配置（定义服务和依赖关系）
├── Dockerfile          # Docker镜像定义（环境配置）
├── .env                # 环境变量（配置参数）
└── README.md           # 项目文档
```

## 安装与设置

### 前提条件

- Docker 和 Docker Compose
- Python 3.8+
- 已配置的Google Cloud账号（用于BigQuery访问）
- PostgreSQL数据库（可通过Docker部署）

### 快速开始

1. 克隆仓库
   ```bash
   git clone https://github.com/95xin/Data-Engineering-Project---Automatic-Batch-Data-Processing.git
   cd Data-Engineering-Project---Automatic-Batch-Data-Processing
   ```

2. 配置环境变量
   ```bash
   cp .env.example .env
   # 编辑.env文件，填入您的配置信息
   ```

3. 在config目录中添加Google Cloud凭证文件
   - 从Google Cloud Console下载服务账号密钥JSON文件
   - 将文件放在config/目录并更新.env文件中的路径

4. 启动环境
   ```bash
   docker-compose up -d
   ```

5. 访问Airflow Web界面
   ```
   http://localhost:8080
   ```
   默认用户名和密码可在docker-compose.yaml文件中找到

## 详细阶段说明

### 1. 数据采集阶段 (Ingestion)

使用Python的requests库下载原始Parquet数据并存储在预定义路径下。此任务确保我们有最新的月度数据用于后续处理。

相关代码示例:
```python
def download_data():
    url = "https://data-source.com/path/to/taxi_data.parquet"
    response = requests.get(url)
    with open("/path/to/data/raw_taxi_data.parquet", "wb") as f:
        f.write(response.content)
```

### 2. 初步清洗阶段 (Preprocessing with Spark)

使用SparkSubmitOperator提交PySpark作业进行数据清洗：
- 移除空值行
- 标准化日期时间格式
- 去除重复记录
- 按月或日期分区以允许多个worker独立处理
- 最终使用coalesce(1)确保下游任务只处理一个文件

相关代码示例:
```python
df.dropna() \
  .dropDuplicates() \
  .repartition("year", "month") \
  .write.parquet("/path/to/partitioned/data/")

# 最终输出单一CSV文件
df.coalesce(1).write.csv("/path/to/output.csv", header=True)
```

### 3. 存入PostgreSQL阶段 (Staging Layer)

此阶段将清洗后的数据加载到PostgreSQL中，作为控制环境允许进一步的SQL转换，然后再发送到BigQuery。

使用分块处理避免内存过载并提高性能：
```python
df_iter = pd.read_csv(csv_file, chunksize=chunksize, iterator=True)
while True:
    try:
        chunk = next(df_iter)
        # 清理列名：去空格
        chunk.columns = [col.strip() for col in chunk.columns]  
        # 字符串转时间
        chunk["tpep_pickup_datetime"] = pd.to_datetime(chunk["tpep_pickup_datetime"], errors="coerce")
        chunk["tpep_dropoff_datetime"] = pd.to_datetime(chunk["tpep_dropoff_datetime"], errors="coerce")
        # 插入数据
        chunk.to_sql("table_name", engine, if_exists="append", index=False, method="multi")
    except StopIteration:
        print("所有 chunk 处理完成，写入 PostgreSQL 完成！")
        break
```

在上传大型数据集后，对常用过滤或连接列创建索引（如B-Tree索引）：
```sql
CREATE INDEX idx_pickup_datetime ON taxi_data(tpep_pickup_datetime);
```

### 4. 数据建模阶段 (Data Modeling)

在此阶段，我们进行数据建模和丰富：
- 创建主表结构，指定pickup_date作为分区键
- 创建子表
- 使用SQL语句和CASE WHEN逻辑丰富数据集

```sql
-- 添加时间分类列
ALTER TABLE taxi_data ADD COLUMN time_bucket VARCHAR(20);

-- 使用CASE WHEN填充值
UPDATE taxi_data
SET time_bucket = CASE
    WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 7 AND 10 THEN 'Morning Rush'
    WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 16 AND 19 THEN 'Evening Rush'
    ELSE 'Other'
END;
```

### 5. 同步到BigQuery阶段 (Warehouse Layer)

配置LoadJobConfig以启用模式自动检测，在摄取过程中自动跳过标题行：

```python
from google.cloud import bigquery

client = bigquery.Client()
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

with open(csv_file, "rb") as source_file:
    load_job = client.load_table_from_file(
        source_file,
        "project.dataset.table",
        job_config=job_config
    )

load_job.result()  # 等待任务完成
```

### 6. 可视化阶段 (Optional Visualization)

使用Grafana或Looker Studio连接BigQuery数据集并构建仪表板，用于洞察行程量、按时间段的平均车费等。

这一部分虽然是可选的，但通过直观地检查处理结果，它有助于验证整个流水线的输出质量。

## 监控与维护

项目包含监控层以跟踪流水线健康状况、任务失败、数据异常和性能指标：

```python
default_args = {
    'owner': 'airflow',
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

此外，我们使用：
- Airflow的任务日志分析执行时间
- Spark UI检查具有高shuffle读/写时间的阶段并相应优化

## 增量加载机制

当数据流水线下次自动运行时，上传新数据的机制是增量加载。这确保了只处理新的或更改的数据，提高了处理效率并减少了资源消耗。

## 总结

总之，本项目涵盖了从摄取到仓库加载的完整批处理工作流程。它展示了在编排(Airflow)、分布式处理(Spark)、关系存储(Postgres)、云仓库(BigQuery)和端到端流水线设计方面的实践经验。

## 贡献

欢迎贡献代码或提出改进建议！请遵循以下步骤：

1. Fork该仓库
2. 创建您的功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交您的更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开Pull Request

## 许可证

[MIT](LICENSE) 