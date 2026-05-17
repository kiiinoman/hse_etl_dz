# «ETL- процессы(tsk4)»

Создадим и настроим кластер

<img src="./assets/2026-04-30 141844.png" width="700">

<img src="./assets/2026-04-30 141907.png" width="700">

<img src="./assets/2026-04-30 141925.png" width="700">

<img src="./assets/2026-04-30 141953.png" width="700">

Загрузим json файл в бакет

<img src="./assets/2026-04-30 143255.png" width="700">

Подключимся по SSH и запустим spark

Выполнил код в spark

```python
import pyspark.sql.functions as F

df = spark.read.option("multiline", "true").json("s3a://mars17bucket/clients.json", multiLine=True)

# Выделяем passport
passport = df.select(
    F.col("passport.type"),
    F.col("passport.dcm_serial_no"),
    F.col("passport.dcm_no"),
    F.col("passport.dcm_date"),
    F.col("passport.issued_by"),
    F.col("tax_number").alias("client_tax_number")
)

passport.show()
passport.write.parquet("s3a://mars17bucket/passport.parquet")

# Основная таблица клиентов
clients = df.select(
    "name_cyr",
    "is_resident",
    "tax_number",
    "last_name",
    "first_name",
    "middle_name",
    "birth_date",
    "death_date",
    "registry_date",
    "risk_status",
    "risk_group",
    "sex",
    "country",
    "birth_place"
)

clients.show()
clients.write.parquet("s3a://mars17bucket/clients.parquet")

# ПРОВЕРКА
passport_check.show(5, truncate=False)
clients_check.show(5, truncate=False)
```

<img src="./assets/2026-04-30 141737.png" width="700">

<img src="./assets/2026-04-30 142344.png" width="700">

<img src="./assets/2026-04-30 150936.png" width="700">

<img src="./assets/2026-04-30 152936.png" width="700">

<img src="./assets/2026-04-30 153043.png" width="700">

После выполнения, parqet файлы находятся в бакете

<img src="./assets/2026-04-30 153620.png" width="700">
