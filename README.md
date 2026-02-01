# ETL Processes (Homework)

Учебный проект по курсу **«Процессы ETL»** (магистратура Data Engineering, ВШЭ).

---

## Стек
- Docker / Docker Compose  
- PostgreSQL  
- Apache Airflow  
- Python  

---

## Описание
В проекте реализованы два ETL-процесса:

### JSON
- Источник: `raw_pets_json`
- Результат: `pets_flat`
- Задача: распарсить JSON и сохранить данные в плоской таблице

### XML
- Источник: `raw_nutrition_xml`
- Результат:
  - `nutrition_flat`
  - `daily_values`
- Задача: распарсить XML и нормализовать вложенные данные

Используемый подход:
```

RAW → PARSED → FLAT

```

---

## Структура проекта
```

.
├── airflow/dags/
│   ├── simple_json_parser.py
│   └── simple_xml_parser.py
├── db/init.sql
├── docker-compose.yaml
└── Dockerfile

````

---

## Запуск
```bash
docker compose up --build
````

---

## Сервисы

* Airflow UI: [http://localhost:8080](http://localhost:8080)
* PostgreSQL: localhost:5432

---

## Проверка данных

```sql
SELECT * FROM pets_flat;
SELECT * FROM nutrition_flat;
SELECT * FROM daily_values;
```

---

## Статус

Проект реализует базовую ETL-логику и предназначен для учебных целей.
