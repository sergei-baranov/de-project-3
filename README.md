# Проект 3

Будет два DAG-а:
- один на backfilling - `etl_backfilling.py`,
- второй на increment-ы - `etl_increment.py`.

Первый запускаем вручную: `start_date` ставим на день до якорной даты
(datetime.today() - timedelta(days=9)), `end_date` - на якорную дату
(datetime.today() - timedelta(days=8)), и запускаем кнопкой в UI.
И ставим `schedule_interval` в `'@once'`.

Task-и первого DAG-а сохраняют `report_id` в Variable `etl_report_id`, а `end_date` - в Variable `etl_backfill_end_date`.

С датой старта второго DAG-а поступаю так:
Дата в etl_backfill_end_date - это поледняя дата уже имеющихся в backfill-е данных, добавляю к ней два дня.
Ему же `schedule_interval` выставляем в `'@daily'`, и `catchup` в `True`.

```python
# вот это дата, за которую были последние данные в backfill
# '2022-06-14 00:00:00'
date_time_str = Variable.get('etl_backfill_end_date')
dt_start_obj = datetime.strptime(date_time_str + ' +0300', '%Y-%m-%d %H:%M:%S %z')
# добавим два дня
dt_start_obj2 = dt_start_obj + timedelta(days=2)
# a вот это - завтра
end_date_obj = datetime.today() + timedelta(days=1)
```

Так же таскам второго DAG-а прописываю

```python
            'depends_on_past': True,
            'wait_for_downstream': True
```

# Этап 0. Backfilling.

## 0.1. Разворачиваем контейнер

```bash
docker run -d -p 3000:3000 -p 15432:5432 -e AIRFLOW__CORE__LOAD_EXAMPLES=False --name=de-project-sprint-3-server sindb/project-sprint-3:latest
```


## 0.2. БД

### Коннект

```bash
psql postgresql://jovyan:jovyan@127.0.0.1:15432/de
```

Ничего не делаем, все миграции в init-таске перевого DAG-а.

## 0.3. Airflow

### Заходим в UI Airflow

```
http://localhost:3000/airflow/

- AirflowAdmin
- airflow_pass
```

### Переменные (Variable)

Следующие переменные создадутся в init-таске первого DAG-а:

- `etl_task_id`
- `etl_report_id`
- `etl_backfill_end_date`
- `etl_increment_id`

### Исполюзуем следующие Connections (в контейнере Проекта они уже есть)

- `http_conn_id` (http)
    - Host: `https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net`
    - X-Api_Key и т.п. жёстко пишем в питоновском коде DAG-а

- `pg_connection` (postgres) - postgresql_de
    - Host: `localhost`
    - Schema: `de`

    ```
    (In Airflow a schema refers to the database name to which a connection is being made.)
    ```

    - Login: `jovyan`
    - Password: `jovyan`
    - Port: `5432`

- `s3_conn` - НЕ заводим, в первой версии ТЗ было недоварено с ним. Можно было бы использовать и коннект, запихав в Extra json с aws_access_key_id и aws_secret_access_key, но это несекьюрно, а как секьюрно - узнаем позже. На данный момент коннект к s3 определяем в питонячем коде в DAG-е.


## 0.4. ФС

### Заходим в контейнер

```bash
sudo docker ps -a

$ sudo docker ps -a
CONTAINER ID IMAGE ...
4007e5e134b4   sindb/project-sprint-3:latest ...

$ sudo docker exec -it 4007e5e134b4 bash
root@4007e5e134b4:/agent# pwd
/agent
```

### удаляем DAG-рыбу

В контейнере DAG-и настроены браться из /lessons/dags

```bash
root@4007e5e134b4:/lessons/# airflow config get-value core dags_folder

/lessons/dags
```

```bash
root@4007e5e134b4:/agent# rm /lessons/dags/sprint3.py
root@4007e5e134b4:/agent# 
```

### создать директории в контейнере

- `/file_staging`

```bash
root@4007e5e134b4:/agent# mkdir /file_staging
root@4007e5e134b4:/agent#
```

### в `/lessons/dags/sql` копируем sql из соотв. папки проекта
(/src/DAG/sql/)

```bash
# например в контейнер 4007e5e134b4 может быть так:
sudo docker cp ~/YA_DE/SPRINT4_ETL_автоматизация_подготовки_данных/de-project-3/src/DAG/sql 4007e5e134b4:/lessons/dags
```

### копируем DAG-файл в директорию для DAG-ов Айрфлоу

- первый

```bash
sudo docker cp ~/YA_DE/SPRINT4_ETL_автоматизация_подготовки_данных/de-project-3/src/DAG/etl_backfilling.py 4007e5e134b4:/lessons/dags/
```

- второй - после отработки первого (ему для определения нужна переменная, заполненная по отработке первого)

```bash
sudo docker cp ~/YA_DE/SPRINT4_ETL_автоматизация_подготовки_данных/de-project-3/src/DAG/etl_increment.py 4007e5e134b4:/lessons/dags/
```

## Запускаем первый DAG

- Он пересоздаст БД,
- сделает миграцию staging.user_order_log,
- создаст view-ху mart.f_customer_retention,
- создаст переменные в Airflow,
- осуществит загрузку инициализирующих данных


# Этап 1. Increment. Учесть status (при refunded в факты едут отрицательные значения)

1. В Airflow создана первым DAG-ом переменная `etl_increment_id`.
В неё DAG для инкрементов будет класть `increment_id` из ответа операции `/get_increment`

2. В DAG `etl_increment.py` прописываем в `start_date` значение из переменной `etl_backfill_end_date` (от DAG-а `etl_backfilling.py`) плюс два дня

```python
# вот это дата, за которую были последние данные в backfill
# '2022-06-14 00:00:00'
date_time_str = Variable.get('etl_backfill_end_date')
dt_start_obj = datetime.strptime(date_time_str + ' +0300', '%Y-%m-%d %H:%M:%S %z')
# добавим два дня
dt_start_obj2 = dt_start_obj + timedelta(days=2)
# a вот это - завтра
end_date_obj = datetime.today() + timedelta(days=1)
...
        description='Increment S3 to Postgres',
        schedule_interval='@daily',
        catchup=True,
        start_date='dt_start_obj2,
        end_date=end_date_obj,
```

3. Так же таскам второго DAG-а прописываю (в default_args DAG-а)

```python
            'depends_on_past': True,
            'wait_for_downstream': True
```

4. Миграция схемы, раз уж мы заранее знаем, что будет приходить status теперь, сделана в init-task-е 1-го DAG-а.

Добавлены два вычисляемых поля, чтобы проще был сиквел для проброса в факты.

```sql
ALTER TABLE staging.user_order_log
ADD COLUMN "status" varchar(20) default 'shipped';

ALTER TABLE staging.user_order_log
ADD COLUMN quantity_signed bigint GENERATED ALWAYS AS
(CASE WHEN "status" = 'refunded' THEN quantity * -1 ELSE quantity END)
STORED;

ALTER TABLE staging.user_order_log
ADD COLUMN payment_amount_signed numeric(10,2) GENERATED ALWAYS AS
(CASE WHEN "status" = 'refunded' THEN payment_amount * -1 ELSE payment_amount END)
STORED;
```

см. тж. migrations/migrate_user_order_log.sql

5. копируем DAG в контейнер

```bash
sudo docker cp ~/YA_DE/SPRINT4_ETL_автоматизация_подготовки_данных/de-project-3/src/DAG/etl_increment.py 4007e5e134b4:/lessons/dags/
```

6. Включаем - работает

7. **Примечание**: В таску load_user_order_log (и соотв. функцию load_file_to_pg)
я включил так же и переброс даннных в измерения и факты.

Это сделано по причине того, что я использую одну и ту же таблицу в стейджинг-схеме, без поля-идентификатора типа batch_id для разных запусков дага, и поэтому хочу, чтобы таска load_user_order_log отрабатывала при catchup-е последовательно из одной и той же таблицы при разных запусках инкрементного DAG-а.

По идее можно вынести эти таски отдельно, и заливку измерений даже распараллелить, но тогда надо что-то хитрое придумать, типа сенсора отработки предыдущего запуска DAG-а, а не только depends_on_past и wait_for_downstream для тасков, а мы этого ещё не проходили как бы.

# Этап 2. Витрина f_customer_retention

Создана заранее в init-task-е 1-го DAG-а.

/migrations/mart_f_customer_retention.sql

```sql
CREATE OR REPLACE VIEW mart.f_customer_retention AS
WITH
cte_weeks AS (
    SELECT DISTINCT
        DATE_TRUNC('week', to_timestamp(s.date_id))::DATE AS period_id
    FROM
        mart.f_sales "s"
)
,
cte_orders AS (
    SELECT
        DATE_TRUNC('week', to_timestamp(s.date_id))::DATE AS period_id,
        customer_id,
        COUNT(*) AS customer_orders_count,
        SUM(payment_amount) AS customer_revenue
    FROM
        mart.f_sales "s"
    GROUP BY
        period_id,
        customer_id
)
,
cte_refunds AS (
    SELECT
        DATE_TRUNC('week', to_timestamp(s.date_id))::DATE AS period_id,
        COUNT(DISTINCT customer_id) AS refunded_customer_count,
        COUNT(*) AS customers_refunded
    FROM
        mart.f_sales "s"
    WHERE
        payment_amount < 0
    GROUP BY
        period_id
)
,
cte_new AS (
    SELECT
        period_id,
        customer_id,
        customer_revenue
    FROM
        cte_orders
    WHERE
        customer_orders_count = 1
)
,
cte_returning AS (
    SELECT
        period_id,
        customer_id,
        customer_revenue
    FROM
        cte_orders
    WHERE
        customer_orders_count > 1
)
SELECT
    'weekly'                        AS "period_name",
    w.period_id                     AS "period_id",
    COUNT(DISTINCT n.customer_id)   AS "new_customers_count",
    SUM(n.customer_revenue)         AS "new_customers_revenue",
    COUNT(DISTINCT rt.customer_id)  AS "returning_customers_count",
    SUM(rt.customer_revenue)        AS "returning_customers_revenue",
    MAX(rf.refunded_customer_count) AS "refunded_customer_count",
    MAX(rf.customers_refunded)      AS "customers_refunded"
FROM
    cte_weeks as "w"
    LEFT JOIN cte_new "n" ON n.period_id = w.period_id
    LEFT JOIN cte_returning "rt" ON rt.period_id = w.period_id
    LEFT JOIN cte_refunds "rf" ON rf.period_id = w.period_id
GROUP BY
    w.period_id
ORDER BY
    w.period_id ASC
;
```

# Этап 3. Перезапустить пайплайн и убедиться, что после перезапуска не появилось дубликатов в витринах mart.f_sales и mart.f_customer_retention.

Работает, у меня предусмотрены удаления перед вставками.

---
---
---
---
---
---
---
---

# Приложение А. Backfilling по первоначальному (неудачному) ТЗ и контейнеру на Проект. Сам себе на память оставил (он вполне рабочий на той версии образа).


## А.1. Разворачиваем контейнер, бэкапим бд de (схемы staging и mart).

```
sudo docker run -d -p 3000:3000 -p 15432:5432 --name=de-project-sprint-3-server sindb/project-sprint-3:latest
```

Далее например в DBeawer
- **меняем тип поля** customer_research.date_id на timestamp

```sql
-- SQL Error [42846]: ERROR: cannot cast type integer to timestamp without time zone
-- поэтому в два шага:
ALTER TABLE staging.customer_research ALTER COLUMN date_id type varchar(30) USING date_id::varchar;
ALTER TABLE staging.customer_research ALTER COLUMN date_id type timestamp USING date_id::timestamp;
```

- staging.customer_research.city_id - сделать nullable

```sql
ALTER TABLE staging.customer_research ALTER COLUMN city_id DROP NOT NULL;
```

- в staging.user_order_log все варчары с длины 15 поменять на 63

```
ALTER TABLE staging.user_order_log ALTER COLUMN city_name TYPE varchar(63) USING city_name::varchar;
ALTER TABLE staging.user_order_log ALTER COLUMN first_name TYPE varchar(63) USING first_name::varchar;
ALTER TABLE staging.user_order_log ALTER COLUMN last_name TYPE varchar(63) USING last_name::varchar;
ALTER TABLE staging.user_order_log ALTER COLUMN item_name TYPE varchar(63) USING item_name::varchar;
```

- staging.user_activity_log.item_id - сделать nullable

```
ALTER TABLE staging.user_activity_log ALTER COLUMN item_id DROP NOT NULL;
```

- mart.d_item.category_id - сделать nullable

```
ALTER TABLE mart.d_item ALTER COLUMN category_id DROP NOT NULL;
```

----

Далее например в DBeawer делаем **бэкап пустой структуры**: правой кнопкой по бд `de` -> Tools -> Backup -> отмечаем схемы `staging` и `mart` -> формат `plain` -> сохраняем в файл
- `migrations/dump-de-init-mart-n-staging.sql`.

Далее будем восстанавливаться отсюда на фазе `backfilling`.


## А.2. Подготавливаем DAG на основе того, что делалось в уроках спринта ("сквозной кейз")

0. Креденшлы в Airflow (http://localhost:3000/airflow/):
    - `AirflowAdmin`
    - `airflow_pass`

1. Содаём три коннекта в Airflow:
- `create_files_api` (http)
    - Host: `https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net`

- `s3_conn` (s3) - не создавать, см. коммент ниже
    - Conn Id = `s3_conn`,
    - aws_access_key_id = `YCAJEWXOyY8Bmyk2eJL-hlt2K`,
    - aws_secret_access_key = `YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA`.

    ```
    (aws_access_key_id и aws_secret_access_key используются в питоновском коде приложения, НЕ в свойствах коннекта Airflow (json в Extra не используем, как я понимаю, из соображений безопасности))
    ```

    ```
    !!! не надо создавать этот бесполезный коннект !!!
    надо в функции get_files_from_s3 в s3 = session.client(...) прописать
    service_name='s3', а второй аргумент (s3_conn_name) выпилить вовсе
    ```

- `pg_connection` (postgres)
    - Host: `localhost`
    - Schema: `de`

    ```
    (In Airflow a schema refers to the database name to which a connection is being made.)
    ```

    - Login: `jovyan`
    - Password: `jovyan`
    - Port: `5432`

2. Создаём две переменные в Airflow

   - `etl_task_id`
   - `etl_report_id`

3. Создаём директории ФС

   - `/file_staging`
   - `/migrations` (сюда копируем sql из соотв. папки проекта)

    ```bash
    # например в контейнер может быть так:
    sudo docker cp ~/YA_DE/SPRINT4_ETL_автоматизация_подготовки_данных/de-project-3/migrations 69e91d39919c:/
    ```

4. DAG-файл помещаем в 

   - `/src/DAG/_etl_backfilling_old.py` (в контейнере - в `/lessons/dags/etl_backfilling_old.py`)

### Приложение B. Backup, сам себе на память.


```
!!! Не надо под Убунтой ставить DBeaver из snap-а !!!
Оно из своей песочницы не получит доступ к /usr/bin/pg_dump
```

- В DBeaver выставить Local Client Home Dir в `/usr/bin`
- Правой кнопкой на бд de -> Tools -> Backup -> отметили все таблицы в схемах staging и mart -> Format 'Plain' -> File name 'dump-de-init-mart-n-staging.sql'.
- скопировать dump-de-init-mart-n-staging.sql в директорию migrations проекта.

## Исходный readme:

### Описание
Репозиторий предназначен для сдачи проекта №3. 

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий 
`de-project-{{ номер проекта }}` после того, как вы привяжете свой 
GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой локальный компьютер:
    * `git clone https://github.com/{{ username }}/de-project-3.git`
    * `cd de-project-3`
3. Выполните проект и сохраните получившийся код в локальном репозитории:
	  * `git add .`
	  * `git commit -m 'my best commit'`
4. Обновите репозиторий в вашем GutHub-аккаунте:
	  * `git push origin main`

### Структура репозитория
1. Папка migrations хранит файлы миграции. 
Файлы миграции должны быть с расширением `.sql` и содержать SQL-скрипт обновления базы данных.
2. В папке src хранятся все необходимые исходники: 
    * Папка DAG содержит DAGs Airflow.

### Как запустить контейнер
Запустите локально команду:

`docker run -d --rm -p 3000:3000 -p 15432:5432 --name=de-project-sprint-3-server sindb/project-sprint-3:latest`

После того как запустится контейнер, у вас будут доступны:
1. Visual Studio Code
2. Airflow
3. Database
