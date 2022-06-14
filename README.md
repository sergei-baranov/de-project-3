# Проект 3

## Этап 0. Backfilling.


### 0.1. Разворачиваем контейнер, бэкапим бд de (схемы staging и mart).

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


### 0.2. Подготавливаем DAG на основе того, что делалось в уроках спринта ("сквозной кейз")

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

   - `/src/DAG/etl_backfilling.py` (в контейнере - в `/lessons/dags/etl_backfilling.py`)


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
