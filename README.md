# Superjob Analytics

Целью данного проекта анализ IT вакансий, размещенных на [ресурсе Superjob](https://www.superjob.ru/vakansii/it-internet-svyaz-telekom/). </br>
Проект состоит из 4 этапов:

### 1. Регулярное скачивание данных
**Стек**: Python, Airflow </br>
Ежедневно запускается dag, который парсит актуальные вакансии и сохраняет данные локально в формате Parquet. </br>
Какая информация ищется:
- Название и url вакансии;
- Зарплата;
- Локация;
- Требуемый уровень образования;
- Требуемый опыт работы;
- Тип занятости;
- Детальное описание вакансии.

Парсинг устроен просто и происходит с помощью библиотеки requests. Сначала ищется список интересующих вакансий, затем итеративно на странице каждой вакансии ищется релевантная информация.

### 2. Загрузка данных в хранилище
**Стек:** Airflow, Hadoop </br>
После того, как новые данные появились локально, происходит их загрузка в хранилище с помощью автоматического запуска DAG. </br>
Внутри себя DAG запускает bash-скрипт, который отправляет parquet-файл на кластер и затем загружает в Hadoop в таблицу **superjob_data**.

### 3. Расчеты метрик в хранилище
**Стек:** Airflow, Hadoop </br>
Рассчитываемые метрики:
- Общее число вакансий по ключевым словам;
- Расчет статистик зарплаты по ключевым словам из описания вакансий;
- *Расчет статистик зарплаты в разрезе уровня образования*;
- *Расчет статистик зарплаты в разрезе опыта работы*;
- *Расчет статистик зарплаты в разрезе формата работы*;

Расчет метрик происходит следующим таском внутри того же DAG-а, что и на предыдущем этапе. Для этого также запускается bash-скрипт, в котором написаны все нужные скрипты расчета метрик. Сначала в таблице **superjob_metrics** чистятся прыдыдущие расчеты, затем происходит загрузка новых чисел.</br>
Далее в том же DAG-е происходит локальное сохранение посчитанных метрик и получения файла с кластера.

### 4. Telegram бот для получения данных
**Стек:** Docker, Hadoop, Python </br>
Внутри бота реализован простой функционал для просмотра полученных метрик. 
Команды:
* <span style="color:blue">/start</span> - начать работу с ботом и посмотреть список возможных команд;
* <span style="color:blue">/vacancies_count</span> - статистика по числу вакансий всего и в различных группировках в виде таблицы и графика;
* <span style="color:blue">/salary_stat_by_keyword</span> - статистика по предлгаемой в вакансиях зарплате в разрезе ключевых слов в виде таблицы и графика;
* <span style="color:blue">/salary_stat_by_experience</span> - статистика по предлгаемой в вакансиях зарплате в разрезе опыта работы в виде таблицы и графика;
* <span style="color:blue">/salary_stat_by_employment</span> - статистика по предлгаемой в вакансиях зарплате в разрезе формата работы в виде таблицы и графика.
