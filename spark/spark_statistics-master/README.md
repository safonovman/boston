Гид по безопасному Бостону
В этом задании предлагается собрать статистику по криминогенной обстановке в разных районах Бостона. 
В качестве исходных данных используется датасет
https://www.kaggle.com/AnalyzeBoston/crimes-in-boston
С помощью Spark соберите агрегат по районам (поле district) со следующими метриками:
crimes_total - общее количество преступлений в этом районе
crimes_monthly - медиана числа преступлений в месяц в этом районе
frequent_crime_types - три самых частых crime_type за всю историю наблюдений в этом районе, 
объединенных через запятую с одним пробелом “, ” 
, расположенных в порядке убывания частоты
crime_type - первая часть NAME из таблицы offense_codes, разбитого по разделителю “-” 
(например, если NAME “BURGLARY - COMMERICAL - ATTEMPT”, то crime_type “BURGLARY”)
lat - широта координаты района, расчитанная как среднее по всем широтам инцидентов
lng - долгота координаты района, расчитанная как среднее по всем долготам инцидентов
Программа должна упаковываться в uber-jar (с помощью sbt-assembly), и запускаться командой

spark-submit --master local[*] --class com.example.BostonCrimesMap /path/to/jar {path/to/crime.csv}
{path/to/offense_codes.csv} {path/to/output_folder}
где {...} - аргументы, передаваемые пользователем.

Пример - 
./spark-submit --master local[*] --class otus.BostonAnalytic 
/home/belstrel/sparksbt/target/scala-2.11/sparkProject-assembly-0.0.1.jar 
"/home/belstrel/offense_codes.csv"  "/home/belstrel/crime.csv"  "/home/belstrel/result.parquet"

Результатом её выполнения должен быть один файл в формате .parquet в папке path/to/output_folder.
Для джойна со справочником необходимо использовать broadcast.




Документация по Spark: https://spark.apache.org/docs/latest/
Документация по SQL функциям Spark: https://spark.apache.org/docs/latest/api/sql/index.html
