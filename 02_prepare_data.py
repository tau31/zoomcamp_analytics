import polars as pl
import polars.selectors as cs
import os
import re


file_l = os.listdir("data")

homework_files = [x for x in file_l if re.match("homework-", x)]
project_files = [x for x in file_l if re.match("project-", x)]
dt_l = list()
for file in file_l:
    task = re.findall("^\w+", file)[0]
    context = re.sub("homework-|.csv", "", file)
    print(file)
    path = f"data/{file}"
    dt = pl.read_csv(path, infer_schema_length=0)
    dt = dt.with_columns(
        task=pl.lit(task),
        context=pl.lit(context)
    )
    dt_l.append(dt)

homework_l = list(filter(lambda x: x.select(
    "task")[0].item() == "homework", dt_l))
project_l = list(filter(lambda x: x.select(
    "task")[0].item() == "project", dt_l))

homework = pl.DataFrame(pl.concat(homework_l, how="diagonal"))
homework = homework.with_columns(
    pl.when(cs.starts_with("time").str.contains("http"))
    .then(None)
    .otherwise(cs.starts_with("time"))
    .cast(pl.Float64)
    .name.keep()
).with_columns(
    pl.when(cs.starts_with("time").gt(336))  # 336 hours in two weeks
    .then(None)
    .otherwise(cs.starts_with("time"))
    .name.keep()
)

project = pl.DataFrame(pl.concat(project_l, how="diagonal"))
project = project.with_columns(
    cs.starts_with("time").cast(pl.Float64).name.keep()
)

homework.write_csv("data/homework_all.csv")
project.write_csv("data/project_all.csv")
